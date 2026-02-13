// SPDX-License-Identifier: AGPL-3.0-only

//! Integration tests for zero-copy indexing lifecycle.

use std::fs;
use std::io::{Cursor, Read};

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::indexer::{IndexConfig, index_directory};
use rk_core::manifest::Manifest;
use rk_core::resolver::ChunkResolver;
use rk_core::verifier;

fn small_chunk_config() -> IndexConfig {
    IndexConfig {
        min_chunk: 512,
        avg_chunk: 1024,
        max_chunk: 2048,
    }
}

/// Create a test directory with known content.
fn make_test_dir(root: &std::path::Path) {
    fs::create_dir_all(root.join("sub")).unwrap();
    fs::write(root.join("hello.txt"), b"hello world from rk test").unwrap();
    fs::write(root.join("data.bin"), b"binary data content here!").unwrap();
    fs::write(root.join("sub/nested.txt"), b"nested file for testing").unwrap();
}

/// Test 1: Index a directory, verify ChunkStore is empty, export via
/// ChunkResolver, verify tar contents match originals.
#[test]
fn index_then_export_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    make_test_dir(&root);

    let store_dir = tmp.path().join("store");
    fs::create_dir_all(&store_dir).unwrap();
    let store = ChunkStore::new(store_dir.clone());
    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    let stats = index_directory(&root, &mut manifest, &catalog, "local", "demo", &config).unwrap();

    assert_eq!(stats.files_indexed, 3);
    assert_eq!(stats.dirs_found, 1);

    // Chunk store should be empty — no data copied
    let chunks_dir = store_dir.join("chunks");
    assert!(
        !chunks_dir.exists() || fs::read_dir(&chunks_dir).unwrap().count() == 0,
        "chunk store should be empty after index"
    );

    // Export via ChunkResolver (manifest-backed, no store data)
    let resolver = ChunkResolver::new(Some(&manifest), &store);
    let mut exported = Vec::new();
    rk_tar::export::export_tar(&mut exported, &resolver, &catalog, "local", "demo", "/").unwrap();

    // Parse exported tar and verify contents
    let mut archive = tar::Archive::new(Cursor::new(&exported));
    let mut found: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().to_string();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        found.insert(path, data);
    }

    assert_eq!(found["hello.txt"], b"hello world from rk test");
    assert_eq!(found["data.bin"], b"binary data content here!");
    assert_eq!(found["sub/nested.txt"], b"nested file for testing");
}

/// Test 2: Index AND ingest same files, verify hash match, resolver
/// prefers manifest.
#[test]
fn cross_mode_dedup() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    let content = b"shared content for cross-mode test";
    fs::write(root.join("file.txt"), content).unwrap();

    let store = ChunkStore::new(tmp.path().join("store"));
    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    // Index (zero-copy)
    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    index_directory(&root, &mut manifest, &catalog, "local", "tape-idx", &config).unwrap();

    // Ingest (copy into store)
    let ingest_result = rk_core::chunker::ingest(content, &store, 512, 1024, 2048).unwrap();
    catalog
        .record_file(
            "local",
            "tape-store",
            "file.txt",
            1,
            content.len() as u64,
            None,
            None,
            1,
            &ingest_result.chunks,
        )
        .unwrap();

    // Both should produce the same hash
    let idx_chunks = catalog
        .get_file_chunks("local", "tape-idx", "file.txt")
        .unwrap();
    let store_chunks = catalog
        .get_file_chunks("local", "tape-store", "file.txt")
        .unwrap();
    assert_eq!(idx_chunks.len(), store_chunks.len());
    for (ic, sc) in idx_chunks.iter().zip(store_chunks.iter()) {
        assert_eq!(
            ic.hash, sc.hash,
            "hashes should match across index and ingest"
        );
    }

    // Resolver should return data (manifest preferred, but store also works)
    let resolver = ChunkResolver::new(Some(&manifest), &store);
    for c in &idx_chunks {
        let data = resolver.get(&c.hash).unwrap();
        assert!(!data.is_empty());
    }
}

/// Test 3: Index same dir into two tapes, manifest chunk count unchanged.
#[test]
fn cross_tape_dedup() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("shared.txt"), b"this data is shared across tapes").unwrap();

    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();

    // Index into tape1
    let stats1 =
        index_directory(&root, &mut manifest, &catalog, "local", "tape1", &config).unwrap();
    let chunks_after_first = manifest.chunks.len();

    // Index same dir into tape2
    let stats2 =
        index_directory(&root, &mut manifest, &catalog, "local", "tape2", &config).unwrap();
    let chunks_after_second = manifest.chunks.len();

    // Same chunks — count should not increase
    assert_eq!(
        chunks_after_first, chunks_after_second,
        "cross-tape dedup should share chunks"
    );
    assert_eq!(
        stats1.chunks_new, stats2.chunks_dedup,
        "second index should dedup all"
    );
}

/// Test 4: Index files, estimate → all chunks local, zero transfer.
#[test]
fn estimate_with_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("local.bin"), b"data that is indexed locally").unwrap();

    let empty_store = ChunkStore::new(tmp.path().join("store"));
    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();

    let resolver = ChunkResolver::new(Some(&manifest), &empty_store);
    let est = rk_scheduler::estimate::estimate_file(&catalog, &resolver, "local", "t", "local.bin")
        .unwrap();

    assert!(est.total_chunks > 0);
    assert_eq!(
        est.local_chunks, est.total_chunks,
        "all chunks should be local via manifest"
    );
    assert_eq!(est.missing_chunks, 0);
    assert_eq!(est.transfer_bytes, 0);
}

/// Test 5: Index files, modify source, verify → StaleSourceFile error on export.
#[test]
fn stale_detection_e2e() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("mutable.txt"), b"original content").unwrap();

    let store = ChunkStore::new(tmp.path().join("store"));
    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();

    // Modify the source file (changes mtime + size)
    fs::write(
        root.join("mutable.txt"),
        b"CHANGED content, different size!!",
    )
    .unwrap();

    // Export should fail with stale error
    let resolver = ChunkResolver::new(Some(&manifest), &store);
    let mut buf = Vec::new();
    let result = rk_tar::export::export_tar(&mut buf, &resolver, &catalog, "local", "t", "/");
    assert!(result.is_err(), "export should fail on stale source");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("stale source file"),
        "error should mention staleness: {err}"
    );
}

/// Test 6: Verify manifest integrity (stat-only and blake3 modes).
#[test]
fn verify_manifest_e2e() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("good.txt"), b"good file content").unwrap();

    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();

    // Stat-only verify
    let result = verifier::verify_manifest(&manifest, false);
    assert_eq!(result.chunks_ok, result.chunks_checked);
    assert_eq!(result.chunks_stale, 0);

    // BLAKE3 verify
    let result = verifier::verify_manifest(&manifest, true);
    assert_eq!(result.chunks_ok, result.chunks_checked);
    assert_eq!(result.chunks_corrupted, 0);
}

/// Test 7: Manifest roundtrip through file.
#[test]
fn manifest_file_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("data");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("a.txt"), b"file a").unwrap();
    fs::write(root.join("b.txt"), b"file b").unwrap();

    let catalog = Catalog::open(tmp.path().join("catalog.db").as_path()).unwrap();

    let mut manifest = Manifest::new();
    let config = small_chunk_config();
    index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();

    let manifest_path = tmp.path().join("manifest.rkm");
    manifest.write_to_file(&manifest_path).unwrap();

    let loaded = Manifest::read_from_file(&manifest_path).unwrap();
    assert_eq!(loaded.sources.len(), manifest.sources.len());
    assert_eq!(loaded.chunks.len(), manifest.chunks.len());

    // All chunks from original should be found in loaded
    for hash in manifest.chunks.keys() {
        assert!(
            loaded.has_chunk(hash),
            "loaded manifest missing chunk {hash}"
        );
    }
}
