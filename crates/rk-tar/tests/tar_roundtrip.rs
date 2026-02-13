//! Integration test: tar → ingest → export → verify contents match.

use std::io::Cursor;
use std::io::Read;

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::resolver::ChunkResolver;
use rk_tar::export;
use rk_tar::ingest::{self, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};

/// Build a tar archive with multiple files of various sizes.
fn make_test_tar() -> (Vec<u8>, Vec<(&'static str, Vec<u8>, u32, u64)>) {
    let entries: Vec<(&str, Vec<u8>, u32, u64)> = vec![
        ("small.txt", b"hello world".to_vec(), 0o644, 1700000000),
        (
            "medium.bin",
            (0..50_000u32).map(|i| (i % 251) as u8).collect(),
            0o600,
            1700000100,
        ),
        ("empty.dat", Vec::new(), 0o444, 1700000200),
        (
            "subdir/nested.txt",
            b"nested file content here".to_vec(),
            0o755,
            1700000300,
        ),
    ];

    let mut buf = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut buf);
        for (path, data, mode, mtime) in &entries {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(data.len() as u64);
            header.set_mode(*mode);
            header.set_mtime(*mtime);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            builder.append(&header, data.as_slice()).unwrap();
        }
        builder.finish().unwrap();
    }
    (buf, entries)
}

#[test]
fn tar_roundtrip_preserves_content_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::new(dir.path().to_path_buf());
    let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

    let (tar_data, original_entries) = make_test_tar();

    // Ingest
    let stats = ingest::ingest_tar(
        Cursor::new(&tar_data),
        &store,
        &catalog,
        "local",
        "roundtrip",
        MIN_CHUNK_SIZE,
        AVG_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
    )
    .unwrap();
    assert_eq!(stats.files, 4);

    // Export
    let mut exported = Vec::new();
    let resolver = ChunkResolver::new(None, &store);
    export::export_tar(
        &mut exported,
        &resolver,
        &catalog,
        "local",
        "roundtrip",
        "/",
    )
    .unwrap();

    // Parse exported tar and compare with originals
    let mut archive = tar::Archive::new(Cursor::new(&exported));
    let mut exported_entries: Vec<(String, Vec<u8>, u32, u64)> = Vec::new();

    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().to_string();
        let mode = entry.header().mode().unwrap();
        let mtime = entry.header().mtime().unwrap();
        let mut data = Vec::new();
        entry.read_to_end(&mut data).unwrap();
        exported_entries.push((path, data, mode, mtime));
    }

    assert_eq!(exported_entries.len(), original_entries.len());

    // Sort both by path so order matches (catalog returns files sorted by path)
    exported_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut sorted_originals = original_entries.clone();
    sorted_originals.sort_by(|a, b| a.0.cmp(&b.0));

    for (
        (exp_path, exp_data, exp_mode, exp_mtime),
        (orig_path, orig_data, orig_mode, orig_mtime),
    ) in exported_entries.iter().zip(sorted_originals.iter())
    {
        assert_eq!(exp_path, *orig_path, "path mismatch");
        assert_eq!(exp_data, orig_data, "data mismatch for {}", orig_path);
        assert_eq!(*exp_mode, *orig_mode, "mode mismatch for {}", orig_path);
        assert_eq!(*exp_mtime, *orig_mtime, "mtime mismatch for {}", orig_path);
    }
}

#[test]
fn ingest_twice_deduplicates() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::new(dir.path().to_path_buf());
    let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

    let (tar_data, _) = make_test_tar();

    let stats1 = ingest::ingest_tar(
        Cursor::new(&tar_data),
        &store,
        &catalog,
        "local",
        "tape1",
        MIN_CHUNK_SIZE,
        AVG_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
    )
    .unwrap();

    let stats2 = ingest::ingest_tar(
        Cursor::new(&tar_data),
        &store,
        &catalog,
        "local",
        "tape2",
        MIN_CHUNK_SIZE,
        AVG_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
    )
    .unwrap();

    // Same number of files and chunks (dedup at store level)
    assert_eq!(stats1.files, stats2.files);
    assert_eq!(stats1.chunks, stats2.chunks);
}
