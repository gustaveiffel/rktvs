//! Integration test: file → chunks → store → catalog → reconstruct → compare.

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::chunker;

/// Generate deterministic test data that's large enough to produce multiple chunks.
fn test_data(size: usize) -> Vec<u8> {
    (0..size as u64)
        .map(|i| {
            // Mix bytes to avoid trivial patterns that chunk poorly
            ((i.wrapping_mul(2654435761)) % 256) as u8
        })
        .collect()
}

#[test]
fn roundtrip_file_through_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::new(dir.path().to_path_buf());
    let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

    // 1. Generate test data (~512 KB — enough for multiple chunks with small sizes)
    let original = test_data(512 * 1024);

    // 2. Ingest: chunk + store
    let result = chunker::ingest(&original, &store, 16_384, 32_768, 65_536).unwrap();
    assert!(result.chunks.len() > 1, "expected multiple chunks");
    assert_eq!(result.total_size, original.len() as u64);

    // 3. Record in catalog
    let file_path = "/test/bigfile.bin";
    catalog
        .record_file("local", "default", file_path, 1, original.len() as u64, None, None, 1, &result.chunks)
        .unwrap();

    // 4. Reconstruct from catalog + store
    let chunk_list = catalog
        .get_file_chunks("local", "default", file_path)
        .unwrap();
    assert_eq!(chunk_list.len(), result.chunks.len());

    let mut reconstructed = Vec::with_capacity(original.len());
    for chunk_meta in &chunk_list {
        let data = store.get(&chunk_meta.hash).unwrap();
        assert_eq!(data.len(), chunk_meta.size);
        reconstructed.extend_from_slice(&data);
    }

    // 5. Compare
    assert_eq!(reconstructed.len(), original.len());
    assert_eq!(reconstructed, original);
}

#[test]
fn dedup_identical_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::new(dir.path().to_path_buf());

    // Two identical inputs should produce the same chunks (dedup)
    let data = test_data(100_000);
    let result1 = chunker::ingest(&data, &store, 8_192, 16_384, 32_768).unwrap();
    let result2 = chunker::ingest(&data, &store, 8_192, 16_384, 32_768).unwrap();

    assert_eq!(result1.chunks.len(), result2.chunks.len());
    for (a, b) in result1.chunks.iter().zip(result2.chunks.iter()) {
        assert_eq!(a.hash, b.hash);
    }
}

#[test]
fn empty_data_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::new(dir.path().to_path_buf());
    let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

    let original = Vec::<u8>::new();
    let result = chunker::ingest(&original, &store, 8_192, 16_384, 32_768).unwrap();

    catalog
        .record_file("local", "default", "/empty.bin", 1, 0, None, None, 1, &result.chunks)
        .unwrap();

    let chunk_list = catalog
        .get_file_chunks("local", "default", "/empty.bin")
        .unwrap();

    let mut reconstructed = Vec::new();
    for chunk_meta in &chunk_list {
        let data = store.get(&chunk_meta.hash).unwrap();
        reconstructed.extend_from_slice(&data);
    }

    assert_eq!(reconstructed, original);
}
