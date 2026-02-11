use std::sync::Arc;

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::chunker;
use rk_core::resolver::ChunkResolver;
use rk_transport::{cert, hub::Hub, satellite::Satellite};
use rk_scheduler::fetcher;

#[tokio::test]
async fn fetch_file_with_resume() {
    // --- Hub side: store with a multi-chunk file ---
    let hub_dir = tempfile::tempdir().unwrap();
    let hub_store = Arc::new(ChunkStore::new(hub_dir.path().to_path_buf()));
    let hub_catalog = Catalog::open(hub_dir.path().join("catalog.db").as_path()).unwrap();

    // Generate data large enough for multiple chunks (use small chunk sizes for test)
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let result = chunker::ingest(&data, &hub_store, 4_096, 8_192, 16_384).unwrap();
    assert!(result.chunks.len() >= 3, "need at least 3 chunks for this test");

    hub_catalog.record_file("lib-a", "tape-1", "/bigfile.bin", 1, data.len() as u64, None, None, 1, &result.chunks).unwrap();

    // --- Satellite side: local store + catalog (simulate partial previous fetch) ---
    let sat_dir = tempfile::tempdir().unwrap();
    let sat_store = ChunkStore::new(sat_dir.path().to_path_buf());
    let sat_catalog = Catalog::open(sat_dir.path().join("catalog.db").as_path()).unwrap();

    // Record the same file metadata in satellite catalog
    sat_catalog.record_file("lib-a", "tape-1", "/bigfile.bin", 1, data.len() as u64, None, None, 1, &result.chunks).unwrap();

    // Pre-populate the first 2 chunks locally (simulate interrupted fetch)
    for chunk in result.chunks.iter().take(2) {
        let chunk_data = hub_store.get(&chunk.hash).unwrap();
        sat_store.put(&chunk_data).unwrap();
    }

    // Create a job
    sat_catalog.create_job("job-resume", "lib-a", "tape-1", "fetch", 1, Some("/bigfile.bin"), Some(result.chunks.len() as i64), Some(data.len() as i64)).unwrap();

    // --- Start hub and connect satellite ---
    let (cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(cert.clone(), key).unwrap();
    let client_config = cert::client_config(&cert).unwrap();

    let hub = Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, hub_store, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let satellite = Satellite::connect(hub_addr, "localhost", client_config, "sat-resume")
        .await
        .unwrap();

    // --- Fetch with resume ---
    let resolver = ChunkResolver::new(None, &sat_store);
    let fetch_result = fetcher::fetch_file(
        &satellite, &resolver, &sat_store, &sat_catalog,
        "lib-a", "tape-1", "/bigfile.bin",
        Some("job-resume"),
    ).await.unwrap();

    assert_eq!(fetch_result.skipped_chunks, 2, "first 2 chunks should be skipped");
    assert_eq!(fetch_result.fetched_chunks, result.chunks.len() - 2);
    assert_eq!(fetch_result.total_chunks, result.chunks.len());

    // Verify all chunks are now local and reconstruct the file
    let chunks = sat_catalog.get_file_chunks("lib-a", "tape-1", "/bigfile.bin").unwrap();
    let mut reconstructed = Vec::new();
    for c in &chunks {
        assert!(sat_store.has(&c.hash), "chunk should be local: {}", c.hash);
        reconstructed.extend_from_slice(&sat_store.get(&c.hash).unwrap());
    }
    assert_eq!(reconstructed, data, "reconstructed file should match original");

    // Verify job is marked complete
    let job = sat_catalog.get_job("job-resume").unwrap().unwrap();
    assert_eq!(job.status, "completed");
    assert_eq!(job.completed_chunks, result.chunks.len() as i64);
}
