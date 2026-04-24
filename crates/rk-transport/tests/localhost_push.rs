// SPDX-License-Identifier: AGPL-3.0-only

use std::sync::Arc;

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;

/// Helper: spin up a hub + connected satellite on localhost.
async fn setup() -> (rk_transport::satellite::Satellite, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().join("store")));
    std::fs::create_dir_all(dir.path().join("store")).unwrap();

    let (cert, key) = rk_transport::cert::generate_self_signed().unwrap();
    let server_config = rk_transport::cert::server_config(cert.clone(), key).unwrap();
    let client_config = rk_transport::cert::client_config(&cert).unwrap();

    let hub = rk_transport::hub::Hub::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_config,
        store,
        None,
        None,
    )
    .await
    .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = rk_transport::satellite::Satellite::connect(
        hub_addr,
        "localhost",
        client_config,
        "push-test",
    )
    .await
    .unwrap();

    (sat, dir)
}

#[tokio::test]
async fn push_chunk_and_have_check_returns_true() {
    let (sat, _dir) = setup().await;

    let data = vec![42u8; 8192];
    let hash = blake3::hash(&data);
    let compressed = zstd::encode_all(data.as_slice(), 3).unwrap();

    // Push the chunk
    let ok = sat
        .push_chunk(&hash, &compressed, data.len() as u64)
        .await
        .unwrap();
    assert!(ok);

    // Verify hub has it
    let have = sat.have_check(&[hash]).await.unwrap();
    assert_eq!(have, vec![true]);
}

#[tokio::test]
async fn have_check_empty_store_returns_all_false() {
    let (sat, _dir) = setup().await;

    let hashes: Vec<blake3::Hash> = (0..3u8).map(|i| blake3::hash(&[i; 64])).collect();

    let result = sat.have_check(&hashes).await.unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.iter().all(|&h| !h));
}

/// Helper: setup with catalog enabled
async fn setup_with_catalog() -> (rk_transport::satellite::Satellite, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store_path = dir.path().join("store");
    std::fs::create_dir_all(&store_path).unwrap();
    let store = Arc::new(ChunkStore::new(store_path));

    let catalog = Catalog::open(&dir.path().join("catalog.db")).unwrap();
    catalog
        .add_library("local", "push-test", "quic://localhost", "")
        .unwrap();
    let catalog = Arc::new(std::sync::Mutex::new(catalog));

    let (cert, key) = rk_transport::cert::generate_self_signed().unwrap();
    let server_config = rk_transport::cert::server_config(cert.clone(), key).unwrap();
    let client_config = rk_transport::cert::client_config(&cert).unwrap();

    let hub = rk_transport::hub::Hub::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_config,
        store,
        None,
        Some(catalog),
    )
    .await
    .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = rk_transport::satellite::Satellite::connect(
        hub_addr,
        "localhost",
        client_config,
        "push-test",
    )
    .await
    .unwrap();

    (sat, dir)
}

#[tokio::test]
async fn push_file_end_to_end() {
    let (sat, _dir) = setup_with_catalog().await;

    // Create file data, chunk it
    let file_data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let chunk_metas = rk_core::chunker::chunk_data(&file_data, 4096, 8192, 16384);

    // Push each chunk
    for meta in &chunk_metas {
        let start = meta.offset as usize;
        let end = start + meta.size;
        let slice = &file_data[start..end];
        let compressed = zstd::encode_all(slice, 3).unwrap();
        let ok = sat
            .push_chunk(&meta.hash, &compressed, meta.size as u64)
            .await
            .unwrap();
        assert!(ok);
    }

    // Push manifest
    let chunk_infos: Vec<(blake3::Hash, u64, u64)> = chunk_metas
        .iter()
        .map(|m| (m.hash, m.offset, m.size as u64))
        .collect();
    let ok = sat
        .push_manifest("test-video.mp4", file_data.len() as u64, &chunk_infos)
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn push_chunk_rejects_bad_hash() {
    let (sat, _dir) = setup().await;

    let data = vec![42u8; 8192];
    let wrong_hash = blake3::hash(b"wrong");
    let compressed = zstd::encode_all(data.as_slice(), 3).unwrap();

    let result = sat
        .push_chunk(&wrong_hash, &compressed, data.len() as u64)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("hash mismatch"), "unexpected error: {err}");
}

#[tokio::test]
async fn push_same_chunk_twice_is_ok() {
    let (sat, _dir) = setup().await;

    let data = vec![42u8; 8192];
    let hash = blake3::hash(&data);
    let compressed = zstd::encode_all(data.as_slice(), 3).unwrap();

    let ok1 = sat
        .push_chunk(&hash, &compressed, data.len() as u64)
        .await
        .unwrap();
    let ok2 = sat
        .push_chunk(&hash, &compressed, data.len() as u64)
        .await
        .unwrap();
    assert!(ok1);
    assert!(ok2);
}

#[tokio::test]
async fn push_manifest_rejects_path_traversal() {
    let (sat, _dir) = setup_with_catalog().await;
    let result = sat.push_manifest("../../etc/passwd", 0, &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn have_check_reflects_pushed_chunks() {
    let (sat, _dir) = setup().await;

    let data1 = vec![1u8; 8192];
    let data2 = vec![2u8; 8192];
    let hash1 = blake3::hash(&data1);
    let hash2 = blake3::hash(&data2);

    // Push only chunk 1
    let compressed = zstd::encode_all(data1.as_slice(), 3).unwrap();
    sat.push_chunk(&hash1, &compressed, data1.len() as u64)
        .await
        .unwrap();

    // Have-check both
    let have = sat.have_check(&[hash1, hash2]).await.unwrap();
    assert_eq!(have, vec![true, false]);
}
