use std::sync::Arc;
use rk_core::chunk_store::ChunkStore;
use rk_transport::{cert, hub::Hub, satellite::Satellite};

#[tokio::test]
async fn fetch_chunk_over_localhost() {
    // Set up hub with a chunk store containing known data
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let data = b"hello from the hub chunk store, this is test data for transport";
    let hash = store.put(data).unwrap();

    let (cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(cert.clone(), key).unwrap();
    let client_config = cert::client_config(&cert).unwrap();

    let hub = Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, store, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    // Satellite connects and fetches the chunk
    let sat = Satellite::connect(hub_addr, "localhost", client_config, "sat-1")
        .await
        .unwrap();

    let fetched = sat.fetch_chunk(&hash).await.unwrap();
    assert!(fetched.is_some(), "chunk should be found");
    let result = fetched.unwrap();
    // Hub has the chunk in store, so it should send compressed bytes
    assert!(result.compressed, "chunk from store should be compressed on wire");
    // Decompress and verify content matches
    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(decompressed, data);

    // Fetch a nonexistent chunk
    let fake_hash = blake3::hash(b"nonexistent");
    let not_found = sat.fetch_chunk(&fake_hash).await.unwrap();
    assert!(not_found.is_none(), "should return None for missing chunk");
}
