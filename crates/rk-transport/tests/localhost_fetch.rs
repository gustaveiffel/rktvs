use std::sync::Arc;
use prost::Message;
use quinn::Endpoint;
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

    let hub = Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, store, None, None)
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

/// Simulate an old satellite (protocol_version = 1) connecting to a v2 hub.
/// The hub should reject the handshake with a clear error message.
#[tokio::test]
async fn old_satellite_rejected_by_new_hub() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let (hub_cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(hub_cert.clone(), key).unwrap();
    let client_config = cert::client_config(&hub_cert).unwrap();

    let hub = Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, store, None, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    // Raw QUIC connection (bypass Satellite::connect to send old version)
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config);
    let conn = endpoint.connect(hub_addr, "localhost").unwrap().await.unwrap();

    let (mut send, mut recv) = conn.open_bi().await.unwrap();

    // Send control tag + old-version handshake
    send.write_all(&[0x00]).await.unwrap();
    let handshake = rk_transport::proto::Handshake {
        satellite_id: "old-sat".into(),
        protocol_version: 1, // old version
    };
    let mut buf = Vec::new();
    handshake.encode_length_delimited(&mut buf).unwrap();
    send.write_all(&buf).await.unwrap();
    send.finish().unwrap();

    // Read ack — should be rejected
    let ack_data = recv.read_to_end(4096).await.unwrap();
    let ack = rk_transport::proto::HandshakeAck::decode_length_delimited(ack_data.as_slice()).unwrap();
    assert!(!ack.ok, "hub should reject old protocol version");
    assert!(ack.message.contains("too old"), "message should explain the rejection: {}", ack.message);
    assert_eq!(ack.protocol_version, rk_transport::hub::PROTOCOL_VERSION);
}
