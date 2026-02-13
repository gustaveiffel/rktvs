use prost::Message;
use quinn::Endpoint;
use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_transport::{cert, hub::Hub, satellite::Satellite};
use std::sync::Arc;

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

    let hub = Hub::bind(
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

    // Satellite connects and fetches the chunk
    let sat = Satellite::connect(hub_addr, "localhost", client_config, "sat-1")
        .await
        .unwrap();

    let fetched = sat.fetch_chunk(&hash).await.unwrap();
    assert!(fetched.is_some(), "chunk should be found");
    let result = fetched.unwrap();
    // Hub has the chunk in store, so it should send compressed bytes
    assert!(
        result.compressed,
        "chunk from store should be compressed on wire"
    );
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

    let hub = Hub::bind(
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

    // Raw QUIC connection (bypass Satellite::connect to send old version)
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(client_config);
    let conn = endpoint
        .connect(hub_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

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
    let ack =
        rk_transport::proto::HandshakeAck::decode_length_delimited(ack_data.as_slice()).unwrap();
    assert!(!ack.ok, "hub should reject old protocol version");
    assert!(
        ack.message.contains("too old"),
        "message should explain the rejection: {}",
        ack.message
    );
    assert_eq!(ack.protocol_version, rk_transport::hub::PROTOCOL_VERSION);
}

/// Full catalog sync round-trip: hub has files, satellite lists tapes and syncs metadata.
#[tokio::test]
async fn catalog_sync_over_localhost() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    // Seed the hub catalog with test data
    let hub_catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();
    let hash1 = blake3::hash(b"chunk-alpha");
    let hash2 = blake3::hash(b"chunk-beta");
    let chunks = vec![
        rk_core::chunker::ChunkMeta {
            hash: hash1,
            offset: 0,
            size: 1000,
            compressed_size: 800,
        },
        rk_core::chunker::ChunkMeta {
            hash: hash2,
            offset: 1000,
            size: 500,
            compressed_size: 400,
        },
    ];
    hub_catalog
        .record_file(
            "local",
            "docs",
            "/readme.md",
            1,
            1500,
            Some(1700000000),
            Some(0o644),
            3,
            &chunks,
        )
        .unwrap();
    hub_catalog
        .record_file(
            "local",
            "docs",
            "/guide.md",
            1,
            200,
            Some(1700000001),
            Some(0o644),
            1,
            &[],
        )
        .unwrap();
    hub_catalog
        .record_file("local", "code", "/main.rs", 1, 300, None, None, 1, &[])
        .unwrap();

    let hub_catalog = Arc::new(std::sync::Mutex::new(hub_catalog));

    let (hub_cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(hub_cert.clone(), key).unwrap();
    let client_config = cert::client_config(&hub_cert).unwrap();

    let hub = Hub::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_config,
        store,
        None,
        Some(hub_catalog),
    )
    .await
    .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = Satellite::connect(hub_addr, "localhost", client_config, "sync-sat")
        .await
        .unwrap();

    // Step 1: list tapes
    let (tapes, files) = sat.sync_catalog("").await.unwrap();
    assert!(files.is_empty(), "listing tapes should not return files");
    assert_eq!(tapes.len(), 2);
    let docs_tape = tapes.iter().find(|(n, _, _)| n == "docs").unwrap();
    assert_eq!(docs_tape.1, 2); // file_count
    assert_eq!(docs_tape.2, 1700); // total_size
    let code_tape = tapes.iter().find(|(n, _, _)| n == "code").unwrap();
    assert_eq!(code_tape.1, 1);

    // Step 2: sync a specific tape
    let (tapes2, files) = sat.sync_catalog("docs").await.unwrap();
    assert!(
        tapes2.is_empty(),
        "syncing a tape should not return tape list"
    );
    assert_eq!(files.len(), 2);

    let readme = files.iter().find(|f| f.path == "/readme.md").unwrap();
    assert_eq!(readme.entry_type, 1);
    assert_eq!(readme.size, 1500);
    assert_eq!(readme.mtime, 1700000000);
    assert_eq!(readme.mode, 0o644);
    assert_eq!(readme.version, 3);
    assert_eq!(readme.chunks.len(), 2);
    assert_eq!(readme.chunks[0].hash, hash1);
    assert_eq!(readme.chunks[0].offset, 0);
    assert_eq!(readme.chunks[0].size, 1000);
    assert_eq!(readme.chunks[1].hash, hash2);
    assert_eq!(readme.chunks[1].offset, 1000);
    assert_eq!(readme.chunks[1].size, 500);

    let guide = files.iter().find(|f| f.path == "/guide.md").unwrap();
    assert!(guide.chunks.is_empty());
}

/// Hub without catalog configured returns a clear error.
#[tokio::test]
async fn catalog_sync_no_catalog_on_hub() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let (hub_cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(hub_cert.clone(), key).unwrap();
    let client_config = cert::client_config(&hub_cert).unwrap();

    // Hub without catalog (None)
    let hub = Hub::bind(
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

    let sat = Satellite::connect(hub_addr, "localhost", client_config, "sync-sat")
        .await
        .unwrap();

    let err = sat.sync_catalog("").await.unwrap_err();
    assert!(
        err.to_string().contains("no catalog"),
        "expected 'no catalog' error, got: {err}"
    );
}
