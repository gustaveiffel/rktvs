// SPDX-License-Identifier: AGPL-3.0-only

//! Tests for rk-transport over degraded network links.
//!
//! Requires the `test-support` feature: cargo test -p rk-transport --features test-support

use std::sync::Arc;
use std::time::Duration;

use rk_core::chunk_store::ChunkStore;
use rk_transport::{
    cert,
    hub::Hub,
    link_simulator::{LinkProfile, LinkSimulator},
    satellite::Satellite,
};

/// Spin up hub + proxy, return (simulator, client_config).
async fn setup_hub_and_proxy(
    store: Arc<ChunkStore>,
    profile: LinkProfile,
) -> (LinkSimulator, quinn::ClientConfig) {
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

    let sim = LinkSimulator::start(hub_addr, profile).await.unwrap();
    (sim, client_config)
}

/// Client config with transport tuned for degraded links.
fn degraded_client_config(
    cert: &rustls::pki_types::CertificateDer<'static>,
    initial_rtt: Duration,
) -> quinn::ClientConfig {
    let mut cc = cert::client_config(cert).unwrap();
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(Duration::from_secs(120)).unwrap(),
    ));
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.initial_rtt(initial_rtt);
    cc.transport_config(Arc::new(transport));
    cc
}

/// Hub + proxy + degraded client config.
async fn setup_degraded(
    store: Arc<ChunkStore>,
    profile: LinkProfile,
    initial_rtt: Duration,
) -> (LinkSimulator, quinn::ClientConfig) {
    let (cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(cert.clone(), key).unwrap();
    let client_config = degraded_client_config(&cert, initial_rtt);

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

    let sim = LinkSimulator::start(hub_addr, profile).await.unwrap();
    (sim, client_config)
}

// --- Tests ---

#[tokio::test]
async fn fetch_chunk_through_lan_proxy() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let data = b"baseline test data through proxy";
    let hash = store.put(data).unwrap();

    let (sim, client_config) = setup_hub_and_proxy(store, LinkProfile::lan()).await;

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "proxy-sat")
        .await
        .unwrap();

    let result = sat.fetch_chunk(&hash).await.unwrap().unwrap();
    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(decompressed, data);
}

#[tokio::test]
async fn fetch_chunk_high_latency() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let data = b"high latency vsat chunk data for testing transport resilience";
    let hash = store.put(data).unwrap();

    let (sim, client_config) =
        setup_degraded(store, LinkProfile::vsat(), Duration::from_millis(600)).await;

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "vsat-sat")
        .await
        .unwrap();

    let result = sat.fetch_chunk(&hash).await.unwrap().unwrap();
    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(decompressed, data);
}

#[tokio::test]
async fn fetch_chunk_packet_loss() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let data = vec![0xABu8; 4096]; // 4KB chunk
    let hash = store.put(&data).unwrap();

    let (sim, client_config) =
        setup_degraded(store, LinkProfile::hostile(), Duration::from_millis(1500)).await;

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "hostile-sat")
        .await
        .unwrap();

    let result = sat.fetch_chunk(&hash).await.unwrap().unwrap();
    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(blake3::hash(&decompressed), hash);
}

#[tokio::test]
async fn fetch_chunk_bandwidth_limited() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    // ~8KB payload — should take ~250ms at 256kbps + latency overhead
    let data = vec![0x42u8; 8192];
    let hash = store.put(&data).unwrap();

    let (sim, client_config) =
        setup_degraded(store, LinkProfile::vsat(), Duration::from_millis(600)).await;

    let start = std::time::Instant::now();

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "bw-sat")
        .await
        .unwrap();

    let result = sat.fetch_chunk(&hash).await.unwrap().unwrap();
    let elapsed = start.elapsed();

    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(blake3::hash(&decompressed), hash);

    // Sanity: should take at least 1 second (handshake RTT + transfer)
    assert!(
        elapsed > Duration::from_secs(1),
        "transfer was suspiciously fast: {elapsed:?}"
    );
}

#[tokio::test]
async fn fetch_multichunk_metered() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    // Store 5 distinct chunks
    let chunks: Vec<(blake3::Hash, Vec<u8>)> = (0..5)
        .map(|i| {
            let data = vec![i as u8; 1024 + i * 512];
            let hash = store.put(&data).unwrap();
            (hash, data)
        })
        .collect();

    let (sim, client_config) =
        setup_degraded(store, LinkProfile::metered(), Duration::from_millis(150)).await;

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "metered-sat")
        .await
        .unwrap();

    // Fetch all 5 in series on the same connection
    for (hash, original) in &chunks {
        let result = sat.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(blake3::hash(&decompressed), *hash);
        assert_eq!(decompressed, *original);
    }
}

#[tokio::test]
async fn connection_drop_and_resume() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    // Store 5 chunks
    let chunks: Vec<(blake3::Hash, Vec<u8>)> = (0..5)
        .map(|i| {
            let data = vec![(i + 10) as u8; 2048];
            let hash = store.put(&data).unwrap();
            (hash, data)
        })
        .collect();

    // Use short idle timeout so blackout kills the connection quickly
    let (cert, key) = cert::generate_self_signed().unwrap();
    let mut server_cfg = cert::server_config(cert.clone(), key).unwrap();
    {
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(4)).unwrap(),
        ));
        transport.keep_alive_interval(Some(Duration::from_secs(1)));
        transport.max_concurrent_bidi_streams(256u32.into());
        server_cfg.transport_config(Arc::new(transport));
    }
    let mut client_config = cert::client_config(&cert).unwrap();
    {
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(4)).unwrap(),
        ));
        transport.keep_alive_interval(Some(Duration::from_secs(1)));
        client_config.transport_config(Arc::new(transport));
    }

    let hub = Hub::bind(
        "127.0.0.1:0".parse().unwrap(),
        server_cfg,
        store.clone(),
        None,
        None,
    )
    .await
    .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sim = LinkSimulator::start(hub_addr, LinkProfile::lan())
        .await
        .unwrap();

    // Phase 1: fetch first 2 chunks
    let sat = Satellite::connect(sim.addr(), "localhost", client_config.clone(), "resume-sat")
        .await
        .unwrap();

    let mut fetched_locally = std::collections::HashSet::new();
    for (hash, original) in &chunks[..2] {
        let result = sat.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(&decompressed, original);
        fetched_locally.insert(*hash);
    }

    // Phase 2: blackout — connection dies after idle timeout (4s)
    sim.set_profile(LinkProfile::blackout()).await;
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Phase 3: restore link, reconnect
    sim.set_profile(LinkProfile::lan()).await;

    let sat2 = Satellite::connect(sim.addr(), "localhost", client_config, "resume-sat-2")
        .await
        .unwrap();

    // Fetch remaining: skip chunks we already have (application-level resume)
    let mut wire_fetched = 0u32;
    for (hash, original) in &chunks {
        if fetched_locally.contains(hash) {
            continue;
        }
        let result = sat2.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(&decompressed, original);
        wire_fetched += 1;
    }

    assert_eq!(wire_fetched, 3, "should only fetch the 3 missing chunks");
}

#[tokio::test]
async fn catalog_sync_degraded() {
    use rk_core::catalog::Catalog;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    // Seed catalog with 50 files across 2 tapes
    let hub_catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();
    for i in 0..30u64 {
        let hash = blake3::hash(format!("chunk-docs-{i}").as_bytes());
        let chunks = vec![rk_core::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: (1000 + i * 100) as usize,
            compressed_size: (800 + i * 50) as usize,
        }];
        hub_catalog
            .record_file(
                "local",
                "docs",
                &format!("/file_{i:03}.txt"),
                1,
                1000 + i * 100,
                Some(1700000000 + i),
                Some(0o644),
                1,
                &chunks,
            )
            .unwrap();
    }
    for i in 0..20u64 {
        hub_catalog
            .record_file(
                "local",
                "media",
                &format!("/video_{i:03}.mp4"),
                1,
                1_000_000 * (i + 1),
                Some(1700000000),
                Some(0o644),
                1,
                &[],
            )
            .unwrap();
    }
    let hub_catalog = Arc::new(std::sync::Mutex::new(hub_catalog));

    let (cert, key) = cert::generate_self_signed().unwrap();
    let server_config = cert::server_config(cert.clone(), key).unwrap();
    let client_config = degraded_client_config(&cert, Duration::from_millis(150));

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

    let sim = LinkSimulator::start(hub_addr, LinkProfile::metered())
        .await
        .unwrap();

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "cat-sat")
        .await
        .unwrap();

    // List tapes
    let (tapes, _) = sat.sync_catalog("").await.unwrap();
    assert_eq!(tapes.len(), 2);
    let docs = tapes.iter().find(|(n, _, _)| n == "docs").unwrap();
    assert_eq!(docs.1, 30);

    // Sync one tape
    let (_, files) = sat.sync_catalog("docs").await.unwrap();
    assert_eq!(files.len(), 30);

    // Verify first file has chunk metadata
    let f0 = files.iter().find(|f| f.path == "/file_000.txt").unwrap();
    assert_eq!(f0.chunks.len(), 1);
    assert_eq!(f0.size, 1000);
}

#[tokio::test]
async fn handshake_high_latency() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

    let (sim, client_config) =
        setup_degraded(store, LinkProfile::vsat(), Duration::from_millis(600)).await;

    let sat = Satellite::connect(sim.addr(), "localhost", client_config, "latency-sat")
        .await
        .unwrap();

    assert_eq!(sat.satellite_id, "latency-sat");
    assert_eq!(
        sat.hub_protocol_version,
        rk_transport::hub::PROTOCOL_VERSION
    );
}
