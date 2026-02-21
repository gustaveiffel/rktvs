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
