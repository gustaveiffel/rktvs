// SPDX-License-Identifier: AGPL-3.0-only

//! Chaos tests using Linux netem for real kernel-level traffic shaping.
//!
//! Run with: RK_CHAOS=1 sudo -E cargo test --test netem_fetch -- --ignored --nocapture
//!
//! Requires: Linux, root, ip/tc commands.
//! Skips gracefully on macOS or without RK_CHAOS=1.

mod netns_helper;

use std::sync::Arc;
use std::time::Duration;

use rk_core::chunk_store::ChunkStore;
use rk_transport::{cert, hub::Hub, satellite::Satellite};

use netns_helper::{NetnsGuard, can_run_netem};

macro_rules! require_netem {
    () => {
        if !can_run_netem() {
            eprintln!("SKIP: netem not available (need RK_CHAOS=1 + root + Linux)");
            return;
        }
    };
}

#[tokio::test]
#[ignore]
async fn netem_vsat_roundtrip() {
    require_netem!();

    let ns = NetnsGuard::create("vsat").unwrap();
    ns.apply_netem(&["delay", "600ms", "100ms", "loss", "8%"])
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));
    let data = b"vsat netem test payload";
    let hash = store.put(data).unwrap();

    let (cert_der, key) =
        cert::generate_self_signed_for(vec![ns.host_addr.clone(), ns.ns_addr.clone()]).unwrap();
    let server_config = cert::server_config(cert_der.clone(), key).unwrap();
    let client_config = cert::client_config(&cert_der).unwrap();

    let bind_addr = format!("{}:0", ns.host_addr).parse().unwrap();
    let hub = Hub::bind(bind_addr, server_config, store, None, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = Satellite::connect(hub_addr, &ns.host_addr, client_config, "netem-vsat")
        .await
        .unwrap();

    let result = sat.fetch_chunk(&hash).await.unwrap().unwrap();
    let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
    assert_eq!(decompressed, data);
}

#[tokio::test]
#[ignore]
async fn netem_hostile_multichunk() {
    require_netem!();

    let ns = NetnsGuard::create("hostile").unwrap();
    ns.apply_netem(&["delay", "1500ms", "500ms", "loss", "25%", "rate", "64kbit"])
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));
    let chunks: Vec<_> = (0..3)
        .map(|i| {
            let data = vec![i as u8; 2048];
            let hash = store.put(&data).unwrap();
            (hash, data)
        })
        .collect();

    let (cert_der, key) =
        cert::generate_self_signed_for(vec![ns.host_addr.clone(), ns.ns_addr.clone()]).unwrap();
    let server_config = cert::server_config(cert_der.clone(), key).unwrap();
    let client_config = cert::client_config(&cert_der).unwrap();

    let bind_addr = format!("{}:0", ns.host_addr).parse().unwrap();
    let hub = Hub::bind(bind_addr, server_config, store, None, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = Satellite::connect(hub_addr, &ns.host_addr, client_config, "netem-hostile")
        .await
        .unwrap();

    for (hash, original) in &chunks {
        let result = sat.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(blake3::hash(&decompressed), *hash);
        assert_eq!(decompressed, *original);
    }
}

#[tokio::test]
#[ignore]
async fn netem_link_flap() {
    require_netem!();

    let ns = NetnsGuard::create("flap").unwrap();
    ns.apply_netem(&["delay", "10ms"]).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));
    let chunks: Vec<_> = (0..4)
        .map(|i| {
            let data = vec![(i + 20) as u8; 2048];
            let hash = store.put(&data).unwrap();
            (hash, data)
        })
        .collect();

    let (cert_der, key) =
        cert::generate_self_signed_for(vec![ns.host_addr.clone(), ns.ns_addr.clone()]).unwrap();
    let server_config = cert::server_config(cert_der.clone(), key).unwrap();
    let client_config = cert::client_config(&cert_der).unwrap();

    let bind_addr = format!("{}:0", ns.host_addr).parse().unwrap();
    let hub = Hub::bind(bind_addr, server_config, store, None, None)
        .await
        .unwrap();
    let hub_addr = hub.local_addr();
    tokio::spawn(async move { hub.run().await });

    let sat = Satellite::connect(hub_addr, &ns.host_addr, client_config.clone(), "netem-flap")
        .await
        .unwrap();

    // Fetch first 2 chunks
    for (hash, original) in &chunks[..2] {
        let result = sat.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(&decompressed, original);
    }

    // Flap: total loss
    ns.reset_netem(&["loss", "100%"]).unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Restore
    ns.reset_netem(&["delay", "10ms"]).unwrap();

    // Reconnect and fetch remaining
    let sat2 = Satellite::connect(hub_addr, &ns.host_addr, client_config, "netem-flap-2")
        .await
        .unwrap();

    for (hash, original) in &chunks[2..] {
        let result = sat2.fetch_chunk(hash).await.unwrap().unwrap();
        let decompressed = zstd::decode_all(result.data.as_slice()).unwrap();
        assert_eq!(&decompressed, original);
    }
}
