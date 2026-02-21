// SPDX-License-Identifier: AGPL-3.0-only

//! UDP proxy for simulating degraded network links in tests.
//!
//! Gated behind the `test-support` feature flag.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// Network degradation parameters for a simulated link.
#[derive(Clone, Debug)]
pub struct LinkProfile {
    /// One-way latency injected per packet.
    pub latency: Duration,
    /// Random +/- variation added to latency.
    pub jitter: Duration,
    /// Probability of dropping a packet (0.0 = no loss, 1.0 = total blackout).
    pub loss_percent: f64,
    /// Bandwidth cap in bytes per second. `None` = unlimited.
    pub bandwidth_bps: Option<u64>,
}

impl LinkProfile {
    /// Baseline: no degradation.
    pub fn lan() -> Self {
        Self {
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_percent: 0.0,
            bandwidth_bps: None,
        }
    }

    /// Home WiFi: 10ms latency, 0.1% loss, 50 Mbps.
    pub fn wifi() -> Self {
        Self {
            latency: Duration::from_millis(10),
            jitter: Duration::from_millis(5),
            loss_percent: 0.001,
            bandwidth_bps: Some(6_250_000), // 50 Mbps
        }
    }

    /// 3G roaming: 150ms latency, 3% loss, 1 Mbps.
    pub fn metered() -> Self {
        Self {
            latency: Duration::from_millis(150),
            jitter: Duration::from_millis(50),
            loss_percent: 0.03,
            bandwidth_bps: Some(125_000), // 1 Mbps
        }
    }

    /// Maritime satellite: 600ms latency, 8% loss, 256 kbps.
    pub fn vsat() -> Self {
        Self {
            latency: Duration::from_millis(600),
            jitter: Duration::from_millis(100),
            loss_percent: 0.08,
            bandwidth_bps: Some(32_000), // 256 kbps
        }
    }

    /// HF radio / worst case: 1500ms latency, 25% loss, 64 kbps.
    pub fn hostile() -> Self {
        Self {
            latency: Duration::from_millis(1500),
            jitter: Duration::from_millis(500),
            loss_percent: 0.25,
            bandwidth_bps: Some(8_000), // 64 kbps
        }
    }

    /// Total link failure: 100% packet loss.
    pub fn blackout() -> Self {
        Self {
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
            loss_percent: 1.0,
            bandwidth_bps: None,
        }
    }
}

/// UDP proxy that sits between satellite and hub, degrading traffic
/// according to a runtime-mutable [`LinkProfile`].
pub struct LinkSimulator {
    proxy_addr: SocketAddr,
    profile: Arc<RwLock<LinkProfile>>,
    task: JoinHandle<()>,
}

impl LinkSimulator {
    /// Start the proxy. Returns immediately; forwarding runs in background.
    /// The satellite should connect to [`Self::addr()`] instead of the hub directly.
    pub async fn start(hub_addr: SocketAddr, profile: LinkProfile) -> std::io::Result<Self> {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let proxy_addr = socket.local_addr()?;
        let profile = Arc::new(RwLock::new(profile));

        let task = {
            let socket = socket.clone();
            let profile = profile.clone();
            tokio::spawn(async move {
                run_proxy(socket, hub_addr, profile).await;
            })
        };

        Ok(Self { proxy_addr, profile, task })
    }

    /// Address the satellite should connect to.
    pub fn addr(&self) -> SocketAddr {
        self.proxy_addr
    }

    /// Change the link profile at runtime (e.g. switch to blackout mid-test).
    pub async fn set_profile(&self, profile: LinkProfile) {
        *self.profile.write().await = profile;
    }
}

impl Drop for LinkSimulator {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run_proxy(
    socket: Arc<UdpSocket>,
    hub_addr: SocketAddr,
    profile: Arc<RwLock<LinkProfile>>,
) {
    let mut buf = [0u8; 65535];
    let satellite_addr: Arc<tokio::sync::Mutex<Option<SocketAddr>>> =
        Arc::new(tokio::sync::Mutex::new(None));
    let next_send_to_hub = Arc::new(tokio::sync::Mutex::new(Instant::now()));
    let next_send_to_sat = Arc::new(tokio::sync::Mutex::new(Instant::now()));

    loop {
        let (len, src) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(_) => break,
        };
        let packet = buf[..len].to_vec();
        let prof = profile.read().await.clone();

        let (dest, next_send) = if src == hub_addr {
            let sat = *satellite_addr.lock().await;
            match sat {
                Some(addr) => (addr, next_send_to_sat.clone()),
                None => continue,
            }
        } else {
            *satellite_addr.lock().await = Some(src);
            (hub_addr, next_send_to_hub.clone())
        };

        // Packet loss
        if prof.loss_percent >= 1.0 {
            continue;
        }
        if prof.loss_percent > 0.0 {
            let mut rng = rand::thread_rng();
            if rng.gen_range(0.0..1.0f64) < prof.loss_percent {
                continue;
            }
        }

        let socket = socket.clone();
        tokio::spawn(async move {
            // Bandwidth limiting (token bucket)
            if let Some(bw) = prof.bandwidth_bps {
                let transfer_time =
                    Duration::from_secs_f64(packet.len() as f64 / bw as f64);
                let mut next = next_send.lock().await;
                let now = Instant::now();
                if *next > now {
                    tokio::time::sleep_until(*next).await;
                }
                *next = std::cmp::max(now, *next) + transfer_time;
            }

            // Latency + jitter
            let delay = {
                let base_ms = prof.latency.as_millis() as f64;
                let jitter_ms = prof.jitter.as_millis() as f64;
                if jitter_ms > 0.0 {
                    let mut rng = rand::thread_rng();
                    let offset = rng.gen_range(-jitter_ms..jitter_ms);
                    Duration::from_millis((base_ms + offset).max(0.0) as u64)
                } else if base_ms > 0.0 {
                    Duration::from_millis(base_ms as u64)
                } else {
                    Duration::ZERO
                }
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }

            let _ = socket.send_to(&packet, dest).await;
        });
    }
}
