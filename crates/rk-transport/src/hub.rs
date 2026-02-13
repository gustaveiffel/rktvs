// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;
use std::sync::Arc;

use quinn::{Endpoint, RecvStream, SendStream};
use rk_core::chunk_store::ChunkStore;
use rk_core::manifest::Manifest;
use rk_core::resolver::ChunkResolver;
use prost::Message;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::frame::{self, StreamTag};
use crate::proto;

/// Current wire protocol version.
/// Bump when ChunkResponse or stream semantics change in incompatible ways.
pub const PROTOCOL_VERSION: u32 = 2;

/// Minimum protocol version this hub accepts from satellites.
const MIN_PROTOCOL_VERSION: u32 = 2;

/// Maximum concurrent connections the hub will accept.
const MAX_CONNECTIONS: usize = 1024;

pub struct Hub {
    endpoint: Endpoint,
    store: Arc<ChunkStore>,
    manifest: Option<Arc<Manifest>>,
    conn_semaphore: Arc<Semaphore>,
}

impl Hub {
    /// Bind the hub QUIC server to an address.
    pub async fn bind(
        addr: SocketAddr,
        server_config: quinn::ServerConfig,
        store: Arc<ChunkStore>,
        manifest: Option<Arc<Manifest>>,
    ) -> std::io::Result<Self> {
        let endpoint = Endpoint::server(server_config, addr)?;
        Ok(Self {
            endpoint,
            store,
            manifest,
            conn_semaphore: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        })
    }

    /// Return the local address the hub is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.endpoint.local_addr().unwrap()
    }

    /// Run the hub, accepting connections forever.
    pub async fn run(&self) {
        while let Some(incoming) = self.endpoint.accept().await {
            let store = self.store.clone();
            let manifest = self.manifest.clone();
            let semaphore = self.conn_semaphore.clone();
            tokio::spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return, // semaphore closed
                };
                match incoming.await {
                    Ok(conn) => {
                        info!(remote = %conn.remote_address(), version = env!("CARGO_PKG_VERSION"), "connection accepted");
                        if let Err(e) = handle_connection(conn, store, manifest).await {
                            warn!("connection error: {e}");
                        }
                    }
                    Err(e) => warn!("incoming connection failed: {e}"),
                }
            });
        }
    }
}

async fn handle_connection(
    conn: quinn::Connection,
    store: Arc<ChunkStore>,
    manifest: Option<Arc<Manifest>>,
) -> anyhow::Result<()> {
    loop {
        let (send, recv) = conn.accept_bi().await?;
        let store = store.clone();
        let manifest = manifest.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(send, recv, store, manifest).await {
                warn!("stream error: {e}");
            }
        });
    }
}

async fn handle_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    store: Arc<ChunkStore>,
    manifest: Option<Arc<Manifest>>,
) -> anyhow::Result<()> {
    // Read the 1-byte stream tag
    let mut tag_buf = [0u8; 1];
    recv.read_exact(&mut tag_buf).await?;
    let tag = StreamTag::try_from(tag_buf[0])
        .map_err(|t| anyhow::anyhow!("unknown stream tag: 0x{t:02x}"))?;

    match tag {
        StreamTag::Control => handle_control(&mut send, &mut recv).await?,
        StreamTag::ChunkRequest => {
            handle_chunk_request(&mut send, &mut recv, store, manifest).await?;
        }
    }

    Ok(())
}

async fn handle_control(send: &mut SendStream, recv: &mut RecvStream) -> anyhow::Result<()> {
    let data = recv.read_to_end(4096).await?;
    let handshake = proto::Handshake::decode_length_delimited(data.as_slice())?;
    info!(satellite = %handshake.satellite_id, version = handshake.protocol_version, "handshake");

    if handshake.protocol_version < MIN_PROTOCOL_VERSION {
        let ack = proto::HandshakeAck {
            ok: false,
            message: format!(
                "protocol version {} too old, hub requires >= {} — upgrade your rk binary",
                handshake.protocol_version, MIN_PROTOCOL_VERSION,
            ),
            protocol_version: PROTOCOL_VERSION,
        };
        let encoded = frame::encode_msg(&ack);
        send.write_all(&encoded).await?;
        send.finish()?;
        return Ok(());
    }

    let ack = proto::HandshakeAck {
        ok: true,
        message: "welcome".into(),
        protocol_version: PROTOCOL_VERSION,
    };
    let encoded = frame::encode_msg(&ack);
    send.write_all(&encoded).await?;
    send.finish()?;
    Ok(())
}

async fn handle_chunk_request(
    send: &mut SendStream,
    recv: &mut RecvStream,
    store: Arc<ChunkStore>,
    manifest: Option<Arc<Manifest>>,
) -> anyhow::Result<()> {
    let data = recv.read_to_end(4096).await?;
    let req = proto::ChunkRequest::decode_length_delimited(data.as_slice())?;

    let hash_bytes: [u8; 32] = req.hash.as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid hash length: {}", req.hash.len()))?;
    let hash = blake3::Hash::from_bytes(hash_bytes);

    // Resolve chunk in a blocking thread to avoid stalling the async runtime
    let resp = tokio::task::spawn_blocking(move || {
        // Prefer serving compressed bytes directly (avoids decompress+recompress)
        if store.has(&hash)
            && let Ok(compressed) = store.get_compressed(&hash)
        {
            let decompressed_size = zstd::decode_all(compressed.as_slice())
                .map(|d| d.len() as u64)
                .unwrap_or(0);
            return proto::ChunkResponse {
                found: true,
                size: decompressed_size,
                data: compressed,
                compressed: true,
            };
        }
        // Fallback: try manifest/resolver path (returns decompressed data)
        let resolver = ChunkResolver::new(manifest.as_deref(), &store);
        match resolver.get(&hash) {
            Ok(chunk_data) => proto::ChunkResponse {
                found: true,
                size: chunk_data.len() as u64,
                data: chunk_data,
                compressed: false,
            },
            Err(_) => proto::ChunkResponse {
                found: false,
                size: 0,
                data: vec![],
                compressed: false,
            },
        }
    }).await?;

    let encoded = frame::encode_msg(&resp);
    send.write_all(&encoded).await?;
    send.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn hub_binds_and_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ChunkStore::new(dir.path().to_path_buf()));

        let (cert, key) = crate::cert::generate_self_signed().unwrap();
        let server_config = crate::cert::server_config(cert, key).unwrap();

        let hub = Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, store, None).await.unwrap();
        let addr = hub.local_addr();
        assert_ne!(addr.port(), 0);
    }
}
