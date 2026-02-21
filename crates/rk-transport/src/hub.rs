// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use quinn::{Endpoint, RecvStream, SendStream};
use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_core::manifest::Manifest;
use rk_core::resolver::ChunkResolver;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::frame::{self, StreamTag};
use crate::proto;

/// Current wire protocol version.
/// Bump when ChunkResponse or stream semantics change in incompatible ways.
pub const PROTOCOL_VERSION: u32 = 3;

/// Minimum protocol version this hub accepts from satellites.
const MIN_PROTOCOL_VERSION: u32 = 2;

/// Maximum concurrent connections the hub will accept.
const MAX_CONNECTIONS: usize = 1024;

pub struct Hub {
    endpoint: Endpoint,
    store: Arc<ChunkStore>,
    manifest: Option<Arc<Manifest>>,
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
    conn_semaphore: Arc<Semaphore>,
}

impl Hub {
    /// Bind the hub QUIC server to an address.
    pub async fn bind(
        addr: SocketAddr,
        server_config: quinn::ServerConfig,
        store: Arc<ChunkStore>,
        manifest: Option<Arc<Manifest>>,
        catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
    ) -> std::io::Result<Self> {
        let endpoint = Endpoint::server(server_config, addr)?;
        Ok(Self {
            endpoint,
            store,
            manifest,
            catalog,
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
            let catalog = self.catalog.clone();
            let semaphore = self.conn_semaphore.clone();
            tokio::spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return, // semaphore closed
                };
                match incoming.await {
                    Ok(conn) => {
                        info!(remote = %conn.remote_address(), version = env!("CARGO_PKG_VERSION"), "connection accepted");
                        if let Err(e) = handle_connection(conn, store, manifest, catalog).await {
                            let msg = e.to_string();
                            if msg.contains("closed by peer") && msg.contains(": 0") {
                                debug!("connection closed cleanly");
                            } else {
                                warn!("connection error: {e}");
                            }
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
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
) -> anyhow::Result<()> {
    loop {
        let (send, recv) = conn.accept_bi().await?;
        let store = store.clone();
        let manifest = manifest.clone();
        let catalog = catalog.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_stream(send, recv, store, manifest, catalog).await {
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
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
) -> anyhow::Result<()> {
    // Read the 1-byte stream tag
    let mut tag_buf = [0u8; 1];
    recv.read_exact(&mut tag_buf).await?;
    let tag = StreamTag::try_from(tag_buf[0])
        .map_err(|t| anyhow::anyhow!("unknown stream tag: 0x{t:02x}"))?;

    match tag {
        StreamTag::Control => handle_control(&mut send, &mut recv).await?,
        StreamTag::ChunkRequest => {
            handle_chunk_request(&mut send, &mut recv, store, manifest, catalog).await?;
        }
        StreamTag::CatalogSync => {
            handle_catalog_sync(&mut send, &mut recv, catalog).await?;
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
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
) -> anyhow::Result<()> {
    let data = recv.read_to_end(4096).await?;
    let req = proto::ChunkRequest::decode_length_delimited(data.as_slice())?;

    let hash_bytes: [u8; 32] = req
        .hash
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid hash length: {}", req.hash.len()))?;
    let hash = blake3::Hash::from_bytes(hash_bytes);

    // Resolve chunk in a blocking thread to avoid stalling the async runtime
    let resp = tokio::task::spawn_blocking(move || {
        // Prefer serving compressed bytes directly (avoids decompress+recompress)
        if store.has(&hash)
            && let Ok(compressed) = store.get_compressed(&hash)
        {
            // Look up decompressed size from catalog instead of decompressing
            let decompressed_size = catalog
                .as_ref()
                .and_then(|cat| {
                    let cat = cat.lock().expect("catalog lock poisoned");
                    cat.get_chunk_decompressed_size(&hash).ok().flatten()
                })
                .unwrap_or(0);
            return proto::ChunkResponse {
                found: true,
                size: decompressed_size,
                data: compressed,
                compressed: true,
            };
        }
        // Fallback: try manifest/resolver path (returns decompressed data)
        // Compress before sending to save bandwidth on hostile links
        let resolver = ChunkResolver::new(manifest.as_deref(), &store);
        match resolver.get(&hash) {
            Ok(chunk_data) => {
                let size = chunk_data.len() as u64;
                match zstd::encode_all(chunk_data.as_slice(), 3) {
                    Ok(compressed) => proto::ChunkResponse {
                        found: true,
                        size,
                        data: compressed,
                        compressed: true,
                    },
                    Err(_) => proto::ChunkResponse {
                        found: true,
                        size,
                        data: chunk_data,
                        compressed: false,
                    },
                }
            }
            Err(_) => proto::ChunkResponse {
                found: false,
                size: 0,
                data: vec![],
                compressed: false,
            },
        }
    })
    .await?;

    let encoded = frame::encode_msg(&resp);
    send.write_all(&encoded).await?;
    send.finish()?;
    Ok(())
}

async fn handle_catalog_sync(
    send: &mut SendStream,
    recv: &mut RecvStream,
    catalog: Option<Arc<std::sync::Mutex<Catalog>>>,
) -> anyhow::Result<()> {
    let data = recv.read_to_end(4096).await?;
    let req = proto::CatalogSyncRequest::decode_length_delimited(data.as_slice())?;

    let catalog = match catalog {
        Some(cat) => cat,
        None => {
            let resp = proto::CatalogSyncResponse {
                ok: false,
                message: "hub has no catalog configured".into(),
                tapes: vec![],
                files: vec![],
            };
            let encoded = frame::encode_msg(&resp);
            send.write_all(&encoded).await?;
            send.finish()?;
            return Ok(());
        }
    };

    let resp = tokio::task::spawn_blocking(move || {
        let cat = catalog.lock().expect("catalog lock poisoned");

        if req.tape.is_empty() {
            // List tapes
            let tapes = cat.list_tapes("local").unwrap_or_default();
            let tape_infos: Vec<proto::TapeInfo> = tapes
                .into_iter()
                .map(|t| proto::TapeInfo {
                    name: t.tape_name,
                    file_count: t.file_count,
                    total_size: t.total_size,
                })
                .collect();
            proto::CatalogSyncResponse {
                ok: true,
                message: String::new(),
                tapes: tape_infos,
                files: vec![],
            }
        } else {
            // Get all files with chunks for the requested tape
            match cat.get_all_files_with_chunks("local", &req.tape) {
                Ok(files) => {
                    let file_metas: Vec<proto::FileMetadata> = files
                        .into_iter()
                        .map(|f| proto::FileMetadata {
                            path: f.path,
                            entry_type: f.entry_type,
                            size: f.size,
                            mtime: f.mtime.unwrap_or(0),
                            mode: f.mode.unwrap_or(0),
                            version: f.version,
                            chunks: f
                                .chunks
                                .into_iter()
                                .map(|c| proto::ChunkInfo {
                                    hash: c.hash.as_bytes().to_vec(),
                                    offset: c.offset,
                                    size: c.size as u64,
                                })
                                .collect(),
                        })
                        .collect();
                    proto::CatalogSyncResponse {
                        ok: true,
                        message: String::new(),
                        tapes: vec![],
                        files: file_metas,
                    }
                }
                Err(e) => proto::CatalogSyncResponse {
                    ok: false,
                    message: format!("catalog error: {e}"),
                    tapes: vec![],
                    files: vec![],
                },
            }
        }
    })
    .await?;

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

        let hub = Hub::bind(
            "127.0.0.1:0".parse().unwrap(),
            server_config,
            store,
            None,
            None,
        )
        .await
        .unwrap();
        let addr = hub.local_addr();
        assert_ne!(addr.port(), 0);
    }
}
