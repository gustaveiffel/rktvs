// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;

use prost::Message;
use quinn::{Connection, Endpoint};
use tracing::info;

use crate::frame::{self, StreamTag};
use crate::proto;

/// Protocol version this satellite speaks.
pub const PROTOCOL_VERSION: u32 = 5;

/// Minimum hub protocol version this satellite accepts.
const MIN_HUB_VERSION: u32 = 2;

/// Maximum response size for a chunk fetch.
/// 16 MB max chunk + protobuf overhead + safety margin.
const MAX_CHUNK_RESPONSE: usize = 17 * 1024 * 1024;

/// Maximum response size for a catalog sync (4 MB).
const MAX_CATALOG_RESPONSE: usize = 4 * 1024 * 1024;

/// Result of a chunk fetch, preserving the compressed flag from the wire.
pub struct ChunkFetchResult {
    pub data: Vec<u8>,
    pub compressed: bool,
}

/// A file received during catalog sync.
#[derive(Debug)]
pub struct SyncedFile {
    pub path: String,
    pub entry_type: i64,
    pub size: u64,
    pub mtime: u64,
    pub mode: u32,
    pub version: u64,
    pub chunks: Vec<SyncedChunk>,
}

/// A chunk reference received during catalog sync.
#[derive(Debug)]
pub struct SyncedChunk {
    pub hash: blake3::Hash,
    pub offset: u64,
    pub size: u64,
}

pub struct Satellite {
    connection: Connection,
    _endpoint: Endpoint,
    pub satellite_id: String,
    pub hub_protocol_version: u32,
}

impl Satellite {
    /// Connect to a hub, perform handshake.
    pub async fn connect(
        hub_addr: SocketAddr,
        server_name: &str,
        client_config: quinn::ClientConfig,
        satellite_id: &str,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
        endpoint.set_default_client_config(client_config);

        let connection = endpoint.connect(hub_addr, server_name)?.await?;
        info!(hub = %hub_addr, version = env!("CARGO_PKG_VERSION"), "connected to hub");

        // Open control stream and handshake
        let (mut send, mut recv) = connection.open_bi().await?;

        // Write stream tag
        send.write_all(&[StreamTag::Control as u8]).await?;

        // Send handshake
        let handshake = proto::Handshake {
            satellite_id: satellite_id.into(),
            protocol_version: PROTOCOL_VERSION,
        };
        let encoded = frame::encode_msg(&handshake);
        send.write_all(&encoded).await?;
        send.finish()?;

        // Read ack
        let ack_data = recv.read_to_end(4096).await?;
        let ack = proto::HandshakeAck::decode_length_delimited(ack_data.as_slice())?;
        if !ack.ok {
            anyhow::bail!("handshake rejected: {}", ack.message);
        }
        if ack.protocol_version < MIN_HUB_VERSION {
            anyhow::bail!(
                "hub protocol version {} too old, satellite requires >= {} — upgrade the hub",
                ack.protocol_version,
                MIN_HUB_VERSION,
            );
        }
        info!(
            hub_version = ack.protocol_version,
            "handshake ok: {}", ack.message
        );

        Ok(Self {
            connection,
            _endpoint: endpoint,
            satellite_id: satellite_id.to_string(),
            hub_protocol_version: ack.protocol_version,
        })
    }

    /// Fetch a single chunk by BLAKE3 hash from the hub.
    /// Returns `None` if the chunk was not found on the hub.
    /// The `ChunkFetchResult.compressed` flag indicates whether data is zstd-compressed.
    pub async fn fetch_chunk(
        &self,
        hash: &blake3::Hash,
    ) -> anyhow::Result<Option<ChunkFetchResult>> {
        let (mut send, mut recv) = self.connection.open_bi().await?;

        // Write stream tag
        send.write_all(&[StreamTag::ChunkRequest as u8]).await?;

        // Send chunk request
        let req = proto::ChunkRequest {
            hash: hash.as_bytes().to_vec(),
        };
        let encoded = frame::encode_msg(&req);
        send.write_all(&encoded).await?;
        send.finish()?;

        // Read response — chunks can be up to 16 MB + protobuf overhead
        let resp_data = recv.read_to_end(MAX_CHUNK_RESPONSE).await?;
        let resp = proto::ChunkResponse::decode_length_delimited(resp_data.as_slice())?;

        if resp.found {
            Ok(Some(ChunkFetchResult {
                data: resp.data,
                compressed: resp.compressed,
            }))
        } else {
            Ok(None)
        }
    }
    /// Push a single compressed chunk to the hub.
    /// Returns true if the hub accepted the chunk.
    pub async fn push_chunk(
        &self,
        hash: &blake3::Hash,
        compressed_data: &[u8],
        decompressed_size: u64,
    ) -> anyhow::Result<bool> {
        let (mut send, mut recv) = self.connection.open_bi().await?;

        send.write_all(&[StreamTag::ChunkPush as u8]).await?;

        let req = proto::ChunkPushRequest {
            hash: hash.as_bytes().to_vec(),
            compressed_data: compressed_data.to_vec(),
            decompressed_size,
        };
        let encoded = frame::encode_msg(&req);
        send.write_all(&encoded).await?;
        send.finish()?;

        let resp_data = recv.read_to_end(4096).await?;
        let resp = proto::ChunkPushResponse::decode_length_delimited(resp_data.as_slice())?;

        if !resp.ok {
            anyhow::bail!("chunk push rejected: {}", resp.message);
        }
        Ok(true)
    }

    /// Push a file manifest to the hub after all chunks have been pushed.
    /// `chunks` is a list of (hash, offset, size) tuples.
    pub async fn push_manifest(
        &self,
        filename: &str,
        file_size: u64,
        chunks: &[(blake3::Hash, u64, u64)],
    ) -> anyhow::Result<bool> {
        let (mut send, mut recv) = self.connection.open_bi().await?;

        send.write_all(&[StreamTag::ManifestPush as u8]).await?;

        let req = proto::ManifestPushRequest {
            filename: filename.into(),
            file_size,
            chunks: chunks
                .iter()
                .map(|(hash, offset, size)| proto::ChunkInfo {
                    hash: hash.as_bytes().to_vec(),
                    offset: *offset,
                    size: *size,
                })
                .collect(),
        };
        let encoded = frame::encode_msg(&req);
        send.write_all(&encoded).await?;
        send.finish()?;

        let resp_data = recv.read_to_end(4096).await?;
        let resp = proto::ManifestPushResponse::decode_length_delimited(resp_data.as_slice())?;

        if !resp.ok {
            anyhow::bail!("manifest push rejected: {}", resp.message);
        }
        Ok(true)
    }

    /// Ask the hub which chunks it already has.
    /// Returns a Vec<bool> in the same order as the input hashes.
    pub async fn have_check(&self, hashes: &[blake3::Hash]) -> anyhow::Result<Vec<bool>> {
        let (mut send, mut recv) = self.connection.open_bi().await?;

        send.write_all(&[StreamTag::HaveCheck as u8]).await?;

        let req = proto::HaveCheckRequest {
            chunk_hashes: hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
        };
        let encoded = frame::encode_msg(&req);
        send.write_all(&encoded).await?;
        send.finish()?;

        let resp_data = recv.read_to_end(hashes.len() + 1024).await?;
        let resp = proto::HaveCheckResponse::decode_length_delimited(resp_data.as_slice())?;

        Ok(resp.have)
    }

    /// Sync catalog metadata from the hub.
    ///
    /// If `tape` is empty, lists available tapes and returns `(tapes, [])`.
    /// If `tape` is set, returns `([], files)` with full chunk lists.
    ///
    /// Tapes are returned as `(name, file_count, total_size)` tuples.
    pub async fn sync_catalog(
        &self,
        tape: &str,
    ) -> anyhow::Result<(Vec<(String, u64, u64)>, Vec<SyncedFile>)> {
        if self.hub_protocol_version < 3 {
            anyhow::bail!(
                "hub protocol version {} does not support catalog sync (requires >= 3)",
                self.hub_protocol_version,
            );
        }

        let (mut send, mut recv) = self.connection.open_bi().await?;

        // Write stream tag
        send.write_all(&[StreamTag::CatalogSync as u8]).await?;

        // Send request
        let req = proto::CatalogSyncRequest { tape: tape.into() };
        let encoded = frame::encode_msg(&req);
        send.write_all(&encoded).await?;
        send.finish()?;

        // Read response
        let resp_data = recv.read_to_end(MAX_CATALOG_RESPONSE).await?;
        let resp = proto::CatalogSyncResponse::decode_length_delimited(resp_data.as_slice())?;

        if !resp.ok {
            anyhow::bail!("catalog sync failed: {}", resp.message);
        }

        let tapes: Vec<(String, u64, u64)> = resp
            .tapes
            .into_iter()
            .map(|t| (t.name, t.file_count, t.total_size))
            .collect();

        let files: Vec<SyncedFile> = resp
            .files
            .into_iter()
            .map(|f| {
                let chunks: Vec<SyncedChunk> = f
                    .chunks
                    .into_iter()
                    .filter_map(|c| {
                        let hash_array: [u8; 32] = c.hash.as_slice().try_into().ok()?;
                        Some(SyncedChunk {
                            hash: blake3::Hash::from_bytes(hash_array),
                            offset: c.offset,
                            size: c.size,
                        })
                    })
                    .collect();
                SyncedFile {
                    path: f.path,
                    entry_type: f.entry_type,
                    size: f.size,
                    mtime: f.mtime,
                    mode: f.mode,
                    version: f.version,
                    chunks,
                }
            })
            .collect();

        Ok((tapes, files))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn satellite_connects_and_handshakes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(rk_core::chunk_store::ChunkStore::new(
            dir.path().to_path_buf(),
        ));

        let (cert, key) = crate::cert::generate_self_signed().unwrap();
        let server_config = crate::cert::server_config(cert.clone(), key).unwrap();
        let client_config = crate::cert::client_config(&cert).unwrap();

        let hub = crate::hub::Hub::bind(
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

        let sat = Satellite::connect(hub_addr, "localhost", client_config, "test-sat")
            .await
            .unwrap();
        assert_eq!(sat.satellite_id, "test-sat");
    }
}
