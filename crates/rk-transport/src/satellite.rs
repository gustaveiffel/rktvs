// SPDX-License-Identifier: AGPL-3.0-only

use std::net::SocketAddr;

use prost::Message;
use quinn::{Connection, Endpoint};
use tracing::info;

use crate::frame::{self, StreamTag};
use crate::proto;

/// Maximum response size for a chunk fetch.
/// 16 MB max chunk + protobuf overhead + safety margin.
const MAX_CHUNK_RESPONSE: usize = 17 * 1024 * 1024;

pub struct Satellite {
    connection: Connection,
    pub satellite_id: String,
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
        info!(hub = %hub_addr, "connected to hub");

        // Open control stream and handshake
        let (mut send, mut recv) = connection.open_bi().await?;

        // Write stream tag
        send.write_all(&[StreamTag::Control as u8]).await?;

        // Send handshake
        let handshake = proto::Handshake {
            satellite_id: satellite_id.into(),
            protocol_version: 1,
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
        info!("handshake ok: {}", ack.message);

        Ok(Self {
            connection,
            satellite_id: satellite_id.to_string(),
        })
    }

    /// Fetch a single chunk by BLAKE3 hash from the hub.
    pub async fn fetch_chunk(&self, hash: &blake3::Hash) -> anyhow::Result<Option<Vec<u8>>> {
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
            Ok(Some(resp.data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn satellite_connects_and_handshakes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(rk_core::chunk_store::ChunkStore::new(dir.path().to_path_buf()));

        let (cert, key) = crate::cert::generate_self_signed().unwrap();
        let server_config = crate::cert::server_config(cert.clone(), key).unwrap();
        let client_config = crate::cert::client_config(&cert).unwrap();

        let hub = crate::hub::Hub::bind("127.0.0.1:0".parse().unwrap(), server_config, store, None)
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
