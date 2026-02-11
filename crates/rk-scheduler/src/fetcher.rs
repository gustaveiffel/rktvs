// SPDX-License-Identifier: AGPL-3.0-only

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;
use rk_transport::satellite::Satellite;
use tracing::info;

/// Result of a fetch operation.
#[derive(Debug)]
pub struct FetchResult {
    pub total_chunks: usize,
    pub fetched_chunks: usize,
    pub skipped_chunks: usize,
    pub bytes_transferred: u64,
}

/// Fetch all missing chunks for a file from the hub.
///
/// Chunks already present in the local store are skipped (resume support).
/// Updates job progress in the catalog after each chunk.
pub async fn fetch_file(
    satellite: &Satellite,
    store: &ChunkStore,
    catalog: &Catalog,
    library_id: &str,
    tape: &str,
    path: &str,
    job_id: Option<&str>,
) -> anyhow::Result<FetchResult> {
    let chunks = catalog.get_file_chunks(library_id, tape, path)?;
    let total_chunks = chunks.len();
    let mut fetched_chunks = 0usize;
    let mut skipped_chunks = 0usize;
    let mut bytes_transferred = 0u64;

    for (i, chunk) in chunks.iter().enumerate() {
        if store.has(&chunk.hash) {
            skipped_chunks += 1;
            info!(hash = %chunk.hash, "chunk already local, skipping");
        } else {
            let data = satellite
                .fetch_chunk(&chunk.hash)
                .await?
                .ok_or_else(|| anyhow::anyhow!("hub does not have chunk {}", chunk.hash))?;

            // Verify hash before storing
            let actual_hash = blake3::hash(&data);
            if actual_hash != chunk.hash {
                anyhow::bail!(
                    "hash mismatch for chunk {}: expected {}, got {}",
                    i, chunk.hash, actual_hash
                );
            }

            store.put(&data)?;
            fetched_chunks += 1;
            bytes_transferred += data.len() as u64;
            info!(hash = %chunk.hash, size = data.len(), "fetched chunk {}/{}", i + 1, total_chunks);
        }

        // Update job progress if tracking
        if let Some(jid) = job_id {
            let completed = (skipped_chunks + fetched_chunks) as i64;
            catalog.update_job_progress(jid, completed, "running")?;
        }
    }

    // Mark job complete
    if let Some(jid) = job_id {
        catalog.update_job_progress(jid, total_chunks as i64, "completed")?;
    }

    Ok(FetchResult {
        total_chunks,
        fetched_chunks,
        skipped_chunks,
        bytes_transferred,
    })
}
