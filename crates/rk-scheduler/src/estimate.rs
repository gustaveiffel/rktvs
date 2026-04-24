// SPDX-License-Identifier: AGPL-3.0-only

use rk_core::catalog::Catalog;
use rk_core::resolver::ChunkResolver;

/// Result of estimating a file transfer.
#[derive(Debug, Clone)]
pub struct Estimate {
    pub total_chunks: usize,
    pub local_chunks: usize,
    pub missing_chunks: usize,
    pub total_bytes: u64,
    pub transfer_bytes: u64,
    /// Estimated wire transfer size accounting for zstd compression.
    pub compressed_transfer_bytes: u64,
}

/// Default compression ratio when compressed_size is unknown (0.65 = 35% savings).
const DEFAULT_COMPRESSION_RATIO: f64 = 0.65;

/// Estimate the cost of fetching a file without transferring anything.
pub fn estimate_file(
    catalog: &Catalog,
    resolver: &ChunkResolver,
    library_id: &str,
    tape: &str,
    path: &str,
) -> anyhow::Result<Estimate> {
    let chunks = catalog.get_file_chunks(library_id, tape, path)?;

    let total_chunks = chunks.len();
    let mut local_chunks = 0usize;
    let mut missing_chunks = 0usize;
    let mut total_bytes = 0u64;
    let mut transfer_bytes = 0u64;
    let mut compressed_transfer_bytes = 0u64;

    for chunk in &chunks {
        total_bytes += chunk.size as u64;
        if resolver.has(&chunk.hash) {
            local_chunks += 1;
        } else {
            missing_chunks += 1;
            transfer_bytes += chunk.size as u64;
            if chunk.compressed_size > 0 {
                compressed_transfer_bytes += chunk.compressed_size as u64;
            } else {
                compressed_transfer_bytes += (chunk.size as f64 * DEFAULT_COMPRESSION_RATIO) as u64;
            }
        }
    }

    Ok(Estimate {
        total_chunks,
        local_chunks,
        missing_chunks,
        total_bytes,
        transfer_bytes,
        compressed_transfer_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::chunk_store::ChunkStore;
    use rk_core::chunker;

    #[test]
    fn estimate_with_partial_local_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let result = chunker::ingest(&data, &store, 4_096, 8_192, 16_384).unwrap();
        catalog
            .record_file(
                "lib-a",
                "tape-1",
                "/test.bin",
                1,
                data.len() as u64,
                None,
                None,
                1,
                &result.chunks,
            )
            .unwrap();

        let resolver = ChunkResolver::new(None, &store);
        let est = estimate_file(&catalog, &resolver, "lib-a", "tape-1", "/test.bin").unwrap();
        assert_eq!(est.total_chunks, result.chunks.len());
        assert_eq!(est.local_chunks, result.chunks.len());
        assert_eq!(est.missing_chunks, 0);
        assert_eq!(est.transfer_bytes, 0);
        assert_eq!(est.total_bytes, data.len() as u64);
    }

    #[test]
    fn estimate_with_no_local_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store_ingest = ChunkStore::new(dir.path().join("ingest").to_path_buf());
        let store_local = ChunkStore::new(dir.path().join("local").to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let result = chunker::ingest(&data, &store_ingest, 4_096, 8_192, 16_384).unwrap();
        catalog
            .record_file(
                "lib-a",
                "tape-1",
                "/remote.bin",
                1,
                data.len() as u64,
                None,
                None,
                1,
                &result.chunks,
            )
            .unwrap();

        let resolver = ChunkResolver::new(None, &store_local);
        let est = estimate_file(&catalog, &resolver, "lib-a", "tape-1", "/remote.bin").unwrap();
        assert_eq!(est.total_chunks, result.chunks.len());
        assert_eq!(est.local_chunks, 0);
        assert_eq!(est.missing_chunks, result.chunks.len());
        assert!(est.transfer_bytes > 0);
    }

    #[test]
    fn estimate_uses_compressed_size() {
        let dir = tempfile::tempdir().unwrap();
        let store_ingest = ChunkStore::new(dir.path().join("ingest").to_path_buf());
        let store_local = ChunkStore::new(dir.path().join("local").to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let result = chunker::ingest(&data, &store_ingest, 4_096, 8_192, 16_384).unwrap();
        catalog
            .record_file(
                "lib-a",
                "tape-1",
                "/remote.bin",
                1,
                data.len() as u64,
                None,
                None,
                1,
                &result.chunks,
            )
            .unwrap();

        let resolver = ChunkResolver::new(None, &store_local);
        let est = estimate_file(&catalog, &resolver, "lib-a", "tape-1", "/remote.bin").unwrap();

        // compressed_transfer_bytes should be less than transfer_bytes
        assert!(
            est.compressed_transfer_bytes < est.transfer_bytes,
            "compressed {} should be < raw {}",
            est.compressed_transfer_bytes,
            est.transfer_bytes
        );
        assert!(est.compressed_transfer_bytes > 0);
    }

    /// Synced data has compressed_size=0 in file_chunks (no chunks table row).
    /// Estimation must use DEFAULT_COMPRESSION_RATIO fallback, not crash.
    #[test]
    fn estimate_falls_back_when_compressed_size_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store_local = ChunkStore::new(dir.path().join("local").to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        // Simulate synced metadata: record_file with compressed_size=0
        let hash = blake3::hash(b"synced-chunk");
        let chunks = vec![rk_core::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: 10000,
            compressed_size: 0, // satellite sync sets this to 0
        }];
        catalog
            .record_file(
                "lib-a",
                "tape-1",
                "/synced.bin",
                1,
                10000,
                None,
                None,
                1,
                &chunks,
            )
            .unwrap();

        let resolver = ChunkResolver::new(None, &store_local);
        let est = estimate_file(&catalog, &resolver, "lib-a", "tape-1", "/synced.bin").unwrap();

        assert_eq!(est.missing_chunks, 1);
        assert_eq!(est.transfer_bytes, 10000);
        // Fallback: 10000 * 0.65 = 6500
        assert_eq!(est.compressed_transfer_bytes, 6500);
    }
}
