use crate::Result;

/// Metadata for a single chunk produced by the chunker.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub hash: blake3::Hash,
    pub offset: u64,
    pub size: usize,
    pub compressed_size: usize,
}

/// Result of chunking and ingesting a file.
#[derive(Debug)]
pub struct IngestResult {
    pub chunks: Vec<ChunkMeta>,
    pub total_size: u64,
}

/// Split data into content-defined chunks using FastCDC, hash each with BLAKE3.
///
/// Returns chunk metadata with hash, offset, and size.
/// `compressed_size` is set to 0 here — it gets filled in during store.
pub fn chunk_data(data: &[u8], min_size: u32, avg_size: u32, max_size: u32) -> Vec<ChunkMeta> {
    use fastcdc::v2020::FastCDC;

    let chunker = FastCDC::new(data, min_size, avg_size, max_size);
    chunker
        .map(|entry| {
            let slice = &data[entry.offset as usize..entry.offset as usize + entry.length];
            ChunkMeta {
                hash: blake3::hash(slice),
                offset: entry.offset as u64,
                size: entry.length,
                compressed_size: 0,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_small_data() {
        // Data smaller than min chunk size -> produces exactly 1 chunk
        let data: Vec<u8> = vec![42; 1024];
        let result = chunk_data(&data, 512, 1024, 2048);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, blake3::hash(&data));
        assert_eq!(result[0].size, 1024);
        assert_eq!(result[0].offset, 0);
    }

    #[test]
    fn chunk_large_data_produces_multiple() {
        // 256 KB of data with 8/16/32 KB chunk sizes -> multiple chunks
        let data: Vec<u8> = (0..262_144_u32).map(|i| (i % 256) as u8).collect();
        let result = chunk_data(&data, 8_192, 16_384, 32_768);
        assert!(result.len() > 1, "expected multiple chunks, got {}", result.len());

        // all chunks should cover the full data without gaps
        let total: u64 = result.iter().map(|c| c.size as u64).sum();
        assert_eq!(total, data.len() as u64);

        // offsets should be sequential
        let mut expected_offset = 0u64;
        for chunk in &result {
            assert_eq!(chunk.offset, expected_offset);
            expected_offset += chunk.size as u64;
        }
    }

    #[test]
    fn each_chunk_hash_matches_its_data() {
        let data: Vec<u8> = (0..100_000_u32).map(|i| (i % 251) as u8).collect();
        let result = chunk_data(&data, 4_096, 8_192, 16_384);
        for chunk in &result {
            let start = chunk.offset as usize;
            let end = start + chunk.size;
            let slice = &data[start..end];
            assert_eq!(chunk.hash, blake3::hash(slice));
        }
    }
}
