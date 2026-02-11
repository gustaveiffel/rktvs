use std::fs;
use std::path::PathBuf;

use crate::Result;

/// Content-addressed chunk storage on disk.
///
/// Chunks are stored zstd-compressed, keyed by BLAKE3 hash.
/// Layout: `<base>/chunks/<first 2 hex chars>/<full hash>.zst`
pub struct ChunkStore {
    base: PathBuf,
}

impl ChunkStore {
    pub fn new(base: PathBuf) -> Self {
        Self { base }
    }

    /// Store raw chunk data. Returns the BLAKE3 hash.
    pub fn put(&self, data: &[u8]) -> Result<blake3::Hash> {
        let hash = blake3::hash(data);
        let path = self.chunk_path(&hash);
        if path.exists() {
            return Ok(hash);
        }
        fs::create_dir_all(path.parent().unwrap())?;
        let compressed = zstd::encode_all(data, 3)?;
        fs::write(&path, &compressed)?;
        Ok(hash)
    }

    /// Check if a chunk exists in the store.
    pub fn has(&self, hash: &blake3::Hash) -> bool {
        self.chunk_path(hash).exists()
    }

    fn chunk_path(&self, hash: &blake3::Hash) -> PathBuf {
        let hex = hash.to_hex();
        let hex = hex.as_str();
        self.base.join("chunks").join(&hex[..2]).join(format!("{hex}.zst"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_has() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let data = b"hello world, this is chunk data";
        let hash = store.put(data).unwrap();

        // hash should be the BLAKE3 hash of the data
        assert_eq!(hash, blake3::hash(data));

        // store should report it has this chunk
        assert!(store.has(&hash));

        // store should not have a random hash
        let fake = blake3::hash(b"nonexistent");
        assert!(!store.has(&fake));
    }
}
