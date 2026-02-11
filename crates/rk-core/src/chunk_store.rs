// SPDX-License-Identifier: AGPL-3.0-only

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
    ///
    /// Writes are atomic: data goes to a temp file first, then renamed
    /// into place. A crash mid-write cannot produce a corrupt chunk.
    pub fn put(&self, data: &[u8]) -> Result<blake3::Hash> {
        let hash = blake3::hash(data);
        let path = self.chunk_path(&hash);
        if path.exists() {
            return Ok(hash);
        }
        let parent = path.parent().unwrap();
        fs::create_dir_all(parent)?;
        let compressed = zstd::encode_all(data, 3)?;

        // Atomic write: temp file + rename
        let tmp_path = parent.join(format!("{}.zst.tmp", hash.to_hex()));
        fs::write(&tmp_path, &compressed)?;
        fs::rename(&tmp_path, &path)?;

        Ok(hash)
    }

    /// Check if a chunk exists in the store.
    pub fn has(&self, hash: &blake3::Hash) -> bool {
        self.chunk_path(hash).exists()
    }

    /// Retrieve and decompress a chunk. Verifies BLAKE3 hash on read.
    pub fn get(&self, hash: &blake3::Hash) -> Result<Vec<u8>> {
        let path = self.chunk_path(hash);
        if !path.exists() {
            return Err(crate::Error::ChunkNotFound(*hash));
        }
        let compressed = fs::read(&path)?;
        let data = zstd::decode_all(compressed.as_slice())?;
        let actual = blake3::hash(&data);
        if &actual != hash {
            return Err(crate::Error::HashMismatch {
                expected: *hash,
                actual,
            });
        }
        Ok(data)
    }

    /// Get the compressed size of a stored chunk in bytes.
    pub fn compressed_size(&self, hash: &blake3::Hash) -> Result<usize> {
        let path = self.chunk_path(hash);
        if !path.exists() {
            return Err(crate::Error::ChunkNotFound(*hash));
        }
        let meta = fs::metadata(&path)?;
        Ok(meta.len() as usize)
    }

    pub fn chunk_path(&self, hash: &blake3::Hash) -> PathBuf {
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

    #[test]
    fn put_then_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let data = b"some chunk content for testing retrieval";
        let hash = store.put(data).unwrap();

        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn get_nonexistent_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let fake = blake3::hash(b"nonexistent");
        let result = store.get(&fake);
        assert!(result.is_err());
    }

    #[test]
    fn get_verifies_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let data = b"original data";
        let hash = store.put(data).unwrap();

        // corrupt the file on disk
        let path = store.chunk_path(&hash);
        let corrupted = zstd::encode_all(&b"tampered"[..], 3).unwrap();
        std::fs::write(&path, &corrupted).unwrap();

        let result = store.get(&hash);
        assert!(matches!(result, Err(crate::Error::HashMismatch { .. })));
    }

    #[test]
    fn put_does_not_leave_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let data = b"temp file cleanup test";
        let _hash = store.put(data).unwrap();

        // Walk the chunks directory — no .tmp files should exist
        fn check_no_tmp(dir: &std::path::Path) {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    assert!(
                        !path.extension().is_some_and(|e| e == "tmp"),
                        "found temp file: {}",
                        path.display()
                    );
                    if path.is_dir() {
                        check_no_tmp(&path);
                    }
                }
            }
        }
        check_no_tmp(&dir.path().join("chunks"));
    }
}
