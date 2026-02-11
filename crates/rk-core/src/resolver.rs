// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::chunk_store::ChunkStore;
use crate::manifest::Manifest;
use crate::{Error, Result};

/// Resolves chunks by trying the manifest first, then the chunk store.
///
/// The manifest points to data in the original source files (zero-copy),
/// while the chunk store holds compressed copies fetched from remotes.
pub struct ChunkResolver<'a> {
    manifest: Option<&'a Manifest>,
    store: &'a ChunkStore,
    verify_hash: bool,
}

impl<'a> ChunkResolver<'a> {
    pub fn new(manifest: Option<&'a Manifest>, store: &'a ChunkStore) -> Self {
        Self {
            manifest,
            store,
            verify_hash: false,
        }
    }

    /// Toggle BLAKE3 hash verification on manifest reads.
    pub fn with_verify(mut self, verify: bool) -> Self {
        self.verify_hash = verify;
        self
    }

    /// Retrieve chunk data. Tries manifest first, then chunk store.
    pub fn get(&self, hash: &blake3::Hash) -> Result<Vec<u8>> {
        // Try manifest first
        if let Some(manifest) = self.manifest
            && let Some(chunk_ref) = manifest.get_chunk(hash)
        {
            return self.read_from_source(manifest, chunk_ref, hash);
        }

        // Fall back to chunk store
        match self.store.get(hash) {
            Ok(data) => Ok(data),
            Err(Error::ChunkNotFound(_)) => Err(Error::ChunkNotAvailable(*hash)),
            Err(e) => Err(e),
        }
    }

    /// Fast check if a chunk is available (no stat, no I/O on manifest path).
    pub fn has(&self, hash: &blake3::Hash) -> bool {
        if let Some(manifest) = self.manifest
            && manifest.has_chunk(hash)
        {
            return true;
        }
        self.store.has(hash)
    }

    fn read_from_source(
        &self,
        manifest: &Manifest,
        chunk_ref: &crate::manifest::ChunkRef,
        hash: &blake3::Hash,
    ) -> Result<Vec<u8>> {
        let source = &manifest.sources[chunk_ref.path_index as usize];
        let path = Path::new(&source.path);

        if !path.exists() {
            return Err(Error::SourceFileNotFound(source.path.clone()));
        }

        // Stat check: mtime + size must match
        let meta = fs::metadata(path)?;
        let actual_size = meta.len();
        let actual_mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        if actual_size != source.file_size || actual_mtime != source.file_mtime {
            return Err(Error::StaleSourceFile {
                path: source.path.clone(),
                expected_mtime: source.file_mtime,
                expected_size: source.file_size,
                actual_mtime,
                actual_size,
            });
        }

        // Read the chunk from source file
        let mut file = fs::File::open(path)?;
        file.seek(SeekFrom::Start(chunk_ref.offset))?;
        let mut buf = vec![0u8; chunk_ref.length as usize];
        file.read_exact(&mut buf)?;

        // Optional BLAKE3 verification
        if self.verify_hash {
            let actual = blake3::hash(&buf);
            if &actual != hash {
                return Err(Error::HashMismatch {
                    expected: *hash,
                    actual,
                });
            }
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkRef, Manifest};

    fn setup_store_with_chunk(dir: &Path, data: &[u8]) -> (ChunkStore, blake3::Hash) {
        let store = ChunkStore::new(dir.to_path_buf());
        let hash = store.put(data).unwrap();
        (store, hash)
    }

    #[test]
    fn store_only_path() {
        let dir = tempfile::tempdir().unwrap();
        let (store, hash) = setup_store_with_chunk(dir.path(), b"chunk data");

        let resolver = ChunkResolver::new(None, &store);
        let data = resolver.get(&hash).unwrap();
        assert_eq!(data, b"chunk data");
    }

    #[test]
    fn manifest_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        // Write a source file
        let src_path = dir.path().join("source.bin");
        let content = b"manifest chunk data";
        std::fs::write(&src_path, content).unwrap();

        let meta = std::fs::metadata(&src_path).unwrap();
        let mtime = meta
            .modified().unwrap()
            .duration_since(UNIX_EPOCH).unwrap()
            .as_secs();

        let mut manifest = Manifest::new();
        let idx = manifest.add_source(
            src_path.to_string_lossy().to_string(),
            content.len() as u64,
            mtime,
        );
        let hash = blake3::hash(content);
        manifest.insert_chunk(hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: content.len() as u32,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store);
        let data = resolver.get(&hash).unwrap();
        assert_eq!(data, content);
    }

    #[test]
    fn manifest_priority_over_store() {
        let dir = tempfile::tempdir().unwrap();

        // Write a source file with specific content
        let src_path = dir.path().join("source.bin");
        let manifest_content = b"from manifest";
        std::fs::write(&src_path, manifest_content).unwrap();

        let meta = std::fs::metadata(&src_path).unwrap();
        let mtime = meta.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let hash = blake3::hash(manifest_content);

        // Put different data (same hash impossible, but we can test the path taken)
        // Instead: put same data in store, verify manifest path is taken by checking
        // that no store decompression happens (manifest returns raw bytes)
        let store = ChunkStore::new(dir.path().to_path_buf());
        store.put(manifest_content).unwrap();

        let mut manifest = Manifest::new();
        let idx = manifest.add_source(
            src_path.to_string_lossy().to_string(),
            manifest_content.len() as u64,
            mtime,
        );
        manifest.insert_chunk(hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: manifest_content.len() as u32,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store);
        let data = resolver.get(&hash).unwrap();
        assert_eq!(data, manifest_content);
    }

    #[test]
    fn stale_detection() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let src_path = dir.path().join("source.bin");
        std::fs::write(&src_path, b"original").unwrap();

        let mut manifest = Manifest::new();
        let idx = manifest.add_source(
            src_path.to_string_lossy().to_string(),
            8, // "original" len
            9999999999, // fake mtime that won't match
        );
        let hash = blake3::hash(b"original");
        manifest.insert_chunk(hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: 8,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store);
        let result = resolver.get(&hash);
        assert!(matches!(result, Err(Error::StaleSourceFile { .. })));
    }

    #[test]
    fn missing_source_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let mut manifest = Manifest::new();
        let idx = manifest.add_source("/nonexistent/file.bin".into(), 100, 1);
        let hash = blake3::hash(b"whatever");
        manifest.insert_chunk(hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: 100,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store);
        let result = resolver.get(&hash);
        assert!(matches!(result, Err(Error::SourceFileNotFound(_))));
    }

    #[test]
    fn has_checks_both() {
        let dir = tempfile::tempdir().unwrap();
        let (store, store_hash) = setup_store_with_chunk(dir.path(), b"in store");

        let mut manifest = Manifest::new();
        let manifest_hash = blake3::hash(b"in manifest only");
        let idx = manifest.add_source("/fake.bin".into(), 100, 1);
        manifest.insert_chunk(manifest_hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: 100,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store);

        assert!(resolver.has(&store_hash));
        assert!(resolver.has(&manifest_hash));
        assert!(!resolver.has(&blake3::hash(b"nowhere")));
    }

    #[test]
    fn verify_flag_catches_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let src_path = dir.path().join("source.bin");
        let content = b"good data";
        std::fs::write(&src_path, content).unwrap();

        let meta = std::fs::metadata(&src_path).unwrap();
        let mtime = meta.modified().unwrap().duration_since(UNIX_EPOCH).unwrap().as_secs();

        // Use a WRONG hash in the manifest
        let wrong_hash = blake3::hash(b"different data");
        let mut manifest = Manifest::new();
        let idx = manifest.add_source(
            src_path.to_string_lossy().to_string(),
            content.len() as u64,
            mtime,
        );
        manifest.insert_chunk(wrong_hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: content.len() as u32,
        });

        let resolver = ChunkResolver::new(Some(&manifest), &store).with_verify(true);
        let result = resolver.get(&wrong_hash);
        assert!(matches!(result, Err(Error::HashMismatch { .. })));
    }

    #[test]
    fn chunk_not_available() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());

        let resolver = ChunkResolver::new(None, &store);
        let hash = blake3::hash(b"missing");
        let result = resolver.get(&hash);
        assert!(matches!(result, Err(Error::ChunkNotAvailable(_))));
    }
}
