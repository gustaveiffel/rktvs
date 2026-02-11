// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::{Error, Result};

const MAGIC: [u8; 4] = *b"RKM\x01";
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const CHUNK_ENTRY_SIZE: usize = 48;

/// Reference to a chunk's location in a source file.
#[derive(Debug, Clone)]
pub struct ChunkRef {
    pub path_index: u32,
    pub offset: u64,
    pub length: u32,
}

/// Metadata for a source file tracked in the manifest.
#[derive(Debug, Clone)]
pub struct SourceFile {
    pub path: String,
    pub file_size: u64,
    pub file_mtime: u64,
}

/// Binary manifest mapping BLAKE3 hashes to source file locations.
///
/// Stored as `.rkm` files. Chunks reference original files by
/// (path_index, offset, length) — no data is copied.
pub struct Manifest {
    pub sources: Vec<SourceFile>,
    pub chunks: HashMap<blake3::Hash, ChunkRef>,
}

impl Manifest {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            chunks: HashMap::new(),
        }
    }

    /// Add a source file entry and return its path index.
    pub fn add_source(&mut self, path: String, file_size: u64, file_mtime: u64) -> u32 {
        let index = self.sources.len() as u32;
        self.sources.push(SourceFile {
            path,
            file_size,
            file_mtime,
        });
        index
    }

    /// Look up an existing source by path.
    pub fn source_index(&self, path: &str) -> Option<u32> {
        self.sources
            .iter()
            .position(|s| s.path == path)
            .map(|i| i as u32)
    }

    /// Insert a chunk reference. First-writer-wins dedup: if the hash
    /// already exists, the new reference is silently dropped.
    pub fn insert_chunk(&mut self, hash: blake3::Hash, chunk_ref: ChunkRef) {
        self.chunks.entry(hash).or_insert(chunk_ref);
    }

    pub fn get_chunk(&self, hash: &blake3::Hash) -> Option<&ChunkRef> {
        self.chunks.get(hash)
    }

    pub fn has_chunk(&self, hash: &blake3::Hash) -> bool {
        self.chunks.contains_key(hash)
    }

    /// Write the manifest to disk atomically (write .tmp + rename).
    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let mut buf = Vec::new();

        // Header (32 bytes)
        buf.extend_from_slice(&MAGIC);
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&(self.sources.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        buf.extend_from_slice(&[0u8; 16]); // reserved

        // Path table
        for source in &self.sources {
            let path_bytes = source.path.as_bytes();
            buf.extend_from_slice(&(path_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(path_bytes);
            buf.extend_from_slice(&source.file_size.to_le_bytes());
            buf.extend_from_slice(&source.file_mtime.to_le_bytes());
        }

        // Chunk table (sorted by hash for binary search potential)
        let mut entries: Vec<_> = self.chunks.iter().collect();
        entries.sort_by_key(|(h, _)| *h.as_bytes());

        for (hash, chunk_ref) in &entries {
            buf.extend_from_slice(hash.as_bytes());
            buf.extend_from_slice(&chunk_ref.path_index.to_le_bytes());
            buf.extend_from_slice(&chunk_ref.offset.to_le_bytes());
            buf.extend_from_slice(&chunk_ref.length.to_le_bytes());
        }

        // Atomic write
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("rkm.tmp");
        fs::write(&tmp_path, &buf)?;
        fs::rename(&tmp_path, path)?;

        Ok(())
    }

    /// Read a manifest from disk.
    pub fn read_from_file(path: &Path) -> Result<Self> {
        let data = fs::read(path)?;
        Self::parse(&data)
    }

    fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(Error::ManifestFormat("file too short for header".into()));
        }

        // Validate magic
        if data[0..4] != MAGIC {
            return Err(Error::ManifestFormat(format!(
                "invalid magic: {:?}",
                &data[0..4]
            )));
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != VERSION {
            return Err(Error::ManifestFormat(format!(
                "unsupported version: {version}"
            )));
        }

        // flags at [6..8] — ignored
        let path_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let chunk_count = u32::from_le_bytes([data[12], data[13], data[14], data[15]]) as usize;

        let mut cursor = HEADER_SIZE;

        // Read path table
        let mut sources = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            if cursor + 2 > data.len() {
                return Err(Error::ManifestFormat("truncated path table".into()));
            }
            let path_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
            cursor += 2;

            if cursor + path_len + 16 > data.len() {
                return Err(Error::ManifestFormat("truncated path entry".into()));
            }
            let path = std::str::from_utf8(&data[cursor..cursor + path_len])
                .map_err(|e| Error::ManifestFormat(format!("invalid UTF-8 path: {e}")))?
                .to_string();
            cursor += path_len;

            let file_size = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let file_mtime = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;

            sources.push(SourceFile {
                path,
                file_size,
                file_mtime,
            });
        }

        // Read chunk table
        let remaining = data.len() - cursor;
        if remaining < chunk_count * CHUNK_ENTRY_SIZE {
            return Err(Error::ManifestFormat("truncated chunk table".into()));
        }

        let mut chunks = HashMap::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            let hash_bytes: [u8; 32] = data[cursor..cursor + 32].try_into().unwrap();
            cursor += 32;
            let path_index = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            let offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let length = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;

            let hash = blake3::Hash::from_bytes(hash_bytes);
            chunks.insert(hash, ChunkRef {
                path_index,
                offset,
                length,
            });
        }

        Ok(Manifest { sources, chunks })
    }
}

impl Default for Manifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.rkm");

        let m = Manifest::new();
        m.write_to_file(&path).unwrap();

        let loaded = Manifest::read_from_file(&path).unwrap();
        assert!(loaded.sources.is_empty());
        assert!(loaded.chunks.is_empty());
    }

    #[test]
    fn single_chunk_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.rkm");

        let mut m = Manifest::new();
        let idx = m.add_source("/tmp/test.txt".into(), 1000, 1700000000);
        let hash = blake3::hash(b"test data");
        m.insert_chunk(hash, ChunkRef {
            path_index: idx,
            offset: 0,
            length: 1000,
        });

        m.write_to_file(&path).unwrap();
        let loaded = Manifest::read_from_file(&path).unwrap();

        assert_eq!(loaded.sources.len(), 1);
        assert_eq!(loaded.sources[0].path, "/tmp/test.txt");
        assert_eq!(loaded.sources[0].file_size, 1000);
        assert_eq!(loaded.sources[0].file_mtime, 1700000000);
        assert_eq!(loaded.chunks.len(), 1);

        let cr = loaded.get_chunk(&hash).unwrap();
        assert_eq!(cr.path_index, 0);
        assert_eq!(cr.offset, 0);
        assert_eq!(cr.length, 1000);
    }

    #[test]
    fn multiple_sources_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.rkm");

        let mut m = Manifest::new();
        let idx0 = m.add_source("/a.txt".into(), 500, 100);
        let idx1 = m.add_source("/b.txt".into(), 800, 200);

        let h1 = blake3::hash(b"chunk1");
        let h2 = blake3::hash(b"chunk2");
        let h3 = blake3::hash(b"chunk3");

        m.insert_chunk(h1, ChunkRef { path_index: idx0, offset: 0, length: 500 });
        m.insert_chunk(h2, ChunkRef { path_index: idx1, offset: 0, length: 400 });
        m.insert_chunk(h3, ChunkRef { path_index: idx1, offset: 400, length: 400 });

        m.write_to_file(&path).unwrap();
        let loaded = Manifest::read_from_file(&path).unwrap();

        assert_eq!(loaded.sources.len(), 2);
        assert_eq!(loaded.chunks.len(), 3);
        assert_eq!(loaded.get_chunk(&h2).unwrap().path_index, idx1);
        assert_eq!(loaded.get_chunk(&h3).unwrap().offset, 400);
    }

    #[test]
    fn dedup_first_writer_wins() {
        let mut m = Manifest::new();
        let idx0 = m.add_source("/a.txt".into(), 100, 1);
        let idx1 = m.add_source("/b.txt".into(), 100, 2);

        let hash = blake3::hash(b"shared");
        m.insert_chunk(hash, ChunkRef { path_index: idx0, offset: 0, length: 100 });
        m.insert_chunk(hash, ChunkRef { path_index: idx1, offset: 0, length: 100 });

        let cr = m.get_chunk(&hash).unwrap();
        assert_eq!(cr.path_index, idx0, "first writer should win");
    }

    #[test]
    fn invalid_magic_rejected() {
        let data = b"BAAD\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let result = Manifest::parse(data);
        assert!(matches!(result, Err(Error::ManifestFormat(_))));
    }

    #[test]
    fn atomic_write_no_tmp_leak() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.rkm");

        let m = Manifest::new();
        m.write_to_file(&path).unwrap();

        // No .tmp files should remain
        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(!name.ends_with(".tmp"), "found tmp file: {name}");
        }
    }

    #[test]
    fn has_chunk_works() {
        let mut m = Manifest::new();
        let idx = m.add_source("/x.bin".into(), 50, 1);
        let h = blake3::hash(b"present");
        let absent = blake3::hash(b"absent");

        m.insert_chunk(h, ChunkRef { path_index: idx, offset: 0, length: 50 });
        assert!(m.has_chunk(&h));
        assert!(!m.has_chunk(&absent));
    }

    #[test]
    fn source_index_lookup() {
        let mut m = Manifest::new();
        m.add_source("/first.txt".into(), 100, 1);
        m.add_source("/second.txt".into(), 200, 2);

        assert_eq!(m.source_index("/first.txt"), Some(0));
        assert_eq!(m.source_index("/second.txt"), Some(1));
        assert_eq!(m.source_index("/nope.txt"), None);
    }
}
