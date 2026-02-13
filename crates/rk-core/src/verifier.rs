// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::UNIX_EPOCH;

use crate::manifest::Manifest;

/// Result of verifying manifest integrity.
#[derive(Debug, Default)]
pub struct VerifyResult {
    pub chunks_checked: usize,
    pub chunks_ok: usize,
    pub chunks_stale: usize,
    pub chunks_missing: usize,
    pub chunks_corrupted: usize,
}

/// Verify integrity of all chunks referenced by the manifest.
///
/// For each chunk: checks that the source file exists and has matching
/// mtime/size. If `blake3_verify` is true, also reads the chunk data
/// and verifies the BLAKE3 hash (slower but catches bit-rot).
pub fn verify_manifest(manifest: &Manifest, blake3_verify: bool) -> VerifyResult {
    let mut result = VerifyResult::default();

    for (hash, chunk_ref) in &manifest.chunks {
        result.chunks_checked += 1;

        let source = &manifest.sources[chunk_ref.path_index as usize];
        let path = Path::new(&source.path);

        if !path.exists() {
            result.chunks_missing += 1;
            continue;
        }

        // Stat check
        let meta = match fs::metadata(path) {
            Ok(m) => m,
            Err(_) => {
                result.chunks_missing += 1;
                continue;
            }
        };

        let actual_size = meta.len();
        let actual_mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        if actual_size != source.file_size || actual_mtime != source.file_mtime {
            result.chunks_stale += 1;
            continue;
        }

        // Optional BLAKE3 verification
        if blake3_verify {
            let verified = (|| -> std::io::Result<bool> {
                let mut file = fs::File::open(path)?;
                file.seek(SeekFrom::Start(chunk_ref.offset))?;
                let mut buf = vec![0u8; chunk_ref.length as usize];
                file.read_exact(&mut buf)?;
                let actual_hash = blake3::hash(&buf);
                Ok(&actual_hash == hash)
            })();

            match verified {
                Ok(true) => result.chunks_ok += 1,
                Ok(false) => result.chunks_corrupted += 1,
                Err(_) => result.chunks_missing += 1,
            }
        } else {
            result.chunks_ok += 1;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkRef, Manifest};

    #[test]
    fn verify_all_ok() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("data.bin");
        let content = b"verify test data";
        fs::write(&src, content).unwrap();

        let meta = fs::metadata(&src).unwrap();
        let mtime = meta
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut m = Manifest::new();
        let idx = m.add_source(
            src.to_string_lossy().to_string(),
            content.len() as u64,
            mtime,
        );
        let hash = blake3::hash(content);
        m.insert_chunk(
            hash,
            ChunkRef {
                path_index: idx,
                offset: 0,
                length: content.len() as u32,
            },
        );

        let result = verify_manifest(&m, false);
        assert_eq!(result.chunks_checked, 1);
        assert_eq!(result.chunks_ok, 1);
        assert_eq!(result.chunks_stale, 0);
        assert_eq!(result.chunks_missing, 0);
    }

    #[test]
    fn verify_with_blake3() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("data.bin");
        let content = b"blake3 verify test";
        fs::write(&src, content).unwrap();

        let meta = fs::metadata(&src).unwrap();
        let mtime = meta
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut m = Manifest::new();
        let idx = m.add_source(
            src.to_string_lossy().to_string(),
            content.len() as u64,
            mtime,
        );
        let hash = blake3::hash(content);
        m.insert_chunk(
            hash,
            ChunkRef {
                path_index: idx,
                offset: 0,
                length: content.len() as u32,
            },
        );

        let result = verify_manifest(&m, true);
        assert_eq!(result.chunks_ok, 1);
        assert_eq!(result.chunks_corrupted, 0);
    }

    #[test]
    fn verify_stale_file() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("data.bin");
        fs::write(&src, b"original").unwrap();

        let mut m = Manifest::new();
        let idx = m.add_source(src.to_string_lossy().to_string(), 8, 9999999999);
        let hash = blake3::hash(b"original");
        m.insert_chunk(
            hash,
            ChunkRef {
                path_index: idx,
                offset: 0,
                length: 8,
            },
        );

        let result = verify_manifest(&m, false);
        assert_eq!(result.chunks_stale, 1);
        assert_eq!(result.chunks_ok, 0);
    }

    #[test]
    fn verify_missing_file() {
        let mut m = Manifest::new();
        let idx = m.add_source("/nonexistent/path.bin".into(), 100, 1);
        let hash = blake3::hash(b"x");
        m.insert_chunk(
            hash,
            ChunkRef {
                path_index: idx,
                offset: 0,
                length: 100,
            },
        );

        let result = verify_manifest(&m, false);
        assert_eq!(result.chunks_missing, 1);
    }

    #[test]
    fn verify_corrupted_data() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("data.bin");
        let content = b"good data here";
        fs::write(&src, content).unwrap();

        let meta = fs::metadata(&src).unwrap();
        let mtime = meta
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut m = Manifest::new();
        let idx = m.add_source(
            src.to_string_lossy().to_string(),
            content.len() as u64,
            mtime,
        );
        // Use a WRONG hash
        let wrong_hash = blake3::hash(b"different data");
        m.insert_chunk(
            wrong_hash,
            ChunkRef {
                path_index: idx,
                offset: 0,
                length: content.len() as u32,
            },
        );

        let result = verify_manifest(&m, true);
        assert_eq!(result.chunks_corrupted, 1);
        assert_eq!(result.chunks_ok, 0);
    }
}
