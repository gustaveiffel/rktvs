// SPDX-License-Identifier: AGPL-3.0-only

//! Per-tape zstd dictionary training and management.

use std::fs;
use std::path::{Path, PathBuf};

use crate::chunk_store::ChunkStore;
use crate::Result;

/// Maximum number of samples used for dictionary training.
const MAX_SAMPLES: usize = 100;

/// Maximum sample size (16 KB per sample).
const MAX_SAMPLE_SIZE: usize = 16 * 1024;

/// Maximum dictionary size (112 KB — zstd recommended max).
const MAX_DICT_SIZE: usize = 112 * 1024;

/// Train a zstd dictionary from chunk samples and save it.
pub fn train_dict(
    store: &ChunkStore,
    sample_hashes: &[blake3::Hash],
    dict_dir: &Path,
    tape: &str,
) -> Result<usize> {
    let mut samples = Vec::new();
    let mut sample_sizes = Vec::new();

    for hash in sample_hashes.iter().take(MAX_SAMPLES) {
        if let Ok(data) = store.get(hash) {
            let len = data.len().min(MAX_SAMPLE_SIZE);
            samples.extend_from_slice(&data[..len]);
            sample_sizes.push(len);
        }
    }

    if samples.is_empty() {
        return Err(crate::Error::Other("no samples available for dictionary training".into()));
    }

    let dict = zstd::dict::from_continuous(&samples, &sample_sizes, MAX_DICT_SIZE)
        .map_err(|e| crate::Error::Other(format!("dictionary training failed: {e}")))?;

    fs::create_dir_all(dict_dir)?;
    let path = dict_path(dict_dir, tape);
    fs::write(&path, &dict)?;

    Ok(dict.len())
}

/// Load a dictionary for a tape, if it exists.
pub fn load_dict(dict_dir: &Path, tape: &str) -> Result<Option<Vec<u8>>> {
    let path = dict_path(dict_dir, tape);
    if path.exists() {
        Ok(Some(fs::read(&path)?))
    } else {
        Ok(None)
    }
}

/// Path to the dictionary file for a tape.
pub fn dict_path(dict_dir: &Path, tape: &str) -> PathBuf {
    dict_dir.join(format!("{tape}.zdict"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_store::ChunkStore;

    #[test]
    fn train_and_load_dict() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().join("store").to_path_buf());

        // Ingest several similar files to train on
        let mut samples = Vec::new();
        for i in 0..20 {
            let content = format!(
                "{{\"id\": {i}, \"name\": \"file_{i}\", \"type\": \"document\", \"size\": {}}}\n",
                i * 100
            );
            let hash = store.put(content.as_bytes()).unwrap();
            samples.push(hash);
        }

        let dict_dir = dir.path().join("dicts");
        train_dict(&store, &samples, &dict_dir, "docs").unwrap();

        assert!(dict_path(&dict_dir, "docs").exists());
        let dict_data = load_dict(&dict_dir, "docs").unwrap();
        assert!(dict_data.is_some());
        assert!(dict_data.unwrap().len() > 0);
    }

    #[test]
    fn load_missing_dict_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_dict(dir.path(), "nonexistent").unwrap().is_none());
    }
}
