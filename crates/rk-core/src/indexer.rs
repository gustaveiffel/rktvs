// SPDX-License-Identifier: AGPL-3.0-only

use std::fs;
use std::path::Path;

use crate::catalog::Catalog;
use crate::chunker;
use crate::manifest::{ChunkRef, Manifest};
use crate::Result;

/// Statistics returned after indexing a directory.
#[derive(Debug, Default)]
pub struct IndexStats {
    pub files_indexed: usize,
    pub dirs_found: usize,
    pub chunks_total: usize,
    pub chunks_new: usize,
    pub chunks_dedup: usize,
    pub total_bytes: u64,
}

/// Configuration for the indexer's chunk sizes.
pub struct IndexConfig {
    pub min_chunk: u32,
    pub avg_chunk: u32,
    pub max_chunk: u32,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            min_chunk: 1_048_576,  // 1 MB
            avg_chunk: 4_194_304,  // 4 MB
            max_chunk: 16_777_216, // 16 MB
        }
    }
}

/// Walk a directory, chunk files in place (no data copy), and record
/// references in the manifest and catalog.
pub fn index_directory(
    root_path: &Path,
    manifest: &mut Manifest,
    catalog: &Catalog,
    library_id: &str,
    tape: &str,
    config: &IndexConfig,
) -> Result<IndexStats> {
    let root_path = root_path.canonicalize()?;
    let mut stats = IndexStats::default();

    for entry in walkdir::WalkDir::new(&root_path).follow_links(false) {
        let entry = entry.map_err(|e| std::io::Error::other(e.to_string()))?;
        let abs_path = entry.path();

        // Compute relative path for catalog (strip root prefix)
        let rel_path = abs_path
            .strip_prefix(&root_path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        // Skip the root itself
        if rel_path.as_os_str().is_empty() {
            continue;
        }

        let rel_str = rel_path.to_string_lossy().to_string();
        let ft = entry.file_type();

        if ft.is_dir() {
            let meta = fs::metadata(abs_path)?;
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs());
            catalog.record_file(library_id, tape, &rel_str, 2, 0, mtime, None, 1, &[])?;
            stats.dirs_found += 1;
        } else if ft.is_file() {
            let meta = fs::metadata(abs_path)?;
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs());

            // Read file data for chunking
            let data = fs::read(abs_path)?;

            // Chunk without writing to store
            let chunk_metas = chunker::chunk_data(
                &data,
                config.min_chunk,
                config.avg_chunk,
                config.max_chunk,
            );

            // Register source file in manifest
            let abs_str = abs_path.to_string_lossy().to_string();
            let path_index = manifest
                .source_index(&abs_str)
                .unwrap_or_else(|| manifest.add_source(abs_str, size, mtime.unwrap_or(0)));

            // Record each chunk in manifest (first-writer-wins dedup)
            for cm in &chunk_metas {
                let was_new = !manifest.has_chunk(&cm.hash);
                manifest.insert_chunk(
                    cm.hash,
                    ChunkRef {
                        path_index,
                        offset: cm.offset,
                        length: cm.size as u32,
                    },
                );
                stats.chunks_total += 1;
                if was_new {
                    stats.chunks_new += 1;
                } else {
                    stats.chunks_dedup += 1;
                }
            }

            // Record in catalog (same as tar ingest does)
            catalog.record_file(
                library_id,
                tape,
                &rel_str,
                1,
                size,
                mtime,
                None,
                1,
                &chunk_metas,
            )?;

            stats.files_indexed += 1;
            stats.total_bytes += size;
        }
        // Skip symlinks and other entry types
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Catalog;
    use crate::manifest::Manifest;
    use std::fs;

    fn make_test_dir(dir: &Path) {
        fs::create_dir_all(dir.join("sub")).unwrap();
        fs::write(dir.join("hello.txt"), b"hello world").unwrap();
        fs::write(dir.join("sub/nested.txt"), b"nested content here").unwrap();
    }

    #[test]
    fn index_single_file() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("test.txt"), b"test data for indexing").unwrap();

        let mut manifest = Manifest::new();
        let catalog = Catalog::open_in_memory().unwrap();
        let config = IndexConfig {
            min_chunk: 512,
            avg_chunk: 1024,
            max_chunk: 2048,
        };

        let stats = index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();
        assert_eq!(stats.files_indexed, 1);
        assert_eq!(stats.chunks_total, 1);
        assert_eq!(stats.chunks_new, 1);
        assert_eq!(stats.chunks_dedup, 0);
        assert!(manifest.sources.len() >= 1);
    }

    #[test]
    fn index_directory_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        make_test_dir(&root);

        let mut manifest = Manifest::new();
        let catalog = Catalog::open_in_memory().unwrap();
        let config = IndexConfig {
            min_chunk: 512,
            avg_chunk: 1024,
            max_chunk: 2048,
        };

        let stats = index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();
        assert_eq!(stats.files_indexed, 2);
        assert_eq!(stats.dirs_found, 1); // "sub"

        let files = catalog.list_files("local", "t", "/").unwrap();
        // Should have 2 files + 1 dir
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn index_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("empty.dat"), b"").unwrap();

        let mut manifest = Manifest::new();
        let catalog = Catalog::open_in_memory().unwrap();
        let config = IndexConfig::default();

        let stats = index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();
        assert_eq!(stats.files_indexed, 1);
        assert_eq!(stats.total_bytes, 0);
    }

    #[test]
    fn dedup_across_identical_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        fs::create_dir_all(&root).unwrap();
        let content = b"identical content in both files";
        fs::write(root.join("a.txt"), content).unwrap();
        fs::write(root.join("b.txt"), content).unwrap();

        let mut manifest = Manifest::new();
        let catalog = Catalog::open_in_memory().unwrap();
        let config = IndexConfig {
            min_chunk: 512,
            avg_chunk: 1024,
            max_chunk: 2048,
        };

        let stats = index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();
        assert_eq!(stats.files_indexed, 2);
        assert_eq!(stats.chunks_total, 2);
        assert_eq!(stats.chunks_new, 1);
        assert_eq!(stats.chunks_dedup, 1);
    }

    #[test]
    fn symlink_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("real.txt"), b"real file").unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.join("real.txt"), root.join("link.txt")).unwrap();
        }

        let mut manifest = Manifest::new();
        let catalog = Catalog::open_in_memory().unwrap();
        let config = IndexConfig::default();

        let stats = index_directory(&root, &mut manifest, &catalog, "local", "t", &config).unwrap();
        // Only the real file, not the symlink
        assert_eq!(stats.files_indexed, 1);
    }
}
