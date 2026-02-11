use std::io::Read;

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;

/// Default chunk sizes for ingestion.
pub const MIN_CHUNK_SIZE: u32 = 1_048_576;   // 1 MB
pub const AVG_CHUNK_SIZE: u32 = 4_194_304;   // 4 MB
pub const MAX_CHUNK_SIZE: u32 = 16_777_216;  // 16 MB

/// Statistics returned after ingesting a tar archive.
#[derive(Debug, Default)]
pub struct IngestStats {
    pub files: usize,
    pub dirs: usize,
    pub bytes: u64,
    pub chunks: usize,
}

/// Ingest a tar archive into the chunk store and catalog.
pub fn ingest_tar<R: Read>(
    reader: R,
    store: &ChunkStore,
    catalog: &Catalog,
    library_id: &str,
    tape: &str,
    min_chunk: u32,
    avg_chunk: u32,
    max_chunk: u32,
) -> rk_core::Result<IngestStats> {
    let mut archive = tar::Archive::new(reader);
    let mut stats = IngestStats::default();

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().to_string();
        let header = entry.header();
        let entry_type = header.entry_type();

        match entry_type {
            tar::EntryType::Regular | tar::EntryType::Continuous => {
                let size = header.size()?;
                let mtime = header.mtime().ok();
                let mode = header.mode().ok();

                let mut data = Vec::with_capacity(size as usize);
                entry.read_to_end(&mut data)?;

                let result = rk_core::chunker::ingest(&data, store, min_chunk, avg_chunk, max_chunk)?;
                catalog.record_file(library_id, tape, &path, 1, size, mtime, mode, 1, &result.chunks)?;

                stats.files += 1;
                stats.bytes += size;
                stats.chunks += result.chunks.len();
            }
            tar::EntryType::Directory => {
                let mtime = header.mtime().ok();
                let mode = header.mode().ok();
                catalog.record_file(library_id, tape, &path, 2, 0, mtime, mode, 1, &[])?;
                stats.dirs += 1;
            }
            _ => {
                // Skip symlinks, devices, etc. for MVP
            }
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Helper: create a tar archive in memory with given files.
    fn make_tar(entries: &[(&str, &[u8], u32, u64)]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut buf);
            for &(path, data, mode, mtime) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_path(path).unwrap();
                header.set_size(data.len() as u64);
                header.set_mode(mode);
                header.set_mtime(mtime);
                header.set_entry_type(tar::EntryType::Regular);
                header.set_cksum();
                builder.append(&header, data).unwrap();
            }
            builder.finish().unwrap();
        }
        buf
    }

    #[test]
    fn ingest_tar_stores_files_and_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let file_a = b"Hello, this is file A content for testing.";
        let file_b = b"And this is file B with different content!";
        let tar_data = make_tar(&[
            ("dir/a.txt", file_a, 0o644, 1700000000),
            ("dir/b.txt", file_b, 0o755, 1700000001),
        ]);

        let stats = ingest_tar(
            Cursor::new(&tar_data),
            &store,
            &catalog,
            "local",
            "test",
            MIN_CHUNK_SIZE,
            AVG_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
        )
        .unwrap();

        assert_eq!(stats.files, 2);
        assert_eq!(stats.bytes, (file_a.len() + file_b.len()) as u64);

        // Files should be in catalog
        let files = catalog.list_files("local", "test", "/").unwrap();
        assert_eq!(files.len(), 2);

        let a = files.iter().find(|f| f.path == "dir/a.txt").unwrap();
        assert_eq!(a.size, file_a.len() as u64);
        assert_eq!(a.mtime, Some(1700000000));
        assert_eq!(a.mode, Some(0o644));

        // Chunks should be retrievable and reconstruct the original data
        let chunks_a = catalog.get_file_chunks("local", "test", "dir/a.txt").unwrap();
        let mut reconstructed = Vec::new();
        for c in &chunks_a {
            reconstructed.extend_from_slice(&store.get(&c.hash).unwrap());
        }
        assert_eq!(reconstructed, file_a);
    }
}
