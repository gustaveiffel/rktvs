use std::io::Write;

use rk_core::catalog::Catalog;
use rk_core::chunk_store::ChunkStore;

/// Export files from the catalog and chunk store as a tar archive.
pub fn export_tar<W: Write>(
    writer: W,
    store: &ChunkStore,
    catalog: &Catalog,
    library_id: &str,
    tape: &str,
    path_prefix: &str,
) -> rk_core::Result<()> {
    let mut builder = tar::Builder::new(writer);
    let files = catalog.list_files(library_id, tape, path_prefix)?;

    for file in &files {
        let mut header = tar::Header::new_gnu();
        header
            .set_path(&file.path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        if file.entry_type == 2 {
            header.set_entry_type(tar::EntryType::Directory);
            header.set_size(0);
        } else {
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(file.size);
        }

        if let Some(mtime) = file.mtime {
            header.set_mtime(mtime);
        }
        if let Some(mode) = file.mode {
            header.set_mode(mode);
        }
        header.set_cksum();

        if file.entry_type == 1 && file.size > 0 {
            let chunks = catalog.get_file_chunks(&file.library_id, &file.tape, &file.path)?;
            let mut data = Vec::with_capacity(file.size as usize);
            for chunk_meta in &chunks {
                let chunk_data = store.get(&chunk_meta.hash)?;
                data.extend_from_slice(&chunk_data);
            }
            builder.append(&header, data.as_slice())?;
        } else {
            builder.append(&header, &[][..])?;
        }
    }

    builder.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::{self, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE};
    use std::io::Cursor;
    use std::io::Read;

    /// Helper: create a tar archive in memory.
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
    fn export_tar_reconstructs_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let store = ChunkStore::new(dir.path().to_path_buf());
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let file_a = b"Content of file A for export test";
        let file_b = b"Content of file B for export test";
        let tar_data = make_tar(&[
            ("alpha.txt", file_a, 0o644, 1700000000),
            ("beta.txt", file_b, 0o755, 1700000001),
        ]);

        // Ingest first
        ingest::ingest_tar(
            Cursor::new(&tar_data),
            &store,
            &catalog,
            "local",
            "mytest",
            MIN_CHUNK_SIZE,
            AVG_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
        )
        .unwrap();

        // Export
        let mut exported = Vec::new();
        export_tar(&mut exported, &store, &catalog, "local", "mytest", "/").unwrap();

        // Parse exported tar and verify contents
        let mut archive = tar::Archive::new(Cursor::new(&exported));
        let mut found = std::collections::HashMap::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            let mode = entry.header().mode().unwrap();
            let mtime = entry.header().mtime().unwrap();
            found.insert(path, (data, mode, mtime));
        }

        assert_eq!(found.len(), 2);
        assert_eq!(found["alpha.txt"].0, file_a);
        assert_eq!(found["alpha.txt"].1, 0o644);
        assert_eq!(found["alpha.txt"].2, 1700000000);
        assert_eq!(found["beta.txt"].0, file_b);
        assert_eq!(found["beta.txt"].1, 0o755);
        assert_eq!(found["beta.txt"].2, 1700000001);
    }
}
