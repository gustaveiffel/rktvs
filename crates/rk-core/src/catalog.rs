use std::path::Path;
use rusqlite::Connection;

use crate::Result;

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub library_id: String,
    pub tape: String,
    pub path: String,
    pub entry_type: i64,
    pub size: u64,
    pub mtime: Option<u64>,
    pub mode: Option<u32>,
    pub version: u64,
}

pub struct Catalog {
    conn: Connection,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS libraries (
    library_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    wg_pubkey BLOB NOT NULL,
    status TEXT DEFAULT 'offline',
    last_seen INTEGER,
    last_catalog_version INTEGER DEFAULT 0,
    trust_level TEXT DEFAULT 'full'
);

CREATE TABLE IF NOT EXISTS tapes (
    library_id TEXT NOT NULL,
    tape_name TEXT NOT NULL,
    description TEXT,
    owner TEXT,
    permission TEXT NOT NULL,
    catalog_version INTEGER DEFAULT 0,
    merkle_root BLOB,
    last_sync INTEGER,
    total_files INTEGER DEFAULT 0,
    total_size INTEGER DEFAULT 0,
    PRIMARY KEY (library_id, tape_name)
);

CREATE TABLE IF NOT EXISTS files (
    library_id TEXT NOT NULL,
    tape TEXT NOT NULL,
    path TEXT NOT NULL,
    entry_type INTEGER NOT NULL,
    size INTEGER,
    mtime INTEGER,
    mode INTEGER,
    merkle_root BLOB,
    version INTEGER NOT NULL,
    deleted_at INTEGER,
    PRIMARY KEY (library_id, tape, path)
);

CREATE TABLE IF NOT EXISTS file_chunks (
    library_id TEXT NOT NULL,
    tape TEXT NOT NULL,
    file_path TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_hash BLOB NOT NULL,
    offset INTEGER NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (library_id, tape, file_path, chunk_index)
);

CREATE TABLE IF NOT EXISTS chunks (
    hash BLOB PRIMARY KEY,
    size INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,
    ref_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS local_chunks (
    chunk_hash BLOB PRIMARY KEY,
    size INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL,
    fetched_at INTEGER NOT NULL,
    last_access INTEGER NOT NULL,
    source_library TEXT,
    ref_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    library_id TEXT NOT NULL,
    tape TEXT NOT NULL,
    job_type TEXT NOT NULL,
    grade INTEGER NOT NULL,
    file_path TEXT,
    total_chunks INTEGER,
    completed_chunks INTEGER DEFAULT 0,
    total_bytes INTEGER,
    status TEXT DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS tape_versions (
    library_id TEXT NOT NULL,
    tape TEXT NOT NULL,
    version INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    merkle_root BLOB NOT NULL,
    change_count INTEGER NOT NULL,
    PRIMARY KEY (library_id, tape, version)
);

CREATE TABLE IF NOT EXISTS tape_acl (
    tape TEXT NOT NULL,
    node_id TEXT NOT NULL,
    permission TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    granted_at INTEGER NOT NULL,
    expires_at INTEGER,
    PRIMARY KEY (tape, node_id)
);
"#;

impl Catalog {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let catalog = Self { conn };
        catalog.init_schema()?;
        Ok(catalog)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let catalog = Self { conn };
        catalog.init_schema()?;
        Ok(catalog)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(SCHEMA)?;
        Ok(())
    }

    /// Record a file and its chunk list in the catalog.
    pub fn record_file(
        &self,
        library_id: &str,
        tape: &str,
        path: &str,
        entry_type: i64,
        size: u64,
        mtime: Option<u64>,
        mode: Option<u32>,
        version: u64,
        chunks: &[crate::chunker::ChunkMeta],
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        tx.execute(
            "INSERT OR REPLACE INTO files (library_id, tape, path, entry_type, size, mtime, mode, version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                library_id,
                tape,
                path,
                entry_type,
                size as i64,
                mtime.map(|v| v as i64),
                mode.map(|v| v as i64),
                version as i64,
            ],
        )?;

        for (i, chunk) in chunks.iter().enumerate() {
            tx.execute(
                "INSERT OR REPLACE INTO file_chunks
                 (library_id, tape, file_path, chunk_index, chunk_hash, offset, size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    library_id,
                    tape,
                    path,
                    i as i64,
                    chunk.hash.as_bytes().as_slice(),
                    chunk.offset as i64,
                    chunk.size as i64,
                ],
            )?;

            tx.execute(
                "INSERT OR IGNORE INTO chunks (hash, size, compressed_size)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    chunk.hash.as_bytes().as_slice(),
                    chunk.size as i64,
                    chunk.compressed_size as i64,
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// Get the ordered list of chunks for a file.
    pub fn get_file_chunks(
        &self,
        library_id: &str,
        tape: &str,
        path: &str,
    ) -> Result<Vec<crate::chunker::ChunkMeta>> {
        let mut stmt = self.conn.prepare(
            "SELECT chunk_hash, offset, size FROM file_chunks
             WHERE library_id = ?1 AND tape = ?2 AND file_path = ?3
             ORDER BY chunk_index",
        )?;

        let chunks = stmt
            .query_map(rusqlite::params![library_id, tape, path], |row| {
                let hash_bytes: Vec<u8> = row.get(0)?;
                let offset: i64 = row.get(1)?;
                let size: i64 = row.get(2)?;
                Ok(crate::chunker::ChunkMeta {
                    hash: blake3::Hash::from_bytes(
                        hash_bytes.as_slice().try_into().expect("invalid hash length"),
                    ),
                    offset: offset as u64,
                    size: size as usize,
                    compressed_size: 0,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(chunks)
    }

    pub fn list_files(
        &self,
        library_id: &str,
        tape: &str,
        path_prefix: &str,
    ) -> Result<Vec<FileEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT library_id, tape, path, entry_type, size, mtime, mode, version
             FROM files
             WHERE library_id = ?1 AND tape = ?2 AND path LIKE ?3
               AND deleted_at IS NULL
             ORDER BY path",
        )?;

        let like_pattern = if path_prefix == "/" {
            "%".to_string()
        } else {
            format!("{}%", path_prefix)
        };

        let files = stmt
            .query_map(rusqlite::params![library_id, tape, like_pattern], |row| {
                let size: i64 = row.get(4)?;
                let mtime: Option<i64> = row.get(5)?;
                let mode: Option<i64> = row.get(6)?;
                let version: i64 = row.get(7)?;
                Ok(FileEntry {
                    library_id: row.get(0)?,
                    tape: row.get(1)?,
                    path: row.get(2)?,
                    entry_type: row.get(3)?,
                    size: size as u64,
                    mtime: mtime.map(|v| v as u64),
                    mode: mode.map(|v| v as u32),
                    version: version as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_schema() {
        let catalog = Catalog::open_in_memory().unwrap();

        // Verify key tables exist by querying them
        let tables: Vec<String> = catalog
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(tables.contains(&"chunks".to_string()));
        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"file_chunks".to_string()));
        assert!(tables.contains(&"local_chunks".to_string()));
        assert!(tables.contains(&"libraries".to_string()));
        assert!(tables.contains(&"tapes".to_string()));
        assert!(tables.contains(&"tape_versions".to_string()));
        assert!(tables.contains(&"tape_acl".to_string()));
        assert!(tables.contains(&"jobs".to_string()));
    }

    #[test]
    fn record_and_query_file_chunks() {
        let catalog = Catalog::open_in_memory().unwrap();

        let hash1 = blake3::hash(b"chunk1");
        let hash2 = blake3::hash(b"chunk2");

        let chunks = vec![
            crate::chunker::ChunkMeta {
                hash: hash1,
                offset: 0,
                size: 1000,
                compressed_size: 800,
            },
            crate::chunker::ChunkMeta {
                hash: hash2,
                offset: 1000,
                size: 500,
                compressed_size: 400,
            },
        ];

        catalog
            .record_file("local", "default", "/test/file.bin", 1, 1500, None, None, 2, &chunks)
            .unwrap();

        let retrieved = catalog.get_file_chunks("local", "default", "/test/file.bin").unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].hash, hash1);
        assert_eq!(retrieved[0].offset, 0);
        assert_eq!(retrieved[0].size, 1000);
        assert_eq!(retrieved[1].hash, hash2);
        assert_eq!(retrieved[1].offset, 1000);
        assert_eq!(retrieved[1].size, 500);
    }

    #[test]
    fn list_files_by_prefix() {
        let catalog = Catalog::open_in_memory().unwrap();

        catalog
            .record_file("local", "docs", "/readme.txt", 1, 100, Some(1700000000), Some(0o644), 1, &[])
            .unwrap();
        catalog
            .record_file("local", "docs", "/src/main.rs", 1, 200, Some(1700000000), Some(0o644), 1, &[])
            .unwrap();
        catalog
            .record_file("local", "docs", "/src/lib.rs", 1, 150, Some(1700000000), Some(0o644), 1, &[])
            .unwrap();
        catalog
            .record_file("local", "other", "/data.bin", 1, 500, None, None, 1, &[])
            .unwrap();

        let all = catalog.list_files("local", "docs", "/").unwrap();
        assert_eq!(all.len(), 3);

        let src = catalog.list_files("local", "docs", "/src/").unwrap();
        assert_eq!(src.len(), 2);

        let other = catalog.list_files("local", "other", "/").unwrap();
        assert_eq!(other.len(), 1);

        let readme = &all.iter().find(|f| f.path == "/readme.txt").unwrap();
        assert_eq!(readme.size, 100);
        assert_eq!(readme.mtime, Some(1700000000));
        assert_eq!(readme.mode, Some(0o644));
    }

    #[test]
    fn query_nonexistent_file_returns_empty() {
        let catalog = Catalog::open_in_memory().unwrap();
        let result = catalog
            .get_file_chunks("local", "default", "/no/such/file")
            .unwrap();
        assert!(result.is_empty());
    }
}
