// SPDX-License-Identifier: AGPL-3.0-only

use rusqlite::Connection;
use std::path::Path;

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

/// Library record from the libraries table.
#[derive(Debug, Clone)]
pub struct LibraryRecord {
    pub library_id: String,
    pub display_name: String,
    pub endpoint: String,
    pub status: String,
    pub last_seen: Option<i64>,
    pub cert_path: Option<String>,
}

/// Job record from the jobs table.
#[derive(Debug, Clone)]
pub struct JobRecord {
    pub job_id: String,
    pub library_id: String,
    pub tape: String,
    pub job_type: String,
    pub grade: i32,
    pub file_path: Option<String>,
    pub total_chunks: Option<i64>,
    pub completed_chunks: i64,
    pub total_bytes: Option<i64>,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub error: Option<String>,
}

/// Summary of a tape within a library.
#[derive(Debug, Clone)]
pub struct TapeSummary {
    pub tape_name: String,
    pub file_count: u64,
    pub total_size: u64,
}

/// A file entry with its associated chunk list.
#[derive(Debug, Clone)]
pub struct FileWithChunks {
    pub path: String,
    pub entry_type: i64,
    pub size: u64,
    pub mtime: Option<u64>,
    pub mode: Option<u32>,
    pub version: u64,
    pub chunks: Vec<crate::chunker::ChunkMeta>,
}

pub struct Catalog {
    conn: Connection,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS libraries (
    library_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    wg_pubkey BLOB,
    status TEXT DEFAULT 'offline',
    last_seen INTEGER,
    last_catalog_version INTEGER DEFAULT 0,
    trust_level TEXT DEFAULT 'full',
    cert_path TEXT
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
        self.conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;
        self.conn.execute_batch(SCHEMA)?;
        self.migrate()?;
        Ok(())
    }

    /// Run schema migrations for databases created by earlier versions.
    fn migrate(&self) -> Result<()> {
        // Migration 1: add cert_path column (v0.1 → v0.2).
        // Ignore error if column already exists (fresh databases).
        let _ = self
            .conn
            .execute_batch("ALTER TABLE libraries ADD COLUMN cert_path TEXT;");
        // Migrate data stashed in trust_level by the v0.1 MVP hack.
        self.conn.execute_batch(
            "UPDATE libraries SET cert_path = trust_level
             WHERE trust_level != 'full' AND cert_path IS NULL;",
        )?;

        // Migration 2: relax wg_pubkey NOT NULL → nullable.
        // Old schema had `wg_pubkey BLOB NOT NULL` but we don't use WireGuard
        // yet. CREATE TABLE IF NOT EXISTS won't update existing constraints,
        // so we must recreate the table.
        let has_notnull: bool = self
            .conn
            .query_row(
                "SELECT \"notnull\" FROM pragma_table_info('libraries') WHERE name = 'wg_pubkey'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if has_notnull {
            self.conn.execute_batch(
                "CREATE TABLE libraries_new (
                    library_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    wg_pubkey BLOB,
                    status TEXT DEFAULT 'offline',
                    last_seen INTEGER,
                    last_catalog_version INTEGER DEFAULT 0,
                    trust_level TEXT DEFAULT 'full',
                    cert_path TEXT
                );
                INSERT INTO libraries_new SELECT * FROM libraries;
                DROP TABLE libraries;
                ALTER TABLE libraries_new RENAME TO libraries;",
            )?;
        }

        Ok(())
    }

    /// Current Unix timestamp in seconds.
    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_secs() as i64
    }

    // ── Library CRUD ─────────────────────────────────────────

    /// Register a remote library.
    pub fn add_library(
        &self,
        library_id: &str,
        display_name: &str,
        endpoint: &str,
        cert_path: &str,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO libraries (library_id, display_name, endpoint, cert_path, status)
             VALUES (?1, ?2, ?3, ?4, 'offline')",
            rusqlite::params![library_id, display_name, endpoint, cert_path],
        )?;
        Ok(())
    }

    /// List all registered libraries.
    pub fn list_libraries(&self) -> Result<Vec<LibraryRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT library_id, display_name, endpoint, status, last_seen, cert_path
             FROM libraries ORDER BY library_id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(LibraryRecord {
                    library_id: row.get(0)?,
                    display_name: row.get(1)?,
                    endpoint: row.get(2)?,
                    status: row.get(3)?,
                    last_seen: row.get(4)?,
                    cert_path: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Get a single library by ID.
    pub fn get_library(&self, library_id: &str) -> Result<Option<LibraryRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT library_id, display_name, endpoint, status, last_seen, cert_path
             FROM libraries WHERE library_id = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![library_id], |row| {
            Ok(LibraryRecord {
                library_id: row.get(0)?,
                display_name: row.get(1)?,
                endpoint: row.get(2)?,
                status: row.get(3)?,
                last_seen: row.get(4)?,
                cert_path: row.get(5)?,
            })
        })?;
        match rows.next() {
            Some(Ok(rec)) => Ok(Some(rec)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Remove a library and all its associated data (cascade delete).
    pub fn remove_library(&self, library_id: &str) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM file_chunks WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM files WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM tape_versions WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM tapes WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM jobs WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.execute(
            "DELETE FROM libraries WHERE library_id = ?1",
            rusqlite::params![library_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Update a library's online status and last_seen timestamp.
    pub fn update_library_status(&self, library_id: &str, status: &str) -> Result<()> {
        let now = Self::unix_now();
        self.conn.execute(
            "UPDATE libraries SET status = ?1, last_seen = ?2 WHERE library_id = ?3",
            rusqlite::params![status, now, library_id],
        )?;
        Ok(())
    }

    // ── File + Chunk operations ─────────────────────────────

    /// Record a file and its chunk list in the catalog.
    #[allow(clippy::too_many_arguments)]
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
            "SELECT fc.chunk_hash, fc.offset, fc.size, COALESCE(c.compressed_size, 0)
             FROM file_chunks fc
             LEFT JOIN chunks c ON fc.chunk_hash = c.hash
             WHERE fc.library_id = ?1 AND fc.tape = ?2 AND fc.file_path = ?3
             ORDER BY fc.chunk_index",
        )?;

        let chunks = stmt
            .query_map(rusqlite::params![library_id, tape, path], |row| {
                let hash_bytes: Vec<u8> = row.get(0)?;
                let offset: i64 = row.get(1)?;
                let size: i64 = row.get(2)?;
                let compressed_size: i64 = row.get(3)?;
                let hash_array: [u8; 32] = hash_bytes.as_slice().try_into()
                    .map_err(|_| rusqlite::Error::InvalidColumnType(
                        0,
                        "chunk_hash".into(),
                        rusqlite::types::Type::Blob,
                    ))?;
                Ok(crate::chunker::ChunkMeta {
                    hash: blake3::Hash::from_bytes(hash_array),
                    offset: offset as u64,
                    size: size as usize,
                    compressed_size: compressed_size as usize,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(chunks)
    }

    /// Look up the decompressed size of a chunk from the chunks table.
    /// Returns None if the chunk hash is not in the catalog.
    pub fn get_chunk_decompressed_size(&self, hash: &blake3::Hash) -> Result<Option<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT size FROM chunks WHERE hash = ?1",
        )?;
        let result = stmt.query_row(
            rusqlite::params![hash.as_bytes().as_slice()],
            |row| {
                let size: i64 = row.get(0)?;
                Ok(size as u64)
            },
        );
        match result {
            Ok(size) => Ok(Some(size)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Sample up to `limit` chunk hashes from a tape (for dictionary training).
    pub fn sample_chunk_hashes(
        &self,
        library_id: &str,
        tape: &str,
        limit: usize,
    ) -> Result<Vec<blake3::Hash>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT chunk_hash FROM file_chunks
             WHERE library_id = ?1 AND tape = ?2
             ORDER BY RANDOM() LIMIT ?3",
        )?;
        let hashes = stmt
            .query_map(rusqlite::params![library_id, tape, limit as i64], |row| {
                let bytes: Vec<u8> = row.get(0)?;
                let arr: [u8; 32] = bytes.as_slice().try_into()
                    .map_err(|_| rusqlite::Error::InvalidColumnType(
                        0, "chunk_hash".into(), rusqlite::types::Type::Blob,
                    ))?;
                Ok(blake3::Hash::from_bytes(arr))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(hashes)
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

    // ── Tape + bulk queries ───────────────────────────────

    /// List distinct tapes for a library, with file count and total size.
    pub fn list_tapes(&self, library_id: &str) -> Result<Vec<TapeSummary>> {
        let mut stmt = self.conn.prepare(
            "SELECT tape, COUNT(*), COALESCE(SUM(size), 0)
             FROM files
             WHERE library_id = ?1 AND entry_type = 1 AND deleted_at IS NULL
             GROUP BY tape
             ORDER BY tape",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![library_id], |row| {
                let count: i64 = row.get(1)?;
                let size: i64 = row.get(2)?;
                Ok(TapeSummary {
                    tape_name: row.get(0)?,
                    file_count: count as u64,
                    total_size: size as u64,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Get all files in a tape with their chunk lists.
    pub fn get_all_files_with_chunks(
        &self,
        library_id: &str,
        tape: &str,
    ) -> Result<Vec<FileWithChunks>> {
        // First get all files
        let mut file_stmt = self.conn.prepare(
            "SELECT path, entry_type, size, mtime, mode, version
             FROM files
             WHERE library_id = ?1 AND tape = ?2 AND deleted_at IS NULL
             ORDER BY path",
        )?;
        let files: Vec<FileWithChunks> = file_stmt
            .query_map(rusqlite::params![library_id, tape], |row| {
                let size: i64 = row.get(2)?;
                let mtime: Option<i64> = row.get(3)?;
                let mode: Option<i64> = row.get(4)?;
                let version: i64 = row.get(5)?;
                Ok(FileWithChunks {
                    path: row.get(0)?,
                    entry_type: row.get(1)?,
                    size: size as u64,
                    mtime: mtime.map(|v| v as u64),
                    mode: mode.map(|v| v as u32),
                    version: version as u64,
                    chunks: Vec::new(),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Then get chunks for each file
        let mut chunk_stmt = self.conn.prepare(
            "SELECT chunk_hash, offset, size FROM file_chunks
             WHERE library_id = ?1 AND tape = ?2 AND file_path = ?3
             ORDER BY chunk_index",
        )?;

        let mut result = Vec::with_capacity(files.len());
        for mut file in files {
            let chunks = chunk_stmt
                .query_map(rusqlite::params![library_id, tape, file.path], |row| {
                    let hash_bytes: Vec<u8> = row.get(0)?;
                    let offset: i64 = row.get(1)?;
                    let size: i64 = row.get(2)?;
                    let hash_array: [u8; 32] = hash_bytes.as_slice().try_into().map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            0,
                            "chunk_hash".into(),
                            rusqlite::types::Type::Blob,
                        )
                    })?;
                    Ok(crate::chunker::ChunkMeta {
                        hash: blake3::Hash::from_bytes(hash_array),
                        offset: offset as u64,
                        size: size as usize,
                        compressed_size: 0,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            file.chunks = chunks;
            result.push(file);
        }

        Ok(result)
    }

    /// Create a new job.
    #[allow(clippy::too_many_arguments)]
    pub fn create_job(
        &self,
        job_id: &str,
        library_id: &str,
        tape: &str,
        job_type: &str,
        grade: i32,
        file_path: Option<&str>,
        total_chunks: Option<i64>,
        total_bytes: Option<i64>,
    ) -> Result<()> {
        let now = Self::unix_now();
        self.conn.execute(
            "INSERT INTO jobs (job_id, library_id, tape, job_type, grade, file_path, total_chunks, total_bytes, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', ?9, ?9)",
            rusqlite::params![job_id, library_id, tape, job_type, grade, file_path, total_chunks, total_bytes, now],
        )?;
        Ok(())
    }

    /// Get a job by ID.
    pub fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT job_id, library_id, tape, job_type, grade, file_path, total_chunks,
                    completed_chunks, total_bytes, status, created_at, updated_at, error
             FROM jobs WHERE job_id = ?1",
        )?;
        let mut rows = stmt.query_map(rusqlite::params![job_id], |row| {
            Ok(JobRecord {
                job_id: row.get(0)?,
                library_id: row.get(1)?,
                tape: row.get(2)?,
                job_type: row.get(3)?,
                grade: row.get(4)?,
                file_path: row.get(5)?,
                total_chunks: row.get(6)?,
                completed_chunks: row.get(7)?,
                total_bytes: row.get(8)?,
                status: row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
                error: row.get(12)?,
            })
        })?;
        match rows.next() {
            Some(Ok(job)) => Ok(Some(job)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// List jobs, optionally filtered by status.
    pub fn list_jobs(&self, status_filter: Option<&str>) -> Result<Vec<JobRecord>> {
        fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<JobRecord> {
            Ok(JobRecord {
                job_id: row.get(0)?,
                library_id: row.get(1)?,
                tape: row.get(2)?,
                job_type: row.get(3)?,
                grade: row.get(4)?,
                file_path: row.get(5)?,
                total_chunks: row.get(6)?,
                completed_chunks: row.get(7)?,
                total_bytes: row.get(8)?,
                status: row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
                error: row.get(12)?,
            })
        }

        if let Some(status) = status_filter {
            let mut stmt = self.conn.prepare(
                "SELECT job_id, library_id, tape, job_type, grade, file_path, total_chunks,
                        completed_chunks, total_bytes, status, created_at, updated_at, error
                 FROM jobs WHERE status = ?1 ORDER BY grade, created_at",
            )?;
            let jobs = stmt
                .query_map(rusqlite::params![status], row_to_job)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(jobs)
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT job_id, library_id, tape, job_type, grade, file_path, total_chunks,
                        completed_chunks, total_bytes, status, created_at, updated_at, error
                 FROM jobs ORDER BY grade, created_at",
            )?;
            let jobs = stmt
                .query_map([], row_to_job)?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(jobs)
        }
    }

    /// Update a job's status and completed_chunks count.
    pub fn update_job_progress(
        &self,
        job_id: &str,
        completed_chunks: i64,
        status: &str,
    ) -> Result<()> {
        let now = Self::unix_now();
        self.conn.execute(
            "UPDATE jobs SET completed_chunks = ?1, status = ?2, updated_at = ?3 WHERE job_id = ?4",
            rusqlite::params![completed_chunks, status, now, job_id],
        )?;
        Ok(())
    }

    /// Cancel a job (set status to 'cancelled').
    pub fn cancel_job(&self, job_id: &str) -> Result<()> {
        let now = Self::unix_now();
        self.conn.execute(
            "UPDATE jobs SET status = 'cancelled', updated_at = ?1 WHERE job_id = ?2",
            rusqlite::params![now, job_id],
        )?;
        Ok(())
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
            .record_file(
                "local",
                "default",
                "/test/file.bin",
                1,
                1500,
                None,
                None,
                2,
                &chunks,
            )
            .unwrap();

        let retrieved = catalog
            .get_file_chunks("local", "default", "/test/file.bin")
            .unwrap();
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
            .record_file(
                "local",
                "docs",
                "/readme.txt",
                1,
                100,
                Some(1700000000),
                Some(0o644),
                1,
                &[],
            )
            .unwrap();
        catalog
            .record_file(
                "local",
                "docs",
                "/src/main.rs",
                1,
                200,
                Some(1700000000),
                Some(0o644),
                1,
                &[],
            )
            .unwrap();
        catalog
            .record_file(
                "local",
                "docs",
                "/src/lib.rs",
                1,
                150,
                Some(1700000000),
                Some(0o644),
                1,
                &[],
            )
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

    #[test]
    fn job_crud() {
        let catalog = Catalog::open_in_memory().unwrap();

        catalog
            .create_job(
                "job-1",
                "lib-a",
                "tape-1",
                "fetch",
                1,
                Some("/file.bin"),
                Some(10),
                Some(1000),
            )
            .unwrap();
        catalog
            .create_job(
                "job-2",
                "lib-a",
                "tape-1",
                "fetch",
                0,
                Some("/urgent.bin"),
                Some(5),
                Some(500),
            )
            .unwrap();

        let job = catalog.get_job("job-1").unwrap().unwrap();
        assert_eq!(job.job_id, "job-1");
        assert_eq!(job.grade, 1);
        assert_eq!(job.status, "pending");
        assert_eq!(job.completed_chunks, 0);

        // Update progress
        catalog.update_job_progress("job-1", 5, "running").unwrap();
        let job = catalog.get_job("job-1").unwrap().unwrap();
        assert_eq!(job.completed_chunks, 5);
        assert_eq!(job.status, "running");

        // List by status
        let pending = catalog.list_jobs(Some("pending")).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].job_id, "job-2");

        // List all — sorted by grade then created_at
        let all = catalog.list_jobs(None).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].job_id, "job-2"); // grade 0 first

        // Cancel
        catalog.cancel_job("job-2").unwrap();
        let job = catalog.get_job("job-2").unwrap().unwrap();
        assert_eq!(job.status, "cancelled");

        // Nonexistent
        assert!(catalog.get_job("nope").unwrap().is_none());
    }

    #[test]
    fn library_crud() {
        let catalog = Catalog::open_in_memory().unwrap();

        // Add
        catalog
            .add_library("hub1", "Hub One", "192.168.1.1:4443", "certs/hub1.cert.der")
            .unwrap();
        catalog
            .add_library("hub2", "Hub Two", "10.0.0.1:4443", "certs/hub2.cert.der")
            .unwrap();

        // List
        let libs = catalog.list_libraries().unwrap();
        assert_eq!(libs.len(), 2);
        assert_eq!(libs[0].library_id, "hub1");
        assert_eq!(libs[1].library_id, "hub2");

        // Get — includes cert_path
        let lib = catalog.get_library("hub1").unwrap().unwrap();
        assert_eq!(lib.display_name, "Hub One");
        assert_eq!(lib.endpoint, "192.168.1.1:4443");
        assert_eq!(lib.status, "offline");
        assert!(lib.last_seen.is_none());
        assert_eq!(lib.cert_path.as_deref(), Some("certs/hub1.cert.der"));

        // Nonexistent
        assert!(catalog.get_library("nope").unwrap().is_none());

        // Update status
        catalog.update_library_status("hub1", "online").unwrap();
        let lib = catalog.get_library("hub1").unwrap().unwrap();
        assert_eq!(lib.status, "online");
        assert!(lib.last_seen.is_some());

        // Remove
        catalog.remove_library("hub1").unwrap();
        assert!(catalog.get_library("hub1").unwrap().is_none());
        assert_eq!(catalog.list_libraries().unwrap().len(), 1);

        // Remove nonexistent — no error
        catalog.remove_library("hub1").unwrap();
    }

    #[test]
    fn remove_library_cascade_deletes_child_data() {
        let catalog = Catalog::open_in_memory().unwrap();

        // Set up a library with files, chunks, tapes, jobs
        catalog
            .add_library("hub-x", "Hub X", "10.0.0.1:4443", "certs/hubx.cert.der")
            .unwrap();

        let hash = blake3::hash(b"data");
        let chunks = vec![crate::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: 100,
            compressed_size: 80,
        }];
        catalog
            .record_file(
                "hub-x",
                "tape1",
                "/file.bin",
                1,
                100,
                None,
                None,
                1,
                &chunks,
            )
            .unwrap();
        catalog
            .create_job(
                "j1",
                "hub-x",
                "tape1",
                "fetch",
                1,
                Some("/file.bin"),
                Some(1),
                Some(100),
            )
            .unwrap();

        // Verify data exists
        assert!(
            !catalog
                .get_file_chunks("hub-x", "tape1", "/file.bin")
                .unwrap()
                .is_empty()
        );
        assert!(catalog.get_job("j1").unwrap().is_some());

        // Remove with cascade
        catalog.remove_library("hub-x").unwrap();

        // All child data should be gone
        assert!(catalog.get_library("hub-x").unwrap().is_none());
        assert!(
            catalog
                .get_file_chunks("hub-x", "tape1", "/file.bin")
                .unwrap()
                .is_empty()
        );
        assert!(
            catalog
                .list_files("hub-x", "tape1", "/")
                .unwrap()
                .is_empty()
        );
        assert!(catalog.get_job("j1").unwrap().is_none());
    }

    #[test]
    fn library_duplicate_add_errors() {
        let catalog = Catalog::open_in_memory().unwrap();
        catalog
            .add_library("hub1", "Hub One", "1.2.3.4:4443", "certs/hub1.cert.der")
            .unwrap();
        let result = catalog.add_library(
            "hub1",
            "Hub One Again",
            "5.6.7.8:4443",
            "certs/hub1b.cert.der",
        );
        assert!(result.is_err());
    }

    #[test]
    fn catalog_uses_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(dir.path().join("test.db").as_path()).unwrap();
        let mode: String = catalog
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn list_tapes_returns_distinct_tapes() {
        let catalog = Catalog::open_in_memory().unwrap();

        let hash = blake3::hash(b"data");
        let chunks = vec![crate::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: 100,
            compressed_size: 80,
        }];

        // Files in two different tapes
        catalog
            .record_file(
                "local",
                "docs",
                "/readme.txt",
                1,
                100,
                None,
                None,
                1,
                &chunks,
            )
            .unwrap();
        catalog
            .record_file("local", "docs", "/guide.txt", 1, 200, None, None, 1, &[])
            .unwrap();
        catalog
            .record_file("local", "code", "/main.rs", 1, 300, None, None, 1, &chunks)
            .unwrap();
        // Directory entry (entry_type=2) should NOT count
        catalog
            .record_file("local", "docs", "/subdir", 2, 0, None, None, 1, &[])
            .unwrap();

        let tapes = catalog.list_tapes("local").unwrap();
        assert_eq!(tapes.len(), 2);

        let docs = tapes.iter().find(|t| t.tape_name == "docs").unwrap();
        assert_eq!(docs.file_count, 2); // only regular files
        assert_eq!(docs.total_size, 300); // 100 + 200

        let code = tapes.iter().find(|t| t.tape_name == "code").unwrap();
        assert_eq!(code.file_count, 1);
        assert_eq!(code.total_size, 300);
    }

    #[test]
    fn list_tapes_empty_library() {
        let catalog = Catalog::open_in_memory().unwrap();
        let tapes = catalog.list_tapes("nonexistent").unwrap();
        assert!(tapes.is_empty());
    }

    #[test]
    fn get_all_files_with_chunks_roundtrip() {
        let catalog = Catalog::open_in_memory().unwrap();

        let hash1 = blake3::hash(b"chunk-a");
        let hash2 = blake3::hash(b"chunk-b");

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
            .record_file(
                "local",
                "tape1",
                "/file.bin",
                1,
                1500,
                Some(1700000000),
                Some(0o644),
                2,
                &chunks,
            )
            .unwrap();
        catalog
            .record_file("local", "tape1", "/empty.txt", 1, 0, None, None, 1, &[])
            .unwrap();

        let files = catalog.get_all_files_with_chunks("local", "tape1").unwrap();
        assert_eq!(files.len(), 2);

        let empty = files.iter().find(|f| f.path == "/empty.txt").unwrap();
        assert!(empty.chunks.is_empty());
        assert_eq!(empty.version, 1);

        let file = files.iter().find(|f| f.path == "/file.bin").unwrap();
        assert_eq!(file.entry_type, 1);
        assert_eq!(file.size, 1500);
        assert_eq!(file.mtime, Some(1700000000));
        assert_eq!(file.mode, Some(0o644));
        assert_eq!(file.version, 2);
        assert_eq!(file.chunks.len(), 2);
        assert_eq!(file.chunks[0].hash, hash1);
        assert_eq!(file.chunks[0].offset, 0);
        assert_eq!(file.chunks[0].size, 1000);
        assert_eq!(file.chunks[1].hash, hash2);
        assert_eq!(file.chunks[1].offset, 1000);
        assert_eq!(file.chunks[1].size, 500);
    }

    #[test]
    fn migrate_relaxes_wg_pubkey_not_null() {
        // Simulate a v0.1 database with wg_pubkey NOT NULL.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("old.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE libraries (
                    library_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    wg_pubkey BLOB NOT NULL,
                    status TEXT DEFAULT 'offline',
                    last_seen INTEGER,
                    last_catalog_version INTEGER DEFAULT 0,
                    trust_level TEXT DEFAULT 'full'
                );",
            )
            .unwrap();
        }
        // Opening with Catalog should migrate and allow NULL wg_pubkey.
        let catalog = Catalog::open(&db_path).unwrap();
        catalog
            .add_library("hub1", "Hub", "1.2.3.4:4443", "certs/hub1.cert.der")
            .unwrap();
        let lib = catalog.get_library("hub1").unwrap().unwrap();
        assert_eq!(lib.endpoint, "1.2.3.4:4443");
    }

    #[test]
    fn get_file_chunks_includes_compressed_size() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let hash = blake3::hash(b"compressed-test");
        let chunks = vec![crate::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: 10000,
            compressed_size: 7000,
        }];
        catalog.record_file("lib-a", "tape-1", "/data.bin", 1, 10000, None, None, 1, &chunks).unwrap();

        let result = catalog.get_file_chunks("lib-a", "tape-1", "/data.bin").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].size, 10000);
        assert_eq!(result[0].compressed_size, 7000);
    }

    #[test]
    fn get_chunk_decompressed_size() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(dir.path().join("catalog.db").as_path()).unwrap();

        let hash = blake3::hash(b"test-chunk");
        let chunks = vec![crate::chunker::ChunkMeta {
            hash,
            offset: 0,
            size: 5000,
            compressed_size: 3500,
        }];
        catalog.record_file("local", "docs", "/file.txt", 1, 5000, None, None, 1, &chunks).unwrap();

        // Should find the decompressed size
        assert_eq!(catalog.get_chunk_decompressed_size(&hash).unwrap(), Some(5000));

        // Unknown hash returns None
        let unknown = blake3::hash(b"unknown");
        assert_eq!(catalog.get_chunk_decompressed_size(&unknown).unwrap(), None);
    }
}
