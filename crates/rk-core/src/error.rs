// SPDX-License-Identifier: AGPL-3.0-only

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Db(#[from] rusqlite::Error),

    #[error("chunk not found: {0}")]
    ChunkNotFound(blake3::Hash),

    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: blake3::Hash,
        actual: blake3::Hash,
    },

    #[error("manifest format error: {0}")]
    ManifestFormat(String),

    #[error(
        "stale source file: {path} (expected mtime={expected_mtime} size={expected_size}, got mtime={actual_mtime} size={actual_size})"
    )]
    StaleSourceFile {
        path: String,
        expected_mtime: u64,
        expected_size: u64,
        actual_mtime: u64,
        actual_size: u64,
    },

    #[error("source file not found: {0}")]
    SourceFileNotFound(String),

    #[error("chunk not available: {0}")]
    ChunkNotAvailable(blake3::Hash),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
