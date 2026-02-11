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
}

pub type Result<T> = std::result::Result<T, Error>;
