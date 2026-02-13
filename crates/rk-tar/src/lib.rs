// SPDX-License-Identifier: AGPL-3.0-only

//! Tar archive ingest and export for rk.
//!
//! Provides streaming tar ingest (stdin to chunk store) and export
//! (chunk store to stdout tar stream) with path traversal protection.

pub mod export;
pub mod ingest;
