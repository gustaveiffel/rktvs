// SPDX-License-Identifier: AGPL-3.0-only

//! Core library for rk: content-addressed chunk store, FastCDC chunker,
//! SQLite catalog, manifest (zero-copy by-reference chunking), indexer,
//! and verifier.

pub mod catalog;
pub mod chunk_store;
pub mod chunker;
pub mod dict;
pub mod error;
pub mod indexer;
pub mod manifest;
pub mod resolver;
pub mod verifier;

pub use error::{Error, Result};
