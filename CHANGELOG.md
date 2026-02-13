# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2026-02-12

### Bug Fixes

- quality hardening — atomic writes, path traversal, panics, PRAGMAs, clippy, SPDX
- security hardening — chunk hash verification, atomic key perms, ID sanitization
- migrate wg_pubkey NOT NULL → nullable for old databases
- add --san flag to hub init, show full error chain on ping failure
- graceful error handling and version display

### Documentation

- add README, ARCHITECTURE, PROTOCOL, TAPES, and COMPARED
- update all documentation for current state + add user guide
- update all docs for hardening + add developer API guide
- clean up TAPES.md — remove duplicated index/verify section, fix table alignment
- document --san flag, protocol version negotiation, TLS SAN troubleshooting
- document catalog sync protocol and CLI

### Features

- scaffold workspace and rk-core crate
- add ChunkStore, Chunker, and Catalog modules
- add ChunkStore get/compressed_size and Catalog record/query
- add ingest function combining chunker and store
- extend Catalog + scaffold rk-tar crate
- add ingest_tar and export_tar functions
- add tar round-trip tests and CLI binary
- add QUIC transport with hub server and satellite client
- add scheduling layer with grades, jobs, estimation, and resumable fetch
- zero-copy by-reference chunking — Manifest, ChunkResolver, Indexer, Verifier
- wire transport layer to CLI — hub serve, library management, fetch
- transport hardening — compressed transfer, timeouts, connection limits, schema fix
- protocol version negotiation at handshake
- library add --force to replace existing entry (cert rotation)
- semantic versioning with cargo-release + git-cliff
- add list_tapes and get_all_files_with_chunks to Catalog
- add CatalogSync proto messages and stream tag
- hub serves catalog metadata over CatalogSync stream
- satellite sync_catalog method for catalog pull
- rk library sync command for catalog pull

### Testing

- add round-trip integration tests
- catalog sync integration tests


