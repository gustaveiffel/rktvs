# Architecture

## Overview

rk is a content-addressed file delivery system for hostile networks. Files are
split into variable-size chunks (FastCDC), hashed (BLAKE3), compressed (zstd),
and stored locally. Metadata lives in an embedded SQLite catalog. Remote
transfer happens over QUIC with a protobuf wire protocol.

Four crates form the core:

```
rk-core       -- catalog (SQLite), chunk store (BLAKE3+zstd), chunker (FastCDC),
                 manifest (zero-copy), indexer, verifier, chunk resolver
rk-tar        -- streaming tar ingest/export
rk-transport  -- QUIC hub server + satellite client (quinn), TLS cert management,
                 protobuf wire protocol
rk-scheduler  -- grades, cost tiers, job queue, estimation, resumable fetcher
```

The `rk` CLI binary lives at the workspace root (`src/main.rs`) and depends on
all four crates.

## Data flow

### Ingest (tar)

```
tar cf - /data | rk ingest --tape backups
```

1. `rk-tar` reads tar entries from stdin via the `tar` crate.
2. Regular files are read into memory. Directories are recorded with
   `entry_type = 2` and no chunks.
3. Each file's bytes are split with FastCDC (1 MB min, 4 MB avg, 16 MB max).
4. Each chunk is hashed with BLAKE3, compressed with zstd (level 3), and
   written to `chunks/<prefix>/<hash>.zst`. Writes are idempotent -- if the
   file already exists, it is skipped.
5. The catalog records a row in `files` and one row per chunk in
   `file_chunks` (ordered by `chunk_index`). A global `chunks` row tracks
   size and compressed size with a reference count.

### Index (zero-copy)

```
rk index --tape backups /data
```

1. `rk-core::indexer` walks the directory tree with `walkdir`.
2. Each regular file is split with FastCDC. Chunks are **not** copied --
   instead, a `Manifest` (`.rkm` file) records each chunk's source file path,
   byte offset, and length alongside its BLAKE3 hash.
3. The catalog is populated identically to tar ingest.
4. At read time, `ChunkResolver` reads chunks directly from the original files
   using the manifest, falling back to the chunk store if the file has changed.

### Export

```
rk tar backups/src | tar xf -
```

1. The catalog is queried for all files matching the tape and path prefix.
2. For each file, `ChunkResolver` resolves chunks -- from the manifest (zero-copy)
   or the chunk store (compressed).
3. A tar entry is written to stdout with the original path, mode, and mtime.

### Verify

```
rk verify --tape backups --blake3
```

1. The verifier iterates over all chunks in the manifest.
2. **Fast mode** (default): checks that source files exist and have not changed
   size or mtime since indexing.
3. **BLAKE3 mode** (`--blake3`): re-reads each chunk from the source file and
   verifies the BLAKE3 hash matches.
4. Reports: chunks checked, OK, stale (file changed), missing, corrupted.

### Remote fetch

```
satellite --> QUIC --> hub
```

1. The satellite opens a QUIC connection to the hub (via `quinn`) using a
   pinned self-signed certificate.
2. A control stream (`0x00` tag) carries a protobuf `Handshake` /
   `HandshakeAck` exchange.
3. For each missing chunk the satellite opens a new bidi stream (`0x01` tag),
   sends a `ChunkRequest` (32-byte BLAKE3 hash), and receives a
   `ChunkResponse` with the raw (decompressed) data.
4. The satellite verifies the BLAKE3 hash before storing locally.
5. Chunks already present in the local store are skipped, providing resume
   support after interrupted transfers.

### Hub-satellite TLS

```
rk hub init          # generates self-signed cert + key (DER format)
rk hub serve         # starts QUIC server with that cert

rk library add       # satellite copies hub's cert, pins it
rk library ping      # verifies QUIC handshake with pinned cert
rk fetch             # fetches chunks over the pinned connection
```

Trust model: Trust On First Use (TOFU). The hub generates a self-signed
certificate at `hub init`. The satellite receives the cert out-of-band and
pins it with `library add --cert`. All subsequent connections verify against
the pinned certificate.

## Chunk store layout

```
<data-dir>/
  catalog.db              <-- SQLite (WAL mode, 9 tables)
  manifest.rkm            <-- zero-copy chunk manifest (if index was used)
  hub.cert.der            <-- hub TLS certificate (if hub init was run)
  hub.key.der             <-- hub TLS private key (mode 0o600)
  chunks/
    ab/
      ab3f...c7.zst       <-- zstd-compressed chunk
    cd/
      cd91...e2.zst
    ...
  certs/
    <library-id>.cert.der <-- pinned certs for known libraries
```

- Chunks are keyed by the first 2 hex characters of the BLAKE3 hash
  (`chunk_path` = `<base>/chunks/<hex[..2]>/<hex>.zst`).
- Writes are idempotent: `ChunkStore::put` returns early if the path exists.
- Reads verify integrity: `ChunkStore::get` decompresses, re-hashes, and
  returns `Error::HashMismatch` on corruption.

## SQLite catalog

Nine tables, all created on first open:

| Table | Purpose |
|---|---|
| `libraries` | Known peers (endpoint, cert path, trust level) |
| `tapes` | Named collections of files within a library |
| `files` | File metadata (path, type, size, mtime, mode, version); soft-deleted via `deleted_at` |
| `file_chunks` | Ordered mapping of file to chunk hashes (PK: library, tape, path, index) |
| `chunks` | Global chunk metadata (hash, size, compressed size, ref count) |
| `local_chunks` | Locally-present chunks with fetch timestamp and access tracking |
| `jobs` | Transfer jobs with grade, progress, status, error |
| `tape_versions` | Version history per tape with Merkle roots and change counts |
| `tape_acl` | Per-tape access control (node, permission, expiry) |

The catalog is the source of truth for metadata. Chunk data is lazy-fetched --
the catalog may reference chunks not yet present in the local store.

PRAGMAs: `journal_mode=WAL`, `foreign_keys=ON`, `busy_timeout=5000`.

## Wire protocol

QUIC (via `quinn`). Each bidirectional stream begins with a single-byte tag:

| Tag | Name | Request | Response |
|---|---|---|---|
| `0x00` | CONTROL | `Handshake` (protobuf, length-delimited) | `HandshakeAck` |
| `0x01` | CHUNK_REQUEST | `ChunkRequest` (32-byte hash) | `ChunkResponse` (found flag + data) |

Protobuf definitions are in `proto/rk.proto`, compiled at build time by
`prost-build` (see `crates/rk-transport/build.rs`).

Messages are framed with protobuf length-delimited encoding
(`encode_length_delimited` / `decode_length_delimited`).

Future: a stream type for catalog sync using Merkle tree deltas (the
`merkle_root` fields in `tapes` and `tape_versions` are in place).

## Scheduling

### Grade x CostTier matrix

Jobs are classified by urgency (`Grade`) and the current link cost
(`CostTier`). The `decide()` function maps the pair to `Run` or `Queue`:

|  | Free | Cheap | Metered | Expensive |
|---|---|---|---|---|
| **Urgent** | Run | Run | Run | Run |
| **Normal** | Run | Run | Run | Queue |
| **Batch** | Run | Run | Queue | Queue |
| **Background** | Run | Queue | Queue | Queue |

### Job tracking

Jobs are stored in the `jobs` table with status lifecycle:
`pending` -> `running` -> `completed` (or `cancelled` / `error`).

Progress is tracked per-chunk (`completed_chunks` / `total_chunks`). Jobs are
ordered by grade (lower = higher priority), then creation time.

### Resumable fetcher

`fetcher::fetch_file` iterates over a file's chunk list and calls
`ChunkStore::has` before each fetch. Already-present chunks are skipped.
After each chunk, job progress is updated in the catalog. On completion the
job status is set to `completed`.

The CLI `rk fetch` command performs the same logic: connect to hub, iterate
chunks, skip local ones, fetch and verify missing ones, update job progress.

## Design decisions

| Choice | Over | Why |
|---|---|---|
| QUIC | TCP | Connection migration, 0-RTT, stream multiplexing, BBR congestion control |
| FastCDC | Fixed-size chunks | Shift-resistant deduplication across file versions |
| BLAKE3 | SHA-256 | Faster on modern hardware, tree-hashable |
| SQLite | Postgres | Embedded, zero-config, works offline |
| zstd | gzip | Better compression ratio at higher speed |
| Protobuf | Text protocols | Typed fields, compact encoding, schema versioning |
| Self-signed + pinning | CA-signed certs | No CA infrastructure needed for field deployments |

## Crate dependency graph

```
rk (CLI binary)
  ├── rk-core
  ├── rk-tar        --> rk-core
  ├── rk-transport  --> rk-core
  └── rk-scheduler  --> rk-core, rk-transport
```
