# rk (rktvs)

**Radio Kills The Video Star** -- Content-addressed file delivery for hostile networks.

## The Problem

Satellite links, maritime VSAT, hotel WiFi, 3G roaming, field radios.
These networks are expensive, slow, intermittent, and sometimes half-duplex.
Existing tools (rsync, rclone, scp) assume a stable connection and fail hard when the link drops at byte 490,000,000 of a 500 MB transfer.

## How rk Solves It

- **Content-addressed storage.** Files are split into variable-size chunks with FastCDC, hashed with BLAKE3, compressed with zstd. Identical content is stored once regardless of filename or location.
- **Catalog-first.** Metadata syncs first into a local SQLite catalog. Browse remote file trees offline. Estimate transfer cost before a single byte of payload moves.
- **Chunk-level resume.** No file-level resume. If a 500 MB transfer (125 chunks) drops at chunk 120, resume at 121.
- **Cost-aware scheduling.** A Grade x CostTier decision matrix controls when jobs run. URGENT always transfers. BACKGROUND waits for a free link. Estimate first, fetch when the price is right.
- **Tar interface.** `tar cf - /local/ | rk ingest` and `rk tar <tape>/<path> | tar xf -`. Unix composability -- no proprietary archive formats.
- **QUIC transport.** Connection migration (roaming WiFi to 4G), 0-RTT reconnect, multiplexed streams, BBR congestion control built for lossy links.

Inspired by UUCP (1978): store-and-forward semantics, priority grades, cost-aware scheduling. Modern guts.

## Quick Start

### Build from source

Requires Rust 1.85+ (edition 2024) and a C compiler (for SQLite).

```bash
git clone https://github.com/rktvs/rk.git
cd rk
cargo build --release
```

The binary is at `target/release/rk`. Data is stored in `~/.rk` by default. Override with `--data-dir` or the `RK_DATA_DIR` environment variable.

### Store files locally

Ingest files from a tar stream:

```bash
tar cf - /path/to/files/ | rk ingest --tape myproject
```

Or index files in-place (zero-copy, no data copying):

```bash
rk index --tape myproject /path/to/files/
```

List what was stored:

```bash
rk ls myproject/
```

Export back to tar:

```bash
rk tar myproject/some/subdir | tar xf - -C /tmp/restore/
```

Verify integrity of indexed files:

```bash
rk verify --tape myproject
rk verify --tape myproject --blake3   # full hash verification
```

### Run a hub server

Initialize a hub (generates TLS certificate):

```bash
rk --data-dir /tmp/rk-hub hub init
```

Ingest some content on the hub, then start serving:

```bash
tar cf - /data/ | rk --data-dir /tmp/rk-hub ingest --tape demo
rk --data-dir /tmp/rk-hub hub serve
```

### Connect a satellite

On another machine (or another terminal):

```bash
rk library add myhub 127.0.0.1:4443 --cert /tmp/rk-hub/hub.cert.der
rk library ping myhub
rk library list
```

Fetch files (requires catalog metadata on the satellite side -- full catalog sync is not yet implemented):

```bash
rk fetch myhub:demo/path/to/file --grade normal
```

### Estimate transfer cost

```bash
rk estimate myproject/large-dataset.bin
```

### Manage jobs

```bash
rk jobs
rk jobs --status pending
```

## CLI Reference

| Command | Description |
|---|---|
| `rk ingest --tape <name>` | Read a tar archive from stdin, chunk and store it |
| `rk tar <tape>/<path>` | Export files as a tar archive to stdout |
| `rk ls <tape>/<path>` | List files in the catalog |
| `rk index --tape <name> <dir>` | Index files in-place (zero-copy by-reference chunking) |
| `rk verify [--tape <name>] [--blake3]` | Verify integrity of indexed files |
| `rk estimate <tape>/<path>` | Estimate transfer cost (chunks needed, bytes to fetch) |
| `rk jobs [--status <s>]` | List transfer jobs, optionally filtered by status |
| `rk hub init [--listen <addr>]` | Initialize a hub (generates TLS certificate) |
| `rk hub serve [--listen <addr>]` | Start the hub QUIC server |
| `rk library add <id> <endpoint> --cert <path>` | Register a remote library with cert pinning |
| `rk library list` | List known libraries |
| `rk library remove <id>` | Remove a library |
| `rk library ping <id>` | Ping a library to check connectivity |
| `rk fetch <library>:<tape>/<path> [--grade <g>]` | Fetch a file from a remote library |

Grades: `urgent` (P0), `normal` (P1), `batch` (P2), `background` (P3).

## Architecture

The project is a Cargo workspace with four crates:

| Crate | Purpose |
|---|---|
| `rk-core` | ChunkStore (BLAKE3 + zstd), FastCDC chunker, SQLite catalog, Manifest (zero-copy), Indexer, Verifier, ChunkResolver |
| `rk-tar` | Tar archive ingest and export via streaming |
| `rk-transport` | QUIC hub server and satellite client (quinn), TLS cert management, protobuf wire protocol |
| `rk-scheduler` | Grade/CostTier decision matrix, job queue, transfer estimation, resumable chunk fetcher |

The top-level `src/main.rs` provides the `rk` CLI built with clap.

Key concepts:

- **Library** -- an rk server (the hub). Satellites connect to libraries to sync catalogs and fetch chunks. Trust is established via certificate pinning.
- **Tape** -- a named logical volume grouping files with ACLs, sync policy, and versioning. Chunks are shared across tapes.
- **Chunk** -- a variable-size data block (~4 MB avg) identified by its BLAKE3 hash, compressed with zstd.
- **Manifest** -- a `.rkm` file mapping chunk hashes to file offsets for zero-copy by-reference chunking.
- **Grade** -- transfer priority: Urgent, Normal, Batch, Background.
- **CostTier** -- link classification: Free, Cheap, Metered, Expensive.

See [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md), [doc/PROTOCOL.md](doc/PROTOCOL.md), [doc/TAPES.md](doc/TAPES.md), [doc/COMPARED.md](doc/COMPARED.md), [doc/GUIDE.md](doc/GUIDE.md), and [doc/API.md](doc/API.md) for details.

## Status

**Alpha -- active development.**

What works today:

- Content-addressed chunk store with BLAKE3 hashing, zstd compression, and integrity verification on read
- FastCDC content-defined chunking with native deduplication
- SQLite catalog with 9 tables (libraries, tapes, files, chunks, jobs, ACLs, versioning)
- Tar ingest from stdin and tar export to stdout
- Zero-copy by-reference chunking (Manifest, Indexer, Verifier, ChunkResolver)
- QUIC transport layer with hub server and satellite client
- TLS with self-signed certificates and cert pinning (TOFU model)
- Hub init/serve and library add/list/remove/ping CLI commands
- Remote chunk fetch with hash verification and chunk-level resume
- Compressed wire transfer (chunks sent zstd-compressed from store, no decompress/recompress)
- Connection/stream limits (256 streams, 1024 connections) and timeouts (10s connect, 30s chunk, 300s idle)
- Endpoint validation at library add, relative cert path storage
- Cascade delete on library remove (files, chunks, tapes, jobs)
- Schema migrations for upgrading from earlier database versions
- Protobuf wire protocol
- Grade x CostTier scheduling matrix
- Transfer cost estimation without data transfer
- Resumable chunk-level fetching with job tracking
- 84 tests passing across all crates, 0 clippy warnings

What is planned:

- Catalog sync between hub and satellite (satellite currently needs metadata pre-populated)
- Connection reuse and 0-RTT reconnect
- Link cost auto-detection
- Tape ACL enforcement
- Cache eviction policies
- Mutual TLS authentication

## Contributing

Contributions are welcome.

- Open an issue to report bugs or discuss features before writing code.
- Fork the repo, create a branch, submit a pull request.
- Run `cargo test --workspace` before submitting. All tests must pass.
- Follow existing code style. No `unsafe` without justification.

## License

AGPL-3.0. See [LICENSE](LICENSE) for the full text.
