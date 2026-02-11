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

The binary is at `target/release/rk`.

### Basic usage

Ingest files from a tar stream:

```bash
tar cf - /path/to/files/ | rk ingest --tape myproject
```

List what was ingested:

```bash
rk ls myproject/
```

Export back to tar:

```bash
rk tar myproject/some/subdir | tar xf - -C /tmp/restore/
```

Estimate transfer cost for a remote file (no data transfer):

```bash
rk estimate myproject/large-dataset.bin
```

Check active transfer jobs:

```bash
rk jobs
rk jobs --status pending
```

Data is stored in `~/.rk` by default. Override with `--data-dir` or the `RK_DATA_DIR` environment variable.

## CLI Reference

| Command | Description |
|---|---|
| `rk ingest --tape <name>` | Read a tar archive from stdin, chunk and store it |
| `rk tar <tape>/<path>` | Export files as a tar archive to stdout |
| `rk ls <tape>/<path>` | List files in the catalog |
| `rk estimate <tape>/<path>` | Estimate transfer cost (chunks needed, bytes to fetch) |
| `rk jobs [--status <s>]` | List transfer jobs, optionally filtered by status |

## Architecture

The project is a Cargo workspace with four crates:

| Crate | Purpose |
|---|---|
| `rk-core` | ChunkStore (BLAKE3 + zstd), FastCDC chunker, SQLite catalog (9 tables) |
| `rk-tar` | Tar archive ingest and export via streaming |
| `rk-transport` | QUIC hub server and satellite client (quinn), protobuf wire protocol |
| `rk-scheduler` | Grade/CostTier decision matrix, job queue, transfer estimation, resumable chunk fetcher |

The top-level `src/main.rs` provides the `rk` CLI built with clap.

Key concepts:

- **Library** -- an rk server (the hub). Satellites connect to libraries to sync catalogs and fetch chunks.
- **Tape** -- a named logical volume grouping files with ACLs, sync policy, and versioning. Chunks are shared across tapes.
- **Chunk** -- a variable-size data block identified by its BLAKE3 hash, compressed with zstd.
- **Grade** -- transfer priority: Urgent, Normal, Batch, Background.
- **CostTier** -- link classification: Free, Cheap, Metered, Expensive.

See [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md), [doc/PROTOCOL.md](doc/PROTOCOL.md), [doc/TAPES.md](doc/TAPES.md), and [doc/COMPARED.md](doc/COMPARED.md) for details.

## Status

**MVP -- 4 weeks of development.**

What works today:

- Content-addressed chunk store with BLAKE3 hashing, zstd compression, and integrity verification on read
- FastCDC content-defined chunking with native deduplication
- SQLite catalog with 9 tables (libraries, tapes, files, chunks, jobs, ACLs, versioning)
- Tar ingest from stdin and tar export to stdout
- QUIC transport layer with hub server and satellite client (handshake, catalog sync, chunk fetch)
- Protobuf wire protocol
- Grade x CostTier scheduling matrix
- Transfer cost estimation without data transfer
- Resumable chunk-level fetching
- Job queue with create, list, progress tracking, and cancellation
- 35 tests passing across all crates

What is planned:

- Multi-library catalog sync
- Link cost auto-detection
- Merkle tree verification
- Tape ACL enforcement
- Cache eviction policies
- Hub server mode (`rk hub serve`)
- `rk fetch` with grade-based scheduling over the network

## Contributing

Contributions are welcome.

- Open an issue to report bugs or discuss features before writing code.
- Fork the repo, create a branch, submit a pull request.
- Run `cargo test --workspace` before submitting. All tests must pass.
- Follow existing code style. No `unsafe` without justification.

## License

AGPL-3.0. See [LICENSE](LICENSE) for the full text.
