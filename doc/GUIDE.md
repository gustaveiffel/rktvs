# User Guide

A complete guide to using rk for content-addressed file delivery over hostile
networks.

## Table of Contents

1. [Installation](#1-installation)
2. [Concepts](#2-concepts)
3. [Getting Started](#3-getting-started)
4. [Storing Files Locally](#4-storing-files-locally)
5. [Browsing and Exporting](#5-browsing-and-exporting)
6. [Zero-Copy Indexing](#6-zero-copy-indexing)
7. [Verification](#7-verification)
8. [Running a Hub Server](#8-running-a-hub-server)
9. [Connecting as a Satellite](#9-connecting-as-a-satellite)
10. [Fetching Files](#10-fetching-files)
11. [Cost Estimation and Scheduling](#11-cost-estimation-and-scheduling)
12. [Job Management](#12-job-management)
13. [Data Directory Layout](#13-data-directory-layout)
14. [Hub-Satellite Tutorial](#14-hub-satellite-tutorial)
15. [Troubleshooting](#15-troubleshooting)
16. [Known Limitations](#16-known-limitations)

---

## 1. Installation

### Build from source

rk is written in Rust. You need:

- **Rust 1.85+** (edition 2024). Install via [rustup](https://rustup.rs/).
- **A C compiler** (gcc, clang, or MSVC) -- required by the bundled SQLite.

```bash
git clone https://github.com/rktvs/rk.git
cd rk
cargo build --release
```

The binary is at `target/release/rk`. Copy it to your PATH:

```bash
cp target/release/rk ~/.local/bin/    # or /usr/local/bin/
```

### Verify the build

```bash
rk --help
cargo test --workspace    # 84 tests, should all pass
```

---

## 2. Concepts

### Chunks

Files are split into variable-size blocks (~4 MB average) using content-defined
chunking (FastCDC). Each chunk is identified by its BLAKE3 hash and compressed
with zstd. Two files sharing content never store the same chunks twice.

### Tapes

A tape is a named logical volume -- think of it as a project or dataset. All
files within a tape share a namespace. Chunks are shared globally across tapes,
so adding the same file to two tapes costs almost nothing.

### Libraries

A library is a remote rk server (the "hub"). Satellites connect to libraries
to browse catalogs and fetch chunks. Trust is established via certificate
pinning.

### Catalog

The local SQLite database. It stores the file tree, the list of chunks per
file, known libraries, and job state. The catalog is the source of truth for
metadata -- chunk data is lazy-fetched.

### Grades and Cost Tiers

Transfer jobs have a **grade** (priority):
- **Urgent (P0)** -- transfer immediately regardless of link cost
- **Normal (P1)** -- transfer if link is not Expensive
- **Batch (P2)** -- transfer if link is Free or Cheap
- **Background (P3)** -- transfer only if link is Free

The network link is classified by **cost tier**: Free, Cheap, Metered, Expensive.
The Grade x CostTier matrix decides whether a job runs or waits.

### Data Directory

All rk state lives in a single directory: `~/.rk` by default. Override with
`--data-dir` or the `RK_DATA_DIR` environment variable.

---

## 3. Getting Started

Check version (includes protocol version for debugging compatibility issues):

```bash
rk --version
# rk 0.1.0 (protocol v2)
```

Every rk command operates on a data directory. You don't need to initialize
it explicitly -- it's created on first use.

```bash
# Use default data directory (~/.rk)
rk ls default/

# Use a custom data directory
rk --data-dir /path/to/data ls default/

# Or set the environment variable
export RK_DATA_DIR=/path/to/data
rk ls default/
```

---

## 4. Storing Files Locally

### Tar ingest

Pipe a tar stream into `rk ingest`. This reads the archive, splits files into
chunks, compresses and stores them, and records metadata in the catalog.

```bash
# Ingest from a directory
tar cf - /path/to/project/ | rk ingest --tape myproject

# Ingest a specific tar file
rk ingest --tape backups < /path/to/backup.tar

# Tape name defaults to "default" if omitted
tar cf - /data/ | rk ingest
```

Output:

```
ingested 42 files, 8 dirs, 157286400 bytes, 39 chunks
```

### What happens during ingest

1. Each regular file is split with FastCDC (1 MB min, 4 MB avg, 16 MB max).
2. Each chunk is hashed with BLAKE3, compressed with zstd, and written to disk.
3. Duplicate chunks are detected and skipped (content-addressed dedup).
4. File metadata (path, size, mtime, mode) and chunk mappings are recorded in
   the catalog.

---

## 5. Browsing and Exporting

### List files

Browse the catalog without touching any chunk data:

```bash
rk ls myproject/
rk ls myproject/src/
rk ls myproject/src/main.rs
```

For remote libraries (requires catalog sync):

```bash
rk ls myhub:project/src/
```

Output:

```
-644       1234  src/main.rs
-644        567  src/lib.rs
d755          0  src/utils/
```

The format is: `type+mode  size  path`. Type is `-` for files, `d` for
directories. If no files are found, a message is displayed with a hint
for remote libraries.

### Export to tar

Stream files back as a tar archive:

```bash
# Export an entire tape
rk tar myproject/ | tar xf - -C /tmp/restore/

# Export a subtree
rk tar myproject/src/ | tar xf - -C /tmp/src-only/

# Pipe to any tar-compatible tool
rk tar myproject/ | gzip > backup.tar.gz
```

The tar stream is written to stdout, so it composes with any Unix tool.

---

## 6. Zero-Copy Indexing

For large datasets, you may not want to copy file data into the chunk store.
`rk index` creates a manifest that maps chunks to their locations in the
original files. The data stays in place.

```bash
rk index --tape photos /path/to/photos/
```

Output:

```
indexed 1500 files, 12 dirs, 4200 chunks (4200 new, 0 dedup), 52428800000 bytes
```

### How it works

1. rk walks the directory and splits each file into chunks with FastCDC.
2. Instead of copying chunks, it records each chunk's source file path, byte
   offset, and length in a **manifest** file (`manifest.rkm`).
3. When reading chunks (e.g., during `rk tar`), the `ChunkResolver` reads
   directly from the original files using the manifest.
4. If a source file has been modified since indexing, the resolver falls back
   to the chunk store (if the chunk was stored there by other means).

### When to use index vs. ingest

| Scenario | Use |
|---|---|
| Files arrive as a tar stream (stdin) | `rk ingest` |
| Files are on local disk, you want dedup + compression | `rk ingest` |
| Files are on local disk, you want zero-copy references | `rk index` |
| Large datasets where copying is too slow/expensive | `rk index` |

---

## 7. Verification

After indexing files with `rk index`, verify that the source files haven't
changed:

```bash
# Fast check (size + mtime)
rk verify --tape photos

# Full BLAKE3 hash verification (slower, reads all data)
rk verify --tape photos --blake3

# Verify all tapes
rk verify
```

Output:

```
Checked:   4200
OK:        4198
Stale:     2
Missing:   0
Corrupted: 0
```

- **OK** -- chunk matches (or hasn't changed in fast mode).
- **Stale** -- source file has been modified since indexing.
- **Missing** -- source file no longer exists.
- **Corrupted** -- BLAKE3 hash mismatch (only in `--blake3` mode).

The command exits with an error if any chunks are stale, missing, or corrupted.

---

## 8. Running a Hub Server

A hub serves chunks to satellites over QUIC.

### Initialize

```bash
rk --data-dir /srv/rk-hub hub init --san 192.168.1.10
```

This generates a self-signed TLS certificate and private key:

```
hub initialized
  cert:        /srv/rk-hub/hub.cert.der
  key:         /srv/rk-hub/hub.key.der
  fingerprint: 3a7f...b2c1
  sans:        localhost, 127.0.0.1, 192.168.1.10
  listen:      0.0.0.0:4443

share /srv/rk-hub/hub.cert.der with satellites to connect
```

The `--san` flag adds Subject Alternative Names to the certificate. **You must
include every IP or hostname that satellites will use to connect.** The
certificate always includes `localhost` and `127.0.0.1`. You can pass `--san`
multiple times:

```bash
rk hub init --san 192.168.1.10 --san hub.example.com
```

If you forget a SAN, satellites connecting to that address will get a TLS
certificate mismatch error. The fix is to regenerate the certificate (see
Re-initialization below).

The certificate file (`hub.cert.der`) must be shared with satellites out-of-band
(copied via USB, scp, etc.). This is the Trust On First Use (TOFU) model.

### Add content

The hub needs content to serve. Ingest or index files into the hub's data
directory:

```bash
tar cf - /data/project/ | rk --data-dir /srv/rk-hub ingest --tape project
# or
rk --data-dir /srv/rk-hub index --tape project /data/project/
```

### Start serving

```bash
rk --data-dir /srv/rk-hub hub serve
```

```
hub listening on 0.0.0.0:4443
```

The hub listens for QUIC connections and serves chunks on request. Press
Ctrl-C to stop.

Custom listen address:

```bash
rk --data-dir /srv/rk-hub hub serve --listen 192.168.1.10:5000
```

### Re-initialization

If you need to regenerate the certificate (e.g., to add a missing SAN):

```bash
rm /srv/rk-hub/hub.cert.der /srv/rk-hub/hub.key.der
rk --data-dir /srv/rk-hub hub init --san 192.168.1.10 --listen 192.168.1.10:5000
```

All satellites will need the new certificate. On each satellite, remove the
old library and re-add with the new cert.

---

## 9. Connecting as a Satellite

### Add a library

Register a remote hub by its endpoint address and certificate:

```bash
rk library add myhub 192.168.1.10:4443 --cert /path/to/hub.cert.der
```

```
added library 'myhub' at 192.168.1.10:4443
  cert: certs/myhub.cert.der
```

The certificate is copied into the satellite's data directory and stored as a
**relative path** (e.g., `certs/myhub.cert.der`). This makes the data directory
relocatable. The original cert file can be deleted afterward.

The endpoint must be a valid `host:port` string. The hostname is extracted for
TLS Server Name Indication (SNI). Invalid endpoints are rejected at add time.

Library IDs must be alphanumeric with hyphens and underscores only
(e.g., `my-hub`, `office_west`, `hub01`).

To replace an existing library (e.g., after certificate rotation on the hub):

```bash
rk library add myhub 192.168.1.10:4443 --cert /new/hub.cert.der --force
```

### List libraries

```bash
rk library list
```

```
myhub            192.168.1.10:4443        online     last_seen=1739352000
backup-hub       10.0.0.5:4443            offline    last_seen=never
```

### Ping a library

Test connectivity to a registered library:

```bash
rk library ping myhub
```

```
ok (12.3ms)
```

The ping performs a full QUIC handshake with TLS verification against the
pinned certificate. On success, the library's status is updated to "online".

### Remove a library

```bash
rk library remove myhub
```

This performs a **cascade delete**: it removes the library from the catalog,
deletes all associated data (files, file_chunks, tapes, tape_versions, jobs),
and removes the stored certificate file.

---

## 10. Fetching Files

Fetch a file from a remote library:

```bash
rk fetch myhub:project/path/to/file.bin --grade normal
```

```
fetching myhub:project/path/to/file.bin (125 chunks, 524288000 bytes)
done: 120 fetched, 5 skipped (already local), 503316480 bytes transferred
```

Chunks are transferred **compressed** when possible. If the hub has the chunk
in its chunk store, it sends the zstd-compressed bytes directly -- no
decompress/recompress round-trip. This saves significant bandwidth on hostile
links. Chunks resolved from the manifest (zero-copy path) are sent
decompressed.

The connection has a **10-second timeout**. Each individual chunk fetch has a
**30-second timeout**. If either is exceeded, the operation fails with an error
(and can be resumed later -- chunks already fetched are skipped).

### Path format

Fetch paths use the format `<library>:<tape>/<path>`:

```
myhub:project/src/main.rs
backup-hub:photos/2024/vacation.tar
```

### Grades

Control transfer priority:

```bash
rk fetch myhub:project/file --grade urgent       # P0: transfer now
rk fetch myhub:project/file --grade normal        # P1: default
rk fetch myhub:project/file --grade batch         # P2: wait for cheap link
rk fetch myhub:project/file --grade background    # P3: wait for free link
```

Short forms work too: `p0`, `p1`, `p2`, `p3`.

### Resume

If a fetch is interrupted (network drop, Ctrl-C), re-running the same command
resumes from where it left off. Chunks already present locally are skipped.

### Current limitation

The satellite must have file/chunk metadata in its local catalog before
fetching. Automatic catalog sync between hub and satellite is not yet
implemented. For now, you can:

- Copy the hub's `catalog.db` to the satellite manually.
- Use the same catalog database on both sides.
- Populate the satellite's catalog through other means.

This is the next major feature planned for rk.

---

## 11. Cost Estimation and Scheduling

### Estimate transfer cost

Before fetching, check how much data would need to be transferred:

```bash
rk estimate myproject/large-dataset.bin
```

```
File: myproject/large-dataset.bin
  Total chunks:   125
  Local chunks:   5
  Missing chunks: 120
  Total size:     524288000 bytes
  Transfer est:   503316480 bytes
```

This tells you:
- How many chunks you already have locally (from previous fetches or shared
  content).
- How many bytes would actually move over the wire.
- No data is transferred during estimation.

### Scheduling matrix

The Grade x CostTier matrix determines whether a job runs:

|  | Free | Cheap | Metered | Expensive |
|---|---|---|---|---|
| **Urgent** | Run | Run | Run | Run |
| **Normal** | Run | Run | Run | Queue |
| **Batch** | Run | Run | Queue | Queue |
| **Background** | Run | Queue | Queue | Queue |

---

## 12. Job Management

Transfer operations create jobs tracked in the catalog.

### List jobs

```bash
rk jobs                    # all jobs
rk jobs --status pending   # only pending
rk jobs --status running   # only running
rk jobs --status completed # only completed
```

Output format:

```
fetch-3a7f1b2c  running  fetch  src/main.rs  [45/125]  normal
fetch-b91c4d8e  completed  fetch  data.bin  [200/200]  batch
```

Fields: job ID, status, type, file path, progress (completed/total chunks),
grade.

---

## 13. Data Directory Layout

```
<data-dir>/                          # ~/.rk by default
  catalog.db                         # SQLite database (metadata, jobs, libraries)
  catalog.db-wal                     # SQLite WAL file
  catalog.db-shm                     # SQLite shared memory file
  manifest.rkm                       # zero-copy chunk manifest (if index was used)
  hub.cert.der                       # hub TLS certificate (if hub init was run)
  hub.key.der                        # hub TLS private key (mode 0600)
  chunks/                            # content-addressed chunk store
    ab/
      ab3f...c7.zst                  # zstd-compressed chunk
    cd/
      cd91...e2.zst
    ...
  certs/                             # pinned certificates for known libraries
    myhub.cert.der
    backup-hub.cert.der
```

- **catalog.db** -- all metadata, file trees, chunk mappings, job state.
  Uses WAL mode for concurrent reads.
- **chunks/** -- flat content-addressed store. Two-character prefix directories
  derived from the BLAKE3 hash hex. Files are zstd-compressed.
- **manifest.rkm** -- maps chunk hashes to source file offsets. Created by
  `rk index`, used by `ChunkResolver` for zero-copy reads.
- **certs/** -- one DER-encoded certificate per registered library.

---

## 14. Hub-Satellite Tutorial

A complete walkthrough of setting up a hub and fetching files from a satellite.
This tutorial uses two terminals on the same machine, but the same steps work
across machines (add `--san <hub-ip>` at init time if connecting by IP).

### Step 1: Set up the hub

```bash
# Create hub data directory and initialize
mkdir -p /tmp/rk-hub
rk --data-dir /tmp/rk-hub hub init
# For cross-machine: rk --data-dir /tmp/rk-hub hub init --san 192.168.1.10
```

Note the certificate path and SANs in the output. You'll need the cert for
the satellite.

### Step 2: Add content to the hub

```bash
# Create some test data
mkdir -p /tmp/test-data
echo "hello from the hub" > /tmp/test-data/hello.txt
dd if=/dev/urandom of=/tmp/test-data/random.bin bs=1M count=10 2>/dev/null

# Ingest into the hub
tar cf - -C /tmp test-data/ | rk --data-dir /tmp/rk-hub ingest --tape demo
```

### Step 3: Start the hub

```bash
rk --data-dir /tmp/rk-hub hub serve
```

Leave this running in its terminal.

### Step 4: Set up the satellite (new terminal)

```bash
# Register the hub as a library
rk --data-dir /tmp/rk-sat library add myhub 127.0.0.1:4443 \
    --cert /tmp/rk-hub/hub.cert.der

# Verify connectivity
rk --data-dir /tmp/rk-sat library ping myhub

# List registered libraries
rk --data-dir /tmp/rk-sat library list
```

### Step 5: Fetch files

For the satellite to fetch, it needs catalog metadata. In this tutorial we copy
the hub's catalog:

```bash
# Copy catalog from hub to satellite (temporary workaround)
cp /tmp/rk-hub/catalog.db /tmp/rk-sat/catalog.db
```

Now fetch:

```bash
rk --data-dir /tmp/rk-sat fetch myhub:demo/test-data/random.bin
```

### Step 6: Verify the fetch

```bash
# Check jobs
rk --data-dir /tmp/rk-sat jobs

# The file's chunks are now in the satellite's chunk store
ls /tmp/rk-sat/chunks/
```

### Step 7: Clean up

Stop the hub with Ctrl-C, then:

```bash
rm -rf /tmp/rk-hub /tmp/rk-sat /tmp/test-data
```

---

## 15. Troubleshooting

### "no certificate found -- run `rk hub init` first"

You tried to run `hub serve` without initializing. Run `rk hub init` first.

### "hub already initialized"

The certificate already exists. To regenerate, delete the cert and key files
first, then re-run `hub init`.

### "invalid library ID"

Library IDs must contain only alphanumeric characters, hyphens, and underscores.
Examples: `my-hub`, `hub_01`, `office`. Not allowed: `../hack`, `my hub`,
`hub@home`.

### "certificate file not found"

The `--cert` path passed to `library add` doesn't exist. Check the path. The
cert is typically at `<hub-data-dir>/hub.cert.der`.

### "ping failed: ... certificate not valid for name"

The hub's TLS certificate does not include the IP or hostname the satellite is
connecting to. Regenerate the hub cert with the correct `--san` flag:

```bash
rm <hub-data-dir>/hub.cert.der <hub-data-dir>/hub.key.der
rk hub init --san <hub-ip>
```

Then redistribute the new cert to all satellites.

### "ping failed: connection refused"

The hub is not running or the address is wrong. Check:
- Is `rk hub serve` running?
- Is the endpoint address correct in `library list`?
- Are firewall rules allowing UDP traffic on the port (QUIC uses UDP)?

### "handshake rejected: protocol version ... too old"

The hub and satellite are running different protocol versions. Both sides must
be upgraded to the same version. Rebuild and redeploy the outdated binary.

### "no chunk metadata for ..."

The satellite doesn't have catalog metadata for the requested file. Catalog
sync is not yet implemented. See the tutorial for how to copy the catalog
manually.

### "hash mismatch for chunk N"

The hub returned data that doesn't match the expected BLAKE3 hash. This
indicates data corruption in transit or on the hub. The fetch is aborted to
prevent storing corrupt data.

### "connection timed out"

The satellite could not establish a QUIC connection to the hub within 10
seconds. Check:
- Is the hub running and reachable?
- Is the endpoint address correct (verify with `rk library list`)?
- Is UDP traffic allowed on the port? QUIC uses UDP, not TCP.
- Is the network particularly slow? The 10-second timeout is not configurable.

### "chunk fetch timed out"

A single chunk fetch took longer than 30 seconds. This can happen on very slow
links with large chunks. The fetch can be retried -- already-fetched chunks
will be skipped automatically.

### Enabling debug logging

rk uses the `tracing` framework. Set the `RUST_LOG` environment variable:

```bash
RUST_LOG=debug rk hub serve
RUST_LOG=rk_transport=trace rk library ping myhub
```

---

## 16. Known Limitations

- **No catalog sync.** Satellites need metadata pre-populated. This is the most
  significant gap and the next planned feature.
- **Single connection per fetch.** Each `rk fetch` creates a new QUIC endpoint.
  Connection reuse and 0-RTT reconnect are not yet implemented.
- **No client authentication.** Any client with the hub's public certificate
  can connect. Mutual TLS and auth tokens are planned.
- **No parallel chunk fetching.** Chunks are fetched one at a time.
- **No cache eviction.** The chunk store grows without bound. Manual cleanup
  is required.
- **CLI only.** No GUI, no web interface, no REST API.

See [COMPARED.md](COMPARED.md) for a detailed comparison with other tools.
