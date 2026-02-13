# Developer API Guide

This guide documents the public API of rk's four crates for developers who
want to use them as Rust libraries. The `rk` CLI binary (`src/main.rs`)
demonstrates how all four crates compose.

## Crate overview

```
rk-core        Chunk store, catalog (SQLite), chunker (FastCDC), manifest,
               indexer, verifier, chunk resolver
rk-tar         Streaming tar ingest and export
rk-transport   QUIC hub server, satellite client, TLS cert management
rk-scheduler   Grade/CostTier scheduling, cost estimation, resumable fetcher
```

Dependency graph:

```
rk (CLI binary)
  ├── rk-core
  ├── rk-tar        --> rk-core
  ├── rk-transport  --> rk-core
  └── rk-scheduler  --> rk-core, rk-transport
```

---

## rk-core

The foundational crate. All other crates depend on it.

### ChunkStore

Content-addressed storage on disk. Chunks are zstd-compressed and keyed by
BLAKE3 hash. Layout: `<base>/chunks/<first 2 hex>/<hash>.zst`.

```rust
use rk_core::chunk_store::ChunkStore;

let store = ChunkStore::new(data_dir);

// Store raw data, returns BLAKE3 hash. Atomic writes (temp + rename).
let hash = store.put(data)?;

// Check existence.
let exists = store.has(&hash);

// Retrieve and decompress. Verifies BLAKE3 hash on read.
let data = store.get(&hash)?;

// Get on-disk compressed size in bytes.
let csize = store.compressed_size(&hash)?;

// Compressed wire transfer support:
// Read raw zstd bytes (no decompress, no hash verify).
let compressed = store.get_compressed(&hash)?;

// Store pre-compressed zstd bytes. Verifies hash internally
// (decompresses, checks BLAKE3, writes if valid).
store.put_compressed(&hash, &compressed)?;
```

**Errors:** `Error::ChunkNotFound(hash)` if the chunk doesn't exist,
`Error::HashMismatch { expected, actual }` on corruption.

### Catalog

SQLite-backed metadata store. 9 tables, WAL mode, created on first open.

```rust
use rk_core::catalog::Catalog;

// Open from a file path (creates database if needed).
let catalog = Catalog::open(Path::new("catalog.db"))?;

// Open in-memory (useful for tests).
let catalog = Catalog::open_in_memory()?;
```

Schema migrations run automatically on open (`Catalog::migrate()`), handling
upgrades from earlier database versions.

#### Library CRUD

```rust
// Register a library. cert_path is stored as-is (use relative paths).
catalog.add_library("hub1", "Hub One", "192.168.1.1:4443", "certs/hub1.cert.der")?;

// Get a single library.
let lib: Option<LibraryRecord> = catalog.get_library("hub1")?;

// List all libraries.
let libs: Vec<LibraryRecord> = catalog.list_libraries()?;

// Update status + last_seen timestamp.
catalog.update_library_status("hub1", "online")?;

// Cascade delete: removes library + all files, file_chunks, tapes,
// tape_versions, and jobs for that library in a single transaction.
catalog.remove_library("hub1")?;
```

**LibraryRecord fields:** `library_id`, `display_name`, `endpoint`, `status`,
`last_seen: Option<i64>`, `cert_path: Option<String>`.

#### File operations

```rust
use rk_core::chunker::ChunkMeta;

// Record a file and its chunk list.
catalog.record_file(
    library_id, tape, path,
    entry_type,   // 1 = regular file, 2 = directory
    size, mtime, mode, version,
    &chunks,      // &[ChunkMeta]
)?;

// Get ordered chunk list for a file.
let chunks: Vec<ChunkMeta> = catalog.get_file_chunks(library_id, tape, path)?;

// List files by prefix.
let files: Vec<FileEntry> = catalog.list_files(library_id, tape, "/src/")?;
```

#### Job operations

```rust
// Create a job.
catalog.create_job(job_id, library_id, tape, "fetch", grade, file_path, total_chunks, total_bytes)?;

// Get a job by ID.
let job: Option<JobRecord> = catalog.get_job("job-1")?;

// List jobs, optionally filtered by status.
let jobs: Vec<JobRecord> = catalog.list_jobs(Some("running"))?;
let all_jobs: Vec<JobRecord> = catalog.list_jobs(None)?;

// Update progress.
catalog.update_job_progress("job-1", completed_chunks, "running")?;

// Cancel.
catalog.cancel_job("job-1")?;
```

### Chunker

Content-defined chunking with FastCDC.

```rust
use rk_core::chunker::{chunk_data, ingest, ChunkMeta, IngestResult};

// Split data into chunks and return metadata (no storage).
let chunks: Vec<ChunkMeta> = chunk_data(data, min, avg, max);
// Default sizes: 1 MB min, 4 MB avg, 16 MB max

// Split, compress, and store chunks in one step.
let result: IngestResult = ingest(data, &store, min, avg, max)?;
// result.chunks: Vec<ChunkMeta>
// result.total_size: u64
```

**ChunkMeta fields:** `hash: blake3::Hash`, `offset: u64`, `size: usize`,
`compressed_size: usize`.

### Manifest

Zero-copy by-reference chunking. Maps chunk hashes to locations in source
files on disk instead of copying data.

```rust
use rk_core::manifest::{Manifest, ChunkRef, SourceFile};

let mut manifest = Manifest::new();

// Register a source file. Returns a path index.
let idx = manifest.add_source("/data/file.bin".into(), file_size, file_mtime);

// Map a chunk hash to a location in the source file.
manifest.insert_chunk(hash, ChunkRef {
    path_index: idx,
    offset: 0,
    length: 4_194_304,
});

// Query.
let has = manifest.has_chunk(&hash);
let chunk_ref: Option<&ChunkRef> = manifest.get_chunk(&hash);

// Persist to / read from disk.
manifest.write_to_file(Path::new("manifest.rkm"))?;
let manifest = Manifest::read_from_file(Path::new("manifest.rkm"))?;
```

### ChunkResolver

Resolves chunk data from the manifest (zero-copy) first, falling back to the
chunk store.

```rust
use rk_core::resolver::ChunkResolver;

let resolver = ChunkResolver::new(Some(&manifest), &store);

// Optionally enable BLAKE3 verification on manifest reads.
let resolver = resolver.with_verify(true);

// Read chunk data (tries manifest first, then store).
let data: Vec<u8> = resolver.get(&hash)?;

// Check if a chunk is available (manifest or store).
let available = resolver.has(&hash);
```

### Indexer

Walk a directory, chunk files by reference, populate manifest + catalog.

```rust
use rk_core::indexer::{index_directory, IndexConfig, IndexStats};

let config = IndexConfig::default(); // 1 MB / 4 MB / 16 MB
let stats: IndexStats = index_directory(
    Path::new("/data/photos"),
    &mut manifest, &catalog,
    "local", "photos",
    &config,
)?;
// stats: files_indexed, dirs_found, chunks_total, chunks_new, chunks_dedup, total_bytes
```

### Verifier

Check that source files referenced by a manifest haven't changed.

```rust
use rk_core::verifier::{verify_manifest, VerifyResult};

// Fast mode: check size + mtime only.
let result: VerifyResult = verify_manifest(&manifest, false);

// Full mode: re-read and verify BLAKE3 hashes.
let result: VerifyResult = verify_manifest(&manifest, true);

// result: chunks_checked, chunks_ok, chunks_stale, chunks_missing, chunks_corrupted
```

---

## rk-tar

Streaming tar interface. Depends on `rk-core`.

### Ingest

Read a tar archive, chunk and store its contents.

```rust
use rk_tar::ingest::{ingest_tar, IngestStats};

let stats: IngestStats = ingest_tar(
    reader,       // impl Read (e.g., stdin)
    &store, &catalog,
    "local", "myproject",
    1_048_576, 4_194_304, 16_777_216,  // min, avg, max chunk sizes
)?;
// stats: files, dirs, bytes, chunks
```

### Export

Write files from the catalog as a tar archive.

```rust
use rk_tar::export::export_tar;

export_tar(
    writer,       // impl Write (e.g., stdout)
    &resolver, &catalog,
    "local", "myproject",
    "/",          // path prefix filter
)?;
```

---

## rk-transport

QUIC transport layer. Depends on `rk-core`.

### cert module

TLS certificate management for the TOFU model.

```rust
use rk_transport::cert;

// Generate a self-signed cert for localhost.
let (cert, key) = cert::generate_self_signed()?;

// Generate with custom SANs (e.g., for a specific hostname or IP).
let (cert, key) = cert::generate_self_signed_for(vec![
    "myhub.example.com".into(),
    "192.168.1.10".into(),
])?;

// Persist to disk. Keys are saved with mode 0o600 (atomic, no TOCTOU).
cert::save_cert(&cert, Path::new("hub.cert.der"))?;
cert::save_key(&key, Path::new("hub.key.der"))?;

// Load from disk.
let cert = cert::load_cert(Path::new("hub.cert.der"))?;
let key = cert::load_key(Path::new("hub.key.der"))?;

// Build quinn configs.
// Server config includes transport hardening:
//   - 256 max concurrent bidi streams
//   - 300s idle timeout
//   - 15s keep-alive interval
let server_config = cert::server_config(cert.clone(), key)?;

// Client config pins a specific certificate.
let client_config = cert::client_config(&cert)?;
```

### Hub

QUIC server that serves chunks to satellites.

```rust
use rk_transport::hub::Hub;
use std::sync::Arc;

let store = Arc::new(ChunkStore::new(data_dir));
let manifest = None; // or Some(Arc::new(manifest))

let hub = Hub::bind(
    "0.0.0.0:4443".parse()?,
    server_config,
    store,
    manifest,
).await?;

let addr = hub.local_addr();

// Run forever, accepting connections.
hub.run().await;
```

The hub enforces a **1024 max connections** semaphore. Chunk resolution runs
on `spawn_blocking` to avoid stalling the async runtime. Chunks from the store
are served compressed (`compressed = true`); chunks from the manifest are
served decompressed (`compressed = false`).

### Satellite

QUIC client that connects to a hub and fetches chunks.

```rust
use rk_transport::satellite::{Satellite, ChunkFetchResult};

let satellite = Satellite::connect(
    hub_addr,           // SocketAddr
    "myhub.example.com", // server_name for TLS SNI
    client_config,
    "my-satellite-id",
).await?;

// Fetch a single chunk by BLAKE3 hash.
// Returns None if the hub doesn't have it.
let result: Option<ChunkFetchResult> = satellite.fetch_chunk(&hash).await?;
if let Some(r) = result {
    if r.compressed {
        store.put_compressed(&hash, &r.data)?;
    } else {
        let actual = blake3::hash(&r.data);
        assert_eq!(actual, hash);
        store.put(&r.data)?;
    }
}
```

**ChunkFetchResult fields:** `data: Vec<u8>`, `compressed: bool`.

---

## rk-scheduler

Scheduling and transfer orchestration. Depends on `rk-core` and `rk-transport`.

### types

Grade/CostTier decision matrix.

```rust
use rk_scheduler::types::{Grade, CostTier, Decision, decide};

let decision = decide(Grade::Normal, CostTier::Metered);
assert_eq!(decision, Decision::Run);

let decision = decide(Grade::Background, CostTier::Cheap);
assert_eq!(decision, Decision::Queue);

// Parse from strings (CLI input).
let grade: Grade = "normal".parse()?;   // also: urgent, batch, background, p0-p3
let tier: CostTier = "metered".parse()?;

// Convert to i32 for storage.
let grade_i32 = Grade::Normal as i32;   // 1
let grade = Grade::from_i32(1);         // Some(Normal)
```

### estimate

Calculate transfer cost without transferring data.

```rust
use rk_scheduler::estimate::{estimate_file, Estimate};

let est: Estimate = estimate_file(
    &catalog, &resolver,
    "myhub", "project", "/path/to/file.bin",
)?;
// est: total_chunks, local_chunks, missing_chunks, total_bytes, transfer_bytes
```

### fetcher

Resumable chunk-by-chunk file fetcher.

```rust
use rk_scheduler::fetcher::{fetch_file, FetchResult};

let result: FetchResult = fetch_file(
    &satellite, &resolver, &store, &catalog,
    "myhub", "project", "/path/to/file.bin",
    Some("job-id"),  // optional job ID for progress tracking
).await?;
// result: total_chunks, fetched_chunks, skipped_chunks, bytes_transferred
```

The fetcher handles both compressed and decompressed responses from the hub
automatically. It skips chunks already present in the resolver (resume
support) and updates job progress in the catalog after each chunk.

---

## Examples

### Minimal hub server

```rust
use std::sync::Arc;
use rk_core::chunk_store::ChunkStore;
use rk_transport::{cert, hub::Hub};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (cert, key) = cert::generate_self_signed()?;
    cert::save_cert(&cert, Path::new("/srv/hub/hub.cert.der"))?;
    cert::save_key(&key, Path::new("/srv/hub/hub.key.der"))?;

    let config = cert::server_config(cert, key)?;
    let store = Arc::new(ChunkStore::new("/srv/hub".into()));

    let hub = Hub::bind("0.0.0.0:4443".parse()?, config, store, None).await?;
    println!("hub listening on {}", hub.local_addr());
    hub.run().await;
    Ok(())
}
```

### Minimal satellite fetch

```rust
use std::path::Path;
use rk_core::{catalog::Catalog, chunk_store::ChunkStore, resolver::ChunkResolver};
use rk_transport::{cert, satellite::Satellite};
use rk_scheduler::fetcher::fetch_file;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = ChunkStore::new("/tmp/sat".into());
    let catalog = Catalog::open(Path::new("/tmp/sat/catalog.db"))?;
    let resolver = ChunkResolver::new(None, &store);

    let hub_cert = cert::load_cert(Path::new("/tmp/sat/certs/myhub.cert.der"))?;
    let client_config = cert::client_config(&hub_cert)?;

    let sat = Satellite::connect(
        "192.168.1.10:4443".parse()?,
        "192.168.1.10",
        client_config,
        "my-sat",
    ).await?;

    let result = fetch_file(
        &sat, &resolver, &store, &catalog,
        "myhub", "project", "/data/file.bin",
        None,
    ).await?;

    println!("fetched {}/{} chunks", result.fetched_chunks, result.total_chunks);
    Ok(())
}
```

### Embedding ChunkStore + Catalog

```rust
use std::path::Path;
use rk_core::{catalog::Catalog, chunk_store::ChunkStore, chunker};

fn main() -> anyhow::Result<()> {
    let store = ChunkStore::new("/tmp/myapp".into());
    let catalog = Catalog::open(Path::new("/tmp/myapp/catalog.db"))?;

    // Store a file.
    let data = std::fs::read("/path/to/file.bin")?;
    let result = chunker::ingest(&data, &store, 1_048_576, 4_194_304, 16_777_216)?;

    catalog.record_file(
        "local", "mydata", "/file.bin",
        1, data.len() as u64, None, None, 1,
        &result.chunks,
    )?;

    // Read it back.
    let chunks = catalog.get_file_chunks("local", "mydata", "/file.bin")?;
    let mut reconstructed = Vec::new();
    for chunk in &chunks {
        reconstructed.extend_from_slice(&store.get(&chunk.hash)?);
    }
    assert_eq!(reconstructed, data);

    Ok(())
}
```
