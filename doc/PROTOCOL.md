# Wire Protocol

## 1. Overview

rk uses QUIC (via [quinn](https://github.com/quinn-rs/quinn)) for all
hub-to-satellite communication. There is one QUIC connection per satellite per
library. Streams are multiplexed over that connection. Messages are protobuf
([prost](https://github.com/tokio-rs/prost)), length-delimited.

## 2. Connection establishment

1. The satellite creates a QUIC client endpoint and connects to the hub address
   with TLS 1.3 (rustls).
2. The hub has a self-signed certificate generated at `rk hub init`. The
   certificate and private key are stored as DER files in the hub's data
   directory (`hub.cert.der`, `hub.key.der`). The certificate's Subject
   Alternative Names (SANs) must include every IP or hostname satellites
   will use to connect (`--san` flag at init time). Defaults: `localhost`,
   `127.0.0.1`.
3. The satellite pins the hub's certificate at `rk library add --cert`. The
   cert is copied to `<data-dir>/certs/<library-id>.cert.der` and stored as a
   **relative path** (`certs/<id>.cert.der`) in the catalog. This makes the
   data directory relocatable.
4. The endpoint is validated at `library add` time: it must be a valid
   `host:port` string. The hostname is extracted for TLS Server Name
   Indication (SNI).
5. Trust model: **Trust On First Use (TOFU).** The hub's cert is transferred
   out-of-band (copied manually, USB key, etc.) and pinned by the satellite.
   There is no CA.

**Planned:**
- WireGuard as identity fabric and network-level encryption; QUIC on top.
- Mutual TLS authentication (mTLS) for multi-tenant deployments.

## 3. Stream types

Each bidirectional QUIC stream begins with a **1-byte tag** written by the
opener (always the satellite).

| Tag    | Name            | Purpose                             |
|--------|-----------------|-------------------------------------|
| `0x00` | CONTROL         | Handshake, heartbeat (future)       |
| `0x01` | CHUNK_REQUEST   | Fetch a single chunk by hash        |
| `0x02` | CATALOG_SYNC    | List tapes or sync file metadata    |

Future stream types (not yet implemented):

| Tag    | Name            | Purpose                       |
|--------|-----------------|-------------------------------|
| `0x03` | JOB_NEGOTIATE   | Job creation/cancellation     |

## 4. Message format

All messages after the stream tag are **length-delimited protobuf**: a varint
length prefix followed by the protobuf-encoded bytes. This corresponds to
prost's `encode_length_delimited` / `decode_length_delimited`.

## 5. CONTROL stream (tag 0x00)

Opened once by the satellite immediately after connecting.

```
Satellite -> Hub:  [0x00] [len-delimited Handshake]
Hub -> Satellite:  [len-delimited HandshakeAck]
```

```protobuf
message Handshake {
  string satellite_id = 1;
  uint32 protocol_version = 2;
}

message HandshakeAck {
  bool ok = 1;
  string message = 2;
  uint32 protocol_version = 3;
}
```

Current `protocol_version`: **5** (v2: compressed wire transfer, v3: catalog sync, v4: catalog size lookup, v5: push protocol).

**Version negotiation:** the satellite sends its version in `Handshake`. The
hub checks it against its minimum supported version and rejects with
`ok = false` and a descriptive message if too old. The hub sends its own
version back in `HandshakeAck.protocol_version`. The satellite checks the
hub's version and aborts if too old. Both sides must be at version >= 2.
Catalog sync requires the hub to be at version >= 3.

## 6. CHUNK_REQUEST stream (tag 0x01)

Opened per chunk fetch. One stream per chunk.

```
Satellite -> Hub:  [0x01] [len-delimited ChunkRequest]
Hub -> Satellite:  [len-delimited ChunkResponse]
```

```protobuf
message ChunkRequest {
  bytes hash = 1;  // 32-byte BLAKE3 hash
}

message ChunkResponse {
  bool found = 1;
  bytes data = 2;        // chunk data (compressed or raw, see `compressed` flag)
  uint64 size = 3;       // decompressed size in bytes
  bool compressed = 4;   // true if `data` is zstd-compressed
}
```

Chunks in the store are zstd-compressed on disk. The hub serves them in two
modes depending on how the chunk is resolved:

- **From chunk store:** `data` contains the raw zstd-compressed bytes as stored
  on disk, `compressed = true`. This avoids a decompress/recompress round-trip
  and saves bandwidth -- exactly the resource rk is designed to conserve.
- **From manifest (zero-copy path):** `data` contains decompressed bytes read
  from the source file, `compressed = false`.

The satellite checks the `compressed` flag: if true, it calls
`ChunkStore::put_compressed()` (which verifies the hash by decompressing
internally); if false, it verifies the BLAKE3 hash and calls
`ChunkStore::put()`.

## 7. CATALOG_SYNC stream (tag 0x02)

Opened by the satellite to list tapes or sync file metadata for a tape.
Introduced in protocol version 3.

```
Satellite -> Hub:  [0x02] [len-delimited CatalogSyncRequest]
Hub -> Satellite:  [len-delimited CatalogSyncResponse]
```

```protobuf
message CatalogSyncRequest {
  string tape = 1;    // empty = list tapes, non-empty = sync that tape
}

message TapeInfo {
  string name = 1;
  uint64 file_count = 2;
  uint64 total_size = 3;
}

message ChunkInfo {
  bytes hash = 1;      // 32-byte BLAKE3 hash
  uint64 offset = 2;
  uint64 size = 3;
}

message FileMetadata {
  string path = 1;
  int64 entry_type = 2;   // 1 = regular file, 2 = directory
  uint64 size = 3;
  uint64 mtime = 4;
  uint32 mode = 5;
  uint64 version = 6;
  repeated ChunkInfo chunks = 7;
}

message CatalogSyncResponse {
  bool ok = 1;
  string message = 2;
  repeated TapeInfo tapes = 3;     // populated when listing tapes
  repeated FileMetadata files = 4; // populated when syncing a tape
}
```

**Two modes:**

- **List tapes** (`tape` field is empty): the hub responds with `tapes` listing
  all tapes in its catalog with file count and total size. `files` is empty.
- **Sync tape** (`tape` field is set): the hub responds with `files` containing
  all files in that tape, each with their full chunk list. `tapes` is empty.

The hub queries its catalog with `library_id = "local"`. The satellite stores
the received metadata under the actual library ID.

**Response size limit:** 4 MB max. This accommodates ~6000 files with chunk
lists. For very large tapes, pagination will be added in a future version.

**Error cases:**
- Hub has no catalog configured: `ok = false`, `message = "hub has no catalog configured"`.
- Hub protocol version < 3: the satellite checks `hub_protocol_version` and
  fails before opening the stream.

## 8. Push protocol (tags 0x03, 0x04, 0x05)

Introduced in protocol version 5. Allows a client to upload chunks and
register files on the hub.

### 8a. HAVE_CHECK stream (tag 0x03)

Batch query: the client asks which chunks the hub already has.

```
Client -> Hub:  [0x03] [len-delimited HaveCheckRequest]
Hub -> Client:  [len-delimited HaveCheckResponse]
```

```protobuf
message HaveCheckRequest {
  repeated bytes chunk_hashes = 1;  // 32-byte BLAKE3 hashes
}

message HaveCheckResponse {
  repeated bool have = 1;  // same order as request
}
```

**Limits:** max 10,000 hashes per request, max 512 KB request size.

### 8b. CHUNK_PUSH stream (tag 0x04)

Push a single zstd-compressed chunk. The hub verifies the hash before storing.

```
Client -> Hub:  [0x04] [len-delimited ChunkPushRequest]
Hub -> Client:  [len-delimited ChunkPushResponse]
```

```protobuf
message ChunkPushRequest {
  bytes hash = 1;               // 32-byte BLAKE3 hash
  bytes compressed_data = 2;    // zstd-compressed chunk bytes
  uint64 decompressed_size = 3;
}

message ChunkPushResponse {
  bool ok = 1;
  string message = 2;
}
```

**Validation:** the hub decompresses the data, checks the decompressed size
matches, and verifies the BLAKE3 hash. On mismatch, `ok = false` with a
descriptive message. Max request size: 17 MB.

Pushing a chunk that already exists is idempotent (returns `ok = true`).

### 8c. MANIFEST_PUSH stream (tag 0x05)

Register a file in the hub's catalog after all its chunks have been pushed.

```
Client -> Hub:  [0x05] [len-delimited ManifestPushRequest]
Hub -> Client:  [len-delimited ManifestPushResponse]
```

```protobuf
message ManifestPushRequest {
  string filename = 1;
  uint64 file_size = 2;
  repeated ChunkInfo chunks = 3;  // reuses existing ChunkInfo
}

message ManifestPushResponse {
  bool ok = 1;
  string message = 2;
}
```

**Filename validation:** rejected if empty, contains NUL bytes, contains `..`
(path traversal), or exceeds 4096 bytes.

Files are recorded under `library_id = "local"`, `tape = "uploads"`.

### 8d. Push flow example

```
[Client]                              [Hub]
    |                                   |
    |--- stream 0: [0x00] Handshake --->|
    |<-- HandshakeAck {ok: true} -------|
    |                                   |
    |--- stream 1: [0x03] HaveCheck --->|  (batch of hashes)
    |<-- HaveCheckResponse {have} ------|
    |                                   |
    |--- stream 2: [0x04] ChunkPush -->|  (chunk not on hub)
    |<-- ChunkPushResponse {ok} --------|
    |                                   |
    |--- stream 3: [0x04] ChunkPush -->|  (parallel)
    |<-- ChunkPushResponse {ok} --------|
    |                                   |
    |--- stream 4: [0x05] Manifest ---->|
    |<-- ManifestPushResponse {ok} -----|
```

## 9. Fetch flow example

```
[Satellite]                           [Hub]
    |                                   |
    |--- QUIC connect ----------------->|
    |<-- QUIC accept -------------------|
    |                                   |
    |--- stream 0: [0x00] Handshake --->|
    |<-- HandshakeAck {ok: true} -------|
    |                                   |
    |--- stream 1: [0x02] CatalogSync ->|  (list tapes)
    |<-- CatalogSyncResponse {tapes} ---|
    |                                   |
    |--- stream 2: [0x02] CatalogSync ->|  (sync tape "docs")
    |<-- CatalogSyncResponse {files} ---|
    |                                   |
    |--- stream 3: [0x01] ChunkReq --->|
    |<-- ChunkResponse {found, data} ---|
    |                                   |
    |--- stream 4: [0x01] ChunkReq --->|
    |<-- ChunkResponse {found, data} ---|
    |                                   |
    |--- connection close -------------->|
```

Multiple streams can be opened concurrently (QUIC multiplexing).

## 10. Error handling

- **Unknown stream tag:** the hub closes the stream with an error.
- **Chunk not found:** the hub responds with `ChunkResponse { found: false }`.
- **Hash mismatch:** the satellite verifies the BLAKE3 hash after receiving the
  data and rejects chunks that don't match. The fetch is aborted with an error.
- **Connection drop:** the satellite can reconnect and resume, skipping
  already-fetched chunks (chunk-level resume).

## 11. Security

**Current:**
- TLS 1.3 via rustls with self-signed certificates (rcgen).
- Certificate pinning: satellite stores hub's cert at `library add` time.
- Private keys stored with 0o600 permissions, created atomically (no TOCTOU).
- Library IDs are validated (alphanumeric + hyphens + underscores only) to
  prevent path traversal.
- Fetched chunks are verified against their BLAKE3 hash before storage.
- Certificate fingerprint (BLAKE3 of cert DER) displayed at `hub init`.

**Transport hardening (hub side):**
- Max 256 concurrent bidirectional streams per connection.
- 300-second idle timeout (connection closed if no activity).
- 15-second keep-alive interval.
- Max 1024 concurrent connections (enforced via semaphore).
- Chunk resolution runs on `spawn_blocking` to avoid stalling the async runtime.

**Transport hardening (satellite side):**
- 10-second connect timeout when establishing a QUIC connection.
- 30-second per-chunk fetch timeout.

**Planned:**
- WireGuard public key as node identity.
- Mutual TLS authentication.
- Certificate expiration and rotation.
