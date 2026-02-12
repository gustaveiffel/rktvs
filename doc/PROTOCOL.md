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
   directory (`hub.cert.der`, `hub.key.der`).
3. The satellite pins the hub's certificate at `rk library add --cert`. The
   cert is copied to `<data-dir>/certs/<library-id>.cert.der` and used for
   all subsequent connections to that library.
4. Trust model: **Trust On First Use (TOFU).** The hub's cert is transferred
   out-of-band (copied manually, USB key, etc.) and pinned by the satellite.
   There is no CA.

**Planned:**
- WireGuard as identity fabric and network-level encryption; QUIC on top.
- Mutual TLS authentication (mTLS) for multi-tenant deployments.

## 3. Stream types

Each bidirectional QUIC stream begins with a **1-byte tag** written by the
opener (always the satellite).

| Tag    | Name            | Purpose                       |
|--------|-----------------|-------------------------------|
| `0x00` | CONTROL         | Handshake, heartbeat (future) |
| `0x01` | CHUNK_REQUEST   | Fetch a single chunk by hash  |

Future stream types (not yet implemented):

| Tag    | Name            | Purpose                       |
|--------|-----------------|-------------------------------|
| `0x02` | CATALOG_SYNC    | Merkle tree delta push        |
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
}
```

Current `protocol_version`: **1**.

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
  bytes data = 2;   // raw (decompressed) chunk data
  uint64 size = 3;
}
```

Chunks are stored compressed (zstd) on disk but transmitted **decompressed** in
`ChunkResponse.data`. The satellite compresses again when storing locally.
Transmitting compressed data is a planned future optimization.

## 7. Flow example

```
[Satellite]                           [Hub]
    |                                   |
    |--- QUIC connect ----------------->|
    |<-- QUIC accept -------------------|
    |                                   |
    |--- stream 0: [0x00] Handshake --->|
    |<-- HandshakeAck {ok: true} -------|
    |                                   |
    |--- stream 1: [0x01] ChunkReq --->|
    |<-- ChunkResponse {found, data} ---|
    |                                   |
    |--- stream 2: [0x01] ChunkReq --->|
    |<-- ChunkResponse {found, data} ---|
    |                                   |
    |--- connection close -------------->|
```

Multiple chunk request streams can be opened concurrently (QUIC multiplexing).

## 8. Error handling

- **Unknown stream tag:** the hub closes the stream with an error.
- **Chunk not found:** the hub responds with `ChunkResponse { found: false }`.
- **Hash mismatch:** the satellite verifies the BLAKE3 hash after receiving the
  data and rejects chunks that don't match. The fetch is aborted with an error.
- **Connection drop:** the satellite can reconnect and resume, skipping
  already-fetched chunks (chunk-level resume).

## 9. Security

**Current:**
- TLS 1.3 via rustls with self-signed certificates (rcgen).
- Certificate pinning: satellite stores hub's cert at `library add` time.
- Private keys stored with 0o600 permissions, created atomically (no TOCTOU).
- Library IDs are validated (alphanumeric + hyphens + underscores only) to
  prevent path traversal.
- Fetched chunks are verified against their BLAKE3 hash before storage.
- Certificate fingerprint (BLAKE3 of cert DER) displayed at `hub init`.

**Planned:**
- WireGuard public key as node identity.
- Mutual TLS authentication.
- Certificate expiration and rotation.
- Connection/stream limits and timeouts on hub.
