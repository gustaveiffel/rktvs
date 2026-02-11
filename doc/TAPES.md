# Tapes

## What is a tape?

A tape is a named logical volume that groups a subtree of files. Think of it
as a project, dataset, or share. Each tape belongs to a library (server) and
has its own:

- **File tree** -- paths are relative to the tape root.
- **ACLs** -- who can read, write, or administer the tape.
- **Sync policy** -- which grade to use, maximum cost tier allowed.
- **Version history** -- every change increments a version counter.

A single library can host many tapes; a single satellite can subscribe to
tapes from multiple libraries. The local catalog merges everything into one
unified SQLite database.

## Cross-tape deduplication

Chunks are shared globally across all tapes and libraries. If two tapes
contain the same 10 GB video, only one copy of the chunks exists in the
store. The catalog tracks which chunks belong to which files, but the
ChunkStore itself is a flat, content-addressed pool keyed by BLAKE3 hash.

This means adding a file to a second tape is essentially free when the
content already exists locally.

## Tape operations (CLI)

### Ingest

Pipe a tar stream into `rk ingest` to add files to a tape:

```bash
tar cf - /data/project-x/ | rk ingest --tape project-x
```

### Browse

The catalog is available offline. No data transfer required:

```bash
rk ls project-x/
rk ls project-x/src/
```

### Export

Stream a tape (or subtree) back out as tar:

```bash
rk tar project-x/ | tar xf - -C /output/
```

### Estimate

Check how much a transfer would cost before fetching anything:

```bash
rk estimate project-x/data/big-model.bin
```

### Future commands (not yet implemented)

```bash
rk tape create <name> --owner <owner>
rk tape acl <name> --grant <node> --perm read
rk tape policy <name> --grade normal --max-cost metered
rk tape versions <name>
```

## Tape metadata in SQLite

The `tapes` table tracks each tape:

| Column              | Purpose                                 |
|---------------------|-----------------------------------------|
| `library_id`        | Library that owns the tape (PK part 1)  |
| `tape_name`         | Unique name within the library (PK part 2) |
| `description`       | Human-readable description              |
| `owner`             | Node that created the tape              |
| `permission`        | Default permission level                |
| `catalog_version`   | Current version counter                 |
| `merkle_root`       | Root hash of the tape's Merkle tree     |
| `last_sync`         | Unix timestamp of last catalog sync     |
| `total_files`       | File count (denormalized for display)   |
| `total_size`        | Total size in bytes (denormalized)      |

Primary key: `(library_id, tape_name)`.

Files within a tape live in the `files` table with primary key
`(library_id, tape, path)`. Each file links to its chunks through
`file_chunks`, and the chunks themselves are stored once in `chunks`
(global) or `local_chunks` (fetched to this node).

## Use cases

- **Maritime** -- A vessel has tape `vessel-logs` syncing daily reports
  over VSAT. Grade is set to BATCH so transfers wait for the cheapest
  window.

- **Field research** -- A remote station has tape `sensor-data` uploading
  readings on satellite passes. The scheduler queues chunks at BACKGROUND
  grade and drains them when the link is FREE.

- **Media production** -- Tape `raw-footage` holds 4K files. The sync
  policy caps cost at CHEAP, so only catalog metadata and low-res previews
  are fetched over metered links. Full files are pulled on LAN.

- **Software distribution** -- Tape `releases` is pushed from HQ and
  pulled by remote offices on schedule. Cross-tape dedup means shared
  dependencies are never transferred twice.

## Versioning (planned)

Each tape has a version counter incremented on every change. The
`tape_versions` table stores snapshots:

| Column         | Purpose                          |
|----------------|----------------------------------|
| `version`      | Monotonic version number         |
| `timestamp`    | When the version was created     |
| `merkle_root`  | Merkle root at that version      |
| `change_count` | Number of file changes in version|

Future CLI:

```bash
rk diff project-x/@v42 @v45
```

This will compare two versions of a tape and list files added, modified, or
deleted between them.
