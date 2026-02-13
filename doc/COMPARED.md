# Compared to Alternatives

A fair comparison of rk against existing tools for large-file transfer and
synchronization. We try to be honest about trade-offs here -- rk is not the
right tool for every situation, and several of these projects are mature,
battle-tested software.

## Quick comparison

| Feature                          | rk         | Resilio Connect | git-annex  | Aspera (IBM) | Syncthing  | rclone     | casync/desync |
|----------------------------------|------------|-----------------|------------|--------------|------------|------------|---------------|
| Content-defined chunking         | Yes        | No              | No         | No           | Fixed-size | No         | Yes           |
| Block-level dedup                | Yes        | No              | Whole-file | No           | No         | No         | Yes           |
| Chunk-level resume               | Yes        | File-level      | File-level | File-level   | File-level | File-level | N/A (batch)   |
| Catalog (browse offline)         | Yes        | No              | Yes        | No           | No         | No         | No            |
| Cost-aware scheduling            | Yes        | No              | No         | No           | No         | No         | No            |
| Transfer estimation              | Yes        | No              | Partial    | No           | No         | No         | No            |
| On-demand fetch (not full sync)  | Yes        | No (mirrors)    | Yes        | N/A          | No (syncs) | N/A (copy) | N/A (batch)   |
| Tar/Unix composability           | Yes        | No              | No         | No           | No         | Partial    | Yes           |
| Connection migration (QUIC)      | Yes        | No              | No         | No           | No         | No         | No            |
| Open source                      | Yes (AGPL) | No              | Yes (GPL)  | No           | Yes (MPL)  | Yes (MIT)  | Yes (LGPL)    |
| Self-hosted                      | Yes        | Yes             | Yes        | On-prem avail| Yes        | Yes        | Yes           |

A checkmark in this table does not mean the feature is production-ready in rk.
See "Honest limitations" at the bottom.

## Detailed notes

### Resilio Connect

Resilio is the closest tool operationally -- it is a real-time sync engine with
selective sync, agent-based deployment, and WAN optimization. If you need to
keep file trees mirrored across many nodes today, Resilio works.

Where rk differs:

- **Mirroring vs. on-demand.** Resilio mirrors entire folders (or selective
  subsets chosen upfront). rk never mirrors anything. It exposes a catalog you
  can browse offline and fetches individual files on demand, chunk by chunk.
- **No cost-awareness.** Resilio has no concept of link cost. It syncs whenever
  the connection is up. On a satellite link billed per megabyte, this is
  expensive.
- **No transfer estimation.** You cannot ask Resilio "how much will syncing this
  folder cost me?" before it starts transferring.
- **Proprietary and expensive.** Per-agent licensing. No source code.
- **No content-defined chunking.** Resilio does not deduplicate at the block
  level across files.

### git-annex

git-annex is the closest tool conceptually. It separates metadata (tracked by
Git) from content (lazy-fetched with `git annex get`, dropped with `git annex
drop`). The catalog-plus-selective-fetch model is very similar to what rk does.

Where rk differs:

- **Scale.** git-annex struggles beyond roughly 100,000 files. The Git index
  and annex metadata become unwieldy. rk uses a SQLite catalog designed for
  millions of entries.
- **Chunking.** git-annex does whole-file dedup only. If two 10 GB files share
  90% of their content, git-annex stores both in full. rk uses content-defined
  chunking (FastCDC) and deduplicates at the block level.
- **Resume granularity.** git-annex resumes at the file level. If a 500 MB
  transfer drops at 490 MB, it restarts. rk resumes at the chunk level (~4 MB
  granularity), so that same transfer would resume from the last incomplete
  chunk.
- **Cost-awareness.** git-annex has no concept of link cost tiers or scheduling
  grades.
- **UX.** git-annex is powerful but has a steep learning curve. The number of
  subcommands, backends, and modes can be overwhelming.

Credit where due: git-annex pioneered the catalog + selective fetch model, and
rk's design owes a debt to it.

### Aspera (IBM)

Aspera is a high-speed WAN transfer tool built around the fasp protocol. It is
fast -- genuinely faster than TCP-based tools over high-latency, lossy links.

Where rk differs:

- **No catalog.** Aspera is a transfer accelerator, not a file management
  system. There is no way to browse remote files offline or estimate transfer
  cost without initiating a transfer.
- **No dedup.** Every transfer ships every byte, even if the destination already
  has most of the data.
- **No chunk-level resume.** Aspera resumes at the file level.
- **No cost-awareness.** No scheduling based on link type.
- **Very expensive.** Aspera licensing is enterprise-priced. Not realistic for
  small teams or individual use.

Aspera is the right choice if raw WAN throughput is your only concern and you
have the budget for it.

### Syncthing

Syncthing is an excellent open-source tool for continuous file synchronization
between a small number of devices. It is well-designed, reliable, and has a
good user experience.

Where rk differs:

- **Always-on sync vs. on-demand fetch.** Syncthing is designed for
  always-connected devices that mirror folders continuously. rk is designed for
  hostile, intermittent links where you fetch specific files when conditions
  allow.
- **No cost-awareness.** Syncthing syncs whenever a connection is available. On
  a metered satellite link, this would drain your data budget.
- **Hostile links.** Syncthing assumes reasonable LAN or WAN connectivity. It
  does not handle half-duplex links, extreme latency, or intermittent
  connectivity as gracefully as a store-and-forward model.
- **No catalog.** You cannot browse what files exist on a remote Syncthing node
  without syncing the data.

If you are syncing between a few always-on machines on a LAN or decent
internet, Syncthing is simpler and more mature than rk. Use it.

### rclone

rclone is a Swiss-army knife for cloud storage. It supports dozens of backends
(S3, Google Drive, SFTP, etc.) and is an indispensable tool for moving data
between cloud providers.

Where rk differs:

- **Copy tool, not a sync engine.** rclone copies files. It does not chunk them,
  deduplicate, or maintain a local catalog of remote content.
- **No block-level resume.** rclone resumes at the file level (where the
  backend supports it).
- **No scheduling.** rclone runs when you tell it to. There is no concept of
  job queues, grades, or cost-aware scheduling.
- **No catalog.** rclone lists remote files on demand (requiring a connection).
  There is no offline browsing.

rclone and rk solve different problems. rclone moves files between storage
backends. rk delivers files over hostile networks with cost awareness.

### casync/desync

casync (and its Go reimplementation desync) are the closest inspiration for
rk's chunk store format. They use content-defined chunking with
content-addressed storage, and casync's `.castr` / `.caidx` formats are
well-designed.

Where rk differs:

- **Batch tools, not client-server.** casync and desync are designed for
  creating and applying filesystem images. They do not maintain persistent
  connections, handle connection migration, or schedule transfers.
- **No catalog sync.** There is no mechanism to synchronize a file tree catalog
  between nodes. You need the index file upfront.
- **No transport layer.** casync expects you to provide your own transport
  (HTTP, SFTP, etc.). rk includes a QUIC-based transport with connection
  migration and multiplexed streams.
- **No scheduling or cost-awareness.** Batch tools run to completion.

casync/desync are good building blocks. rk layers scheduling, transport, and
catalog management on top of a similar chunking philosophy.

## What makes rk different

No single feature in rk is unique. Content-defined chunking exists in casync.
Catalog + selective fetch exists in git-annex. QUIC transport exists in many
modern tools. Cost-aware scheduling exists in UUCP (from 1978).

The combination is what is new: **on-demand fetch + cost-aware scheduling +
pre-transfer estimation + tar interface + chunk-level resume**, all designed for
hostile network links (satellite, radio, hotel WiFi, maritime VSAT).

The tar interface deserves special mention. `rk tar <tape>/<path> | tar xf -`
means rk composes with existing Unix tools. No FUSE mount, no proprietary
client, no GUI required.

## Honest limitations of rk

This is a young project. Be aware of what it cannot do today:

- **Alpha stage.** Core functionality works and is tested (91 tests), but this
  is not production-hardened software. Expect rough edges.
- **No GUI.** CLI only. If you need a graphical interface, look elsewhere for
  now.
- **No mobile client.** There is no iOS or Android app. This is a server and
  workstation tool.
- **No P2P between satellites.** Satellites fetch from a hub (library). There
  is no peer-to-peer mesh between satellite nodes.
- **Single-threaded chunk fetching.** The fetcher currently processes one chunk
  at a time. Parallel chunk fetching is planned but not implemented.
- **TLS without mutual auth.** The hub accepts any client that has the public
  cert. There is no client authentication yet (mTLS or auth tokens).

If any of these are blockers for your use case, rk is probably not ready for
you yet. Check back later, or contribute.
