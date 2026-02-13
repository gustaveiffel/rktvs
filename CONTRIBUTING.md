# Contributing to rk

Thank you for your interest in contributing to rk.

## Getting Started

```bash
git clone https://github.com/gustaveiffel/rktvs.git
cd rktvs
cargo build
cargo test --workspace
```

Requires **Rust 1.85+** (edition 2024) and a C compiler (for bundled SQLite).

## Development Workflow

1. Open an issue to discuss the change before writing code.
2. Fork the repo, create a feature branch from `dev`.
3. Write tests first when possible. All new functionality should have tests.
4. Run the full test suite: `cargo test --workspace`
5. Run clippy: `cargo clippy --all-targets --all-features`
6. Run formatting: `cargo fmt --all`
7. Submit a pull request against `dev`.

## Code Style

- Follow existing patterns. The codebase is consistent -- match it.
- Every source file must have the SPDX license header:
  ```rust
  // SPDX-License-Identifier: AGPL-3.0-only
  ```
- Use `thiserror` for error types in `rk-core`, `anyhow` elsewhere.
- Use `tracing` for logging, not `println!` (except in the CLI binary for
  user-facing output).
- No `unsafe` without justification and review.

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add chunk prefetching
fix: handle empty tar archives gracefully
docs: update protocol specification
test: add catalog sync integration tests
```

Scope is optional: `feat(rk-core): add batch chunk lookup`

## Architecture

The project is a Cargo workspace:

| Crate | Purpose |
|---|---|
| `rk-core` | ChunkStore, FastCDC chunker, SQLite catalog, Manifest, Indexer, Verifier |
| `rk-tar` | Tar archive ingest and export |
| `rk-transport` | QUIC hub/satellite, TLS, protobuf wire protocol |
| `rk-scheduler` | Grade/CostTier scheduling, estimation, resumable fetch |
| `rk` (root) | CLI binary |

See [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md) for details.

## Tests

Run the full suite:

```bash
cargo test --workspace        # 91 tests
cargo clippy --all-targets    # no warnings
cargo fmt --all -- --check    # formatting
```

Integration tests that start a QUIC hub are in `crates/rk-transport/tests/`
and `crates/rk-scheduler/tests/`.

## License

By contributing, you agree that your contributions will be licensed under the
AGPL-3.0 license.
