.PHONY: all build test clippy fmt check audit clean

all: check test

build:
	cargo build

test:
	cargo test --workspace

clippy:
	cargo clippy --all-targets --all-features

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

check: fmt-check clippy
	cargo build

audit:
	cargo deny check

clean:
	cargo clean
