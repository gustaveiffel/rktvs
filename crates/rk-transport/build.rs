// SPDX-License-Identifier: AGPL-3.0-only

fn main() {
    prost_build::compile_protos(&["../../proto/rk.proto"], &["../../proto/"])
        .expect("failed to compile protobuf");
}
