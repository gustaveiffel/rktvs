fn main() {
    prost_build::compile_protos(&["../../proto/rk.proto"], &["../../proto/"])
        .expect("failed to compile protobuf");
}
