//! Generates the Rust messages and gRPC stubs from the protocol definitions
//! in `src/proto`.
//!
//! The definitions are compiled by `protox`, a Protocol Buffers compiler
//! written in Rust, so neither Cargo nor Bazel needs a `protoc` binary on
//! the machine (docs/decisions/0002-grpc-transport.md).

use std::env;
use std::error::Error;
use std::path::PathBuf;

/// The protocol files to compile, relative to the protocol root.
const PROTOCOL_FILES: [&str; 1] = ["bitacora/v1/replica.proto"];

fn main() -> Result<(), Box<dyn Error>> {
    let protocol_root = protocol_root()?;
    for protocol_file in PROTOCOL_FILES {
        println!(
            "cargo:rerun-if-changed={}",
            protocol_root.join(protocol_file).display()
        );
    }
    let file_descriptors = protox::compile(PROTOCOL_FILES, [&protocol_root])?;
    tonic_prost_build::configure()
        // Batch frames are handed around as reference-counted buffers, so a
        // received frame is never copied out of the message it arrived in.
        .bytes(".bitacora.v1")
        .compile_fds(file_descriptors)?;
    Ok(())
}

/// Returns the directory that holds the `bitacora/` protocol package:
/// `src/proto`, two levels above this crate, under Cargo and Bazel alike.
fn protocol_root() -> Result<PathBuf, Box<dyn Error>> {
    let crate_directory = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    Ok(crate_directory.join("../../proto"))
}
