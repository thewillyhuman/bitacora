//! The checked-in C header, held to what the Rust source generates.
//!
//! # What is covered
//!
//! `src/c/include/bitacora.h` is what C programs compile against, and `src/rust/bitacora-c` is what they
//! link. If the two disagreed about one argument, a C program would compile, link and corrupt memory. The
//! header is therefore not written by hand: it is generated from the Rust source by cbindgen, checked in
//! so that C users need no Rust tooling to read it, and this test generates it again and fails when the
//! checked-in copy is not byte for byte what comes out.
//!
//! # How the fixtures work
//!
//! The generator reads the crate's source files and `cbindgen.toml` from the crate directory, and never
//! runs Cargo. Under Bazel the directory and the header come from environment variables that the target
//! sets; under Cargo they are found from the crate's manifest directory.
//!
//! To write the header again after a change, run `hack/update_c_header.sh`. It runs this test with
//! `BITA_UPDATE_C_HEADER` set, which makes the test write the header into the source tree instead of
//! comparing.

// A test of a file reads that file, and the update mode writes it: there is no injected disk to go
// through here.
#![allow(clippy::disallowed_methods)]

use std::path::{Path, PathBuf};

/// The source files that declare something the header must contain, in the order the header lists them.
const INTERFACE_SOURCE_FILES: [&str; 8] = [
    "status.rs",
    "last_error.rs",
    "version.rs",
    "client.rs",
    "writer.rs",
    "records.rs",
    "reader.rs",
    "admin.rs",
];

/// Where the header lives in the repository, from its root.
const HEADER_PATH_IN_REPOSITORY: &str = "src/c/include/bitacora.h";

/// The crate's directory: named by Bazel through the environment, or the manifest directory under Cargo.
fn crate_directory() -> PathBuf {
    match std::env::var_os("BITA_C_CRATE_DIRECTORY") {
        Some(directory) => PathBuf::from(directory),
        None => PathBuf::from(env!("CARGO_MANIFEST_DIR")),
    }
}

/// The checked-in header: named by Bazel through the environment, or found from the crate under Cargo.
fn checked_in_header_path() -> PathBuf {
    match std::env::var_os("BITA_C_HEADER") {
        Some(header_path) => PathBuf::from(header_path),
        None => crate_directory().join("../../c/include/bitacora.h"),
    }
}

/// Generates the header from the crate's source.
fn generated_header(crate_directory: &Path) -> String {
    let config =
        cbindgen::Config::from_file(crate_directory.join("cbindgen.toml")).expect("cbindgen.toml is a valid config");
    let mut builder = cbindgen::Builder::new().with_config(config);
    for source_file in INTERFACE_SOURCE_FILES {
        builder = builder.with_src(crate_directory.join("src").join(source_file));
    }
    let bindings = builder.generate().expect("the interface can be expressed in C");
    let mut header = Vec::new();
    bindings.write(&mut header);
    String::from_utf8(header).expect("a header is text")
}

#[test]
fn checked_in_header_is_what_the_rust_source_generates() {
    let generated = generated_header(&crate_directory());

    // `bazel run` sets the workspace directory, which is where an update has to be written: the test's
    // own copy of the header is read-only and thrown away.
    if std::env::var_os("BITA_UPDATE_C_HEADER").is_some() {
        let workspace_directory = std::env::var_os("BUILD_WORKSPACE_DIRECTORY")
            .expect("updating the header needs `bazel run`, which says where the workspace is");
        let header_path = PathBuf::from(workspace_directory).join(HEADER_PATH_IN_REPOSITORY);
        std::fs::write(&header_path, &generated).expect("the header can be written");
        eprintln!("wrote {}", header_path.display());
        return;
    }

    let checked_in = std::fs::read_to_string(checked_in_header_path()).expect("the header is checked in");
    assert!(
        checked_in == generated,
        "{HEADER_PATH_IN_REPOSITORY} is not what the Rust source generates; run hack/update_c_header.sh.\n\
         --- generated ---\n{generated}"
    );
}
