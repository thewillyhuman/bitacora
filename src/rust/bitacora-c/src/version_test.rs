//! Tests of the two version functions.
//!
//! # What is covered
//!
//! - The interface version the library reports is the constant of the header, and that constant is the
//!   number programs were compiled with.
//! - The release is the version of the workspace, as a C string that is never null.
//!
//! # How the fixtures work
//!
//! `release` reads the text the way a C caller does: through the exported function and the pointer it
//! returns.

use super::*;

/// The release the library reports, read through the C function.
fn release() -> &'static str {
    let release = bitacora_version();
    assert!(!release.is_null(), "the release pointer is never null");
    // SAFETY: the pointer is not null and names static, NUL-terminated text.
    let release = unsafe { CStr::from_ptr(release) };
    release.to_str().expect("a version is ASCII")
}

#[test]
fn interface_version_is_the_constant_of_the_header() {
    assert_eq!(bitacora_abi_version(), BITACORA_ABI_VERSION);
}

#[test]
fn interface_version_is_the_one_programs_were_compiled_with() {
    assert_eq!(BITACORA_ABI_VERSION, 1);
}

#[test]
fn release_is_the_version_of_the_workspace() {
    assert_eq!(release(), env!("CARGO_PKG_VERSION"));
}

#[test]
fn release_is_the_same_text_on_every_call() {
    assert_eq!(bitacora_version(), bitacora_version());
}
