//! Which library this is: its version and the version of its interface.
//!
//! # Why this file exists
//!
//! A program that loads the shared library at run time, or that is built against one copy of the header
//! and linked against another, needs to find out what it actually got before it calls anything else.
//! Two numbers answer that: the release, for people and logs, and the interface version, for the program.
//!
//! # What it contains
//!
//! - [`bitacora_version`]: the release, as text.
//! - [`bitacora_abi_version`]: the version of the C interface, as a number.
//! - [`BITACORA_ABI_VERSION`]: the same number as a constant of the header, to compare against.
//!
//! # How it works
//!
//! The release is the version of the workspace, the same one the daemons print. The interface version is
//! a counter kept by hand in this file. It changes only when a program compiled against the old header
//! could misbehave with the new library: a function removed, an argument or a structure field changed in
//! meaning. Adding functions, status codes at the end, or fields at the end of a sized structure does not
//! change it.
//!
//! ```
//! use std::ffi::CStr;
//!
//! use bitacora_c::{BITACORA_ABI_VERSION, bitacora_abi_version, bitacora_version};
//!
//! assert_eq!(bitacora_abi_version(), BITACORA_ABI_VERSION);
//! // SAFETY: the returned text is static and NUL-terminated.
//! let release = unsafe { CStr::from_ptr(bitacora_version()) };
//! assert_eq!(release.to_str(), Ok(env!("CARGO_PKG_VERSION")));
//! ```
//!
//! # Invariants
//!
//! - Both functions can be called at any time, from any thread, and never fail.
//!
//! # See also
//!
//! - `docs/clients/c.md`: what counts as a compatible change.

use std::ffi::{CStr, c_char};

/// The version of the C interface this header describes. Compare it with what `bitacora_abi_version`
/// returns to make sure the library that was loaded is the one the program was compiled for.
pub const BITACORA_ABI_VERSION: u32 = 1;

/// The release, NUL-terminated, built into the library.
const RELEASE: &CStr = match CStr::from_bytes_with_nul(concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes()) {
    Ok(release) => release,
    Err(_) => panic!("a version has no NUL byte in it"),
};

/// Returns the release of the library, for example `1.4.0`.
///
/// # Returns
///
/// A NUL-terminated string that is never null and stays valid for as long as the library is loaded. Do
/// not free it.
#[unsafe(no_mangle)]
pub extern "C" fn bitacora_version() -> *const c_char {
    RELEASE.as_ptr()
}

/// Returns the version of the C interface the library implements.
///
/// # Returns
///
/// The number that `BITACORA_ABI_VERSION` had in the header the library was built from. A program
/// compiled against a header with the same number can use this library.
#[unsafe(no_mangle)]
pub extern "C" fn bitacora_abi_version() -> u32 {
    BITACORA_ABI_VERSION
}

#[cfg(test)]
#[path = "version_test.rs"]
mod tests;
