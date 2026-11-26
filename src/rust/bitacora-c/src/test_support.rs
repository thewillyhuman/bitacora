//! What the unit tests of this crate share.
//!
//! # Why this file exists
//!
//! Most entry points need a client before they can be called at all, and every test of a failure wants
//! to read the last error message the way C does. Both would otherwise be copied into each test file.
//!
//! # What it contains
//!
//! - [`TestClient`]: a client of a cluster that is not there, freed when the test ends.
//! - [`last_message`]: the calling thread's last error message, read through the C function.
//! - [`ABSENT_CLUSTER_CONFIGURATION`]: the configuration such a client is connected with.
//!
//! # How it works
//!
//! Connecting sends nothing, so a client can be made for an etcd endpoint nobody listens on. Everything
//! that is decided before the first request works against it: argument checks, opening readers, closed
//! handles. What needs a cluster is tested by `tests/c_programs_test.rs` against real processes.
//!
//! # Invariants
//!
//! - Nothing here reaches the network: tests that use it stay fast and need no ports.
//!
//! # See also
//!
//! - `tests/c_programs_test.rs`: the tests that do talk to a cluster.

use std::ffi::CStr;
use std::sync::Arc;

use crate::client::{ClientCore, ClientHandle, bitacora_client_connect_text, bitacora_client_free};
use crate::last_error::bitacora_last_error_message;
use crate::status::BitacoraStatus;

/// A cluster configuration that names an etcd endpoint nobody listens on.
pub(crate) const ABSENT_CLUSTER_CONFIGURATION: &CStr = c"etcd_endpoints = [\"http://127.0.0.1:1\"]";

/// A client of a cluster that is not there. It is freed when the value is dropped.
pub(crate) struct TestClient {
    /// The handle, as C would hold it.
    handle: *mut ClientHandle,
}

impl TestClient {
    /// Connects a client with [`ABSENT_CLUSTER_CONFIGURATION`] and every default.
    ///
    /// # Panics
    ///
    /// Panics if connecting does not succeed, which would be a defect: connecting sends nothing.
    pub(crate) fn connect() -> Self {
        let mut handle = std::ptr::null_mut();
        // SAFETY: the configuration is NUL-terminated, null options mean the defaults, and the output is a
        // live pointer.
        let status = unsafe {
            bitacora_client_connect_text(ABSENT_CLUSTER_CONFIGURATION.as_ptr(), std::ptr::null(), &raw mut handle)
        };
        assert_eq!(status, BitacoraStatus::Ok, "{}", last_message());
        Self { handle }
    }

    /// The client, as the pointer an entry point takes.
    pub(crate) const fn handle(&self) -> *const ClientHandle {
        self.handle
    }

    /// A share of the client and its threads, as a handle opened from this client would hold it.
    pub(crate) fn core(&self) -> Arc<ClientCore> {
        // SAFETY: the handle was returned by the library and is freed only when this value is dropped.
        unsafe { &*self.handle }.core()
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        // SAFETY: the handle was returned by the library, is freed here once, and is not used afterwards.
        unsafe { bitacora_client_free(self.handle) };
    }
}

/// The calling thread's last error message, read through the C function.
pub(crate) fn last_message() -> String {
    // SAFETY: the function returns the thread's NUL-terminated message, never null, and nothing replaces
    // it before it is copied here.
    let message = unsafe { CStr::from_ptr(bitacora_last_error_message()) };
    message.to_str().expect("messages are UTF-8").to_owned()
}
