//! The message that explains the last call that did not succeed, kept per thread.
//!
//! # Why this file exists
//!
//! A status code says what kind of thing went wrong, which is what a program needs. A person reading a log
//! needs more: which stream, which replica, which limit. C has no way to return both from one call, so the
//! text is kept where the caller can fetch it afterwards, as `errno` and `strerror` do.
//!
//! # What it contains
//!
//! - [`bitacora_last_error_message`]: the C function that returns the text.
//! - `remember_message` and `forget_message`: what the boundary calls around every entry point.
//!
//! # How it works
//!
//! Each thread has one message. An entry point clears it when it starts and sets it when it does not
//! return `BITACORA_STATUS_OK`, so the message always belongs to the most recent call on that thread. The
//! returned pointer is into storage the thread owns; the next call of any function of this library on the
//! same thread may replace it.
//!
//! ```
//! use std::ffi::CStr;
//!
//! use bitacora_c::{BitacoraStatus, bitacora_last_error_message, bitacora_writer_durable_sequence};
//!
//! // A call that fails: there is no writer.
//! let mut durable_sequence = 0_u64;
//! // SAFETY: a null handle is allowed, and the output points to a live `u64`.
//! let status = unsafe { bitacora_writer_durable_sequence(std::ptr::null(), &raw mut durable_sequence) };
//! assert_eq!(status, BitacoraStatus::InvalidArgument);
//!
//! // SAFETY: the returned text is valid until the next call on this thread, and is read at once.
//! let message = unsafe { CStr::from_ptr(bitacora_last_error_message()) };
//! assert_eq!(message.to_str(), Ok("writer is null"));
//! ```
//!
//! # Invariants
//!
//! - The returned pointer is never null and always names a NUL-terminated string, empty if the last call
//!   on the thread succeeded or none was made.
//! - A message never crosses threads.
//!
//! # See also
//!
//! - `guard.rs`: the boundary that calls this for every entry point.

use std::cell::RefCell;
use std::ffi::{CString, c_char};

/// What stands in for a NUL byte inside a message, which a C string cannot hold.
const NUL_REPLACEMENT: u8 = b'?';

thread_local! {
    /// The message of the last call on this thread that did not succeed. Empty after a call that did.
    static LAST_ERROR_MESSAGE: RefCell<CString> = RefCell::new(CString::default());
}

/// Makes `message` the calling thread's last error message.
///
/// A NUL byte inside the message is replaced, so that the text C sees is not cut short.
///
/// # Arguments
///
/// * `message` - What happened, for people.
pub(crate) fn remember_message(message: &str) {
    let mut message_bytes = message.as_bytes().to_vec();
    for message_byte in &mut message_bytes {
        if *message_byte == 0 {
            *message_byte = NUL_REPLACEMENT;
        }
    }
    let c_message = CString::new(message_bytes).expect("every NUL byte was replaced");
    replace_message(c_message);
}

/// Empties the calling thread's last error message, as at the start of every call.
pub(crate) fn forget_message() {
    replace_message(CString::default());
}

/// Puts `message` in the place of the calling thread's last error message.
///
/// A thread that is exiting may have given up its message already. Nothing is replaced then, and nothing
/// fails: these functions run outside the panic guard of the entry points, so they must not panic.
fn replace_message(message: CString) {
    let _thread_is_exiting = LAST_ERROR_MESSAGE
        .try_with(|last_message| *last_message.borrow_mut() = message)
        .is_err();
}

/// Returns the message that explains why the last call on this thread did not return
/// `BITACORA_STATUS_OK`.
///
/// The message is for people and logs: it names the stream, the replica or the limit involved. A program
/// decides what to do from the status alone.
///
/// # Returns
///
/// A NUL-terminated UTF-8 string, never null. It is empty if the last call on this thread succeeded, or
/// if this thread has made no call. The string belongs to the library: do not free it. It stays valid
/// until this thread calls any function of this library again, so copy it if it is needed for longer.
///
/// This function itself does not change the message.
#[unsafe(no_mangle)]
pub extern "C" fn bitacora_last_error_message() -> *const c_char {
    // A thread that is exiting may have given up its message already; it reads the empty one.
    LAST_ERROR_MESSAGE
        .try_with(|last_message| last_message.borrow().as_ptr())
        .unwrap_or(c"".as_ptr())
}

#[cfg(test)]
#[path = "last_error_test.rs"]
mod tests;
