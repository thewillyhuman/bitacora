//! The boundary every call from C passes through.
//!
//! # Why this file exists
//!
//! Three things must hold for every function C can call, and none of them may depend on each function
//! remembering to do it. A panic must never travel into C, where unwinding is undefined behaviour. The
//! thread's last error message must describe the call that just ended and no earlier one. And a failure
//! inside must come out as a status code. This file does all three in one place.
//!
//! # What it contains
//!
//! - `run_guarded`: runs the body of an entry point that returns a status.
//! - `run_guarded_value`: the same for the few entry points that return something else.
//!
//! # How it works
//!
//! An entry point is a thin `extern "C"` function whose whole body is a call to one of these two with a
//! closure. The closure does the work and returns `Result<(), Failure>`. The guard clears the last error
//! message, runs the closure inside `catch_unwind`, and turns what comes back into the status: `Ok` for
//! success, the failure's own status with its message remembered, or `Internal` for a panic with whatever
//! the panic said.
//!
//! A panic is a defect in bitacora. It is caught so that the caller's process survives and can report it,
//! not because it is expected.
//!
//! # Invariants
//!
//! - No unwinding leaves this crate: every `extern "C"` function with a body that can panic goes through
//!   one of these two functions.
//! - After any guarded call, the thread's last error message is empty if and only if the call returned
//!   `BITACORA_STATUS_OK`.
//!
//! # See also
//!
//! - `status.rs`: [`Failure`] and the status codes.
//! - `last_error.rs`: where the message is kept.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::last_error::{forget_message, remember_message};
use crate::status::{BitacoraStatus, Failure};

/// Runs the body of an entry point and turns its outcome into the status the entry point returns.
///
/// See the file header for what the guard does around the body.
///
/// # Arguments
///
/// * `entry_point_body` - The work of the entry point. It is run once, on the calling thread.
///
/// # Returns
///
/// [`BitacoraStatus::Ok`] if the body succeeded, the status of its [`Failure`] if it failed, and
/// [`BitacoraStatus::Internal`] if it panicked.
pub(crate) fn run_guarded(entry_point_body: impl FnOnce() -> Result<(), Failure>) -> BitacoraStatus {
    forget_message();
    // The closure borrows the entry point's arguments. If it panics, nothing of what it borrowed is used
    // again: the entry point returns at once with a status.
    match catch_unwind(AssertUnwindSafe(entry_point_body)) {
        Ok(Ok(())) => BitacoraStatus::Ok,
        Ok(Err(failure)) => {
            remember_message(&failure.message);
            failure.status
        }
        Err(panic_payload) => {
            remember_message(&describe_panic(panic_payload.as_ref()));
            BitacoraStatus::Internal
        }
    }
}

/// Runs the body of an entry point that returns a value instead of a status.
///
/// # Arguments
///
/// * `value_after_a_panic` - What the entry point returns if the body panics.
/// * `entry_point_body` - The work of the entry point. It is run once, on the calling thread.
///
/// # Returns
///
/// What the body returned, or `value_after_a_panic`. The thread's last error message is empty afterwards
/// unless the body panicked.
pub(crate) fn run_guarded_value<Value>(value_after_a_panic: Value, entry_point_body: impl FnOnce() -> Value) -> Value {
    forget_message();
    match catch_unwind(AssertUnwindSafe(entry_point_body)) {
        Ok(value) => value,
        Err(panic_payload) => {
            remember_message(&describe_panic(panic_payload.as_ref()));
            value_after_a_panic
        }
    }
}

/// Says what a caught panic was about, for the last error message.
///
/// A panic carries a `&str` or a `String` when it comes from `panic!`, `expect` or an assertion, which
/// covers everything this workspace does. Anything else is reported without its content.
fn describe_panic(panic_payload: &(dyn Any + Send)) -> String {
    let what_the_panic_said = if let Some(text) = panic_payload.downcast_ref::<&str>() {
        text
    } else if let Some(text) = panic_payload.downcast_ref::<String>() {
        text.as_str()
    } else {
        "a panic without a message"
    };
    format!("internal error in bitacora: {what_the_panic_said}")
}

#[cfg(test)]
#[path = "guard_test.rs"]
mod tests;
