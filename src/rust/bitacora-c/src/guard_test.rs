//! Tests of the boundary every call from C passes through.
//!
//! # What is covered
//!
//! - Success, failure and panic each come out as the right status.
//! - The last error message afterwards belongs to the call that just ended: empty after success, the
//!   failure's message after a failure, what the panic said after a panic.
//! - A panic never leaves the guard, for bodies that return a status and for bodies that return a value.
//!
//! # How the fixtures work
//!
//! Bodies are closures that succeed, fail or panic on purpose. The panics print their message to standard
//! error, as any panic does; that output is expected. The thread's message is read through the C
//! function, as a caller would.

use super::*;
use crate::test_support::last_message;

/// A failure with a status and a message that the tests can recognize.
fn not_found_failure() -> Failure {
    Failure::new(BitacoraStatus::NotFound, "stream \"orders\" does not exist")
}

#[test]
fn body_that_succeeds_returns_ok() {
    let status = run_guarded(|| Ok(()));

    assert_eq!(status, BitacoraStatus::Ok);
}

#[test]
fn body_that_succeeds_leaves_no_message() {
    remember_message("left over from an earlier call");

    run_guarded(|| Ok(()));

    assert_eq!(last_message(), "");
}

#[test]
fn body_that_fails_returns_the_status_of_its_failure() {
    let status = run_guarded(|| Err(not_found_failure()));

    assert_eq!(status, BitacoraStatus::NotFound);
}

#[test]
fn body_that_fails_leaves_the_message_of_its_failure() {
    run_guarded(|| Err(not_found_failure()));

    assert_eq!(last_message(), "stream \"orders\" does not exist");
}

#[test]
fn body_that_panics_returns_internal() {
    let status = run_guarded(|| panic!("an invariant was broken"));

    assert_eq!(status, BitacoraStatus::Internal);
}

#[test]
fn body_that_panics_leaves_what_the_panic_said() {
    run_guarded(|| panic!("an invariant was broken"));

    assert_eq!(last_message(), "internal error in bitacora: an invariant was broken");
}

#[test]
fn panic_with_a_formatted_message_is_reported_with_it() {
    let broken_invariant = 7;

    run_guarded(|| panic!("invariant {broken_invariant} was broken"));

    assert_eq!(last_message(), "internal error in bitacora: invariant 7 was broken");
}

#[test]
fn panic_that_carries_no_text_is_still_reported() {
    run_guarded(|| std::panic::panic_any(42_u32));

    assert_eq!(last_message(), "internal error in bitacora: a panic without a message");
}

#[test]
fn value_of_a_body_that_succeeds_is_returned() {
    let value = run_guarded_value(0, || 5);

    assert_eq!(value, 5);
}

#[test]
fn value_body_that_succeeds_leaves_no_message() {
    remember_message("left over from an earlier call");

    run_guarded_value(0, || 5);

    assert_eq!(last_message(), "");
}

#[test]
fn value_body_that_panics_returns_the_fallback() {
    let value = run_guarded_value(0, || -> i32 { panic!("an invariant was broken") });

    assert_eq!(value, 0);
}

#[test]
fn value_body_that_panics_leaves_what_the_panic_said() {
    run_guarded_value(0, || -> i32 { panic!("an invariant was broken") });

    assert_eq!(last_message(), "internal error in bitacora: an invariant was broken");
}
