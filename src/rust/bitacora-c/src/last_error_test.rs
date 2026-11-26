//! Tests of the per-thread last error message.
//!
//! # What is covered
//!
//! - The message a thread remembers is the one it reads back, as a C string.
//! - Forgetting leaves an empty string, never a null pointer.
//! - A NUL byte inside a message does not cut the text short.
//! - A message belongs to the thread that remembered it.
//!
//! # How the fixtures work
//!
//! `last_message` reads the message the way a C caller does: through the exported function and the pointer
//! it returns. Each test runs on its own thread, so tests do not see each other's messages.

use std::ffi::CStr;

use super::*;

/// The calling thread's last error message, read through the C function.
fn last_message() -> String {
    let message = bitacora_last_error_message();
    assert!(!message.is_null(), "the message pointer is never null");
    // SAFETY: the pointer is not null and names the thread's NUL-terminated message, which nothing
    // replaces before it is copied here.
    let message = unsafe { CStr::from_ptr(message) };
    message.to_str().expect("messages are UTF-8").to_owned()
}

#[test]
fn thread_that_made_no_call_has_an_empty_message() {
    assert_eq!(last_message(), "");
}

#[test]
fn remembered_message_is_read_back() {
    remember_message("stream \"orders\" does not exist");

    assert_eq!(last_message(), "stream \"orders\" does not exist");
}

#[test]
fn later_message_replaces_the_earlier_one() {
    remember_message("first");

    remember_message("second");

    assert_eq!(last_message(), "second");
}

#[test]
fn forgotten_message_is_empty() {
    remember_message("something failed");

    forget_message();

    assert_eq!(last_message(), "");
}

#[test]
fn reading_the_message_does_not_change_it() {
    remember_message("still here");
    let _first_read = last_message();

    assert_eq!(last_message(), "still here");
}

#[test]
fn nul_byte_inside_a_message_does_not_cut_it_short() {
    remember_message("before\0after");

    assert_eq!(last_message(), "before?after");
}

#[test]
fn message_of_one_thread_is_not_seen_by_another() {
    remember_message("only on this thread");

    // A test may start a thread: the point is to look at another thread's message.
    #[allow(clippy::disallowed_methods)]
    let message_on_another_thread = std::thread::spawn(last_message)
        .join()
        .expect("the other thread does not panic");

    assert_eq!(message_on_another_thread, "");
}
