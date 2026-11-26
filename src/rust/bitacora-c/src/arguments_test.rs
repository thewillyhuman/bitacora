//! Tests of checking what C passes in and writing what it gets back.
//!
//! # What is covered
//!
//! - Every way a pointer becomes something safe: a null pointer is refused by name where one is required
//!   and accepted where it means "left out"; text that is not UTF-8 is refused.
//! - Output slots write what they are given and nothing else.
//! - Handles survive the trip to C and back.
//! - Sized structures: a shorter one from an older program gets defaults for what it lacks, a longer one
//!   from a newer program is cut to what this library knows, and one whose size was not set is refused.
//!
//! # How the fixtures work
//!
//! `ExampleOptions` is a sized structure made for these tests, with a field that an "older" caller does
//! not have. Byte buffers stand in for the memory of callers compiled against other versions of it.

use std::ffi::c_char;

use super::*;
use crate::status::BitacoraStatus;

/// A sized structure as some version of the interface might define it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExampleOptions {
    /// The size the caller declares.
    struct_size: u32,
    /// A field every version has.
    first_setting: u32,
    /// A field that was added later: an older caller's structure ends before it.
    later_setting: u64,
}

// SAFETY: `ExampleOptions` is `#[repr(C)]`, begins with the `u32` size, consists only of integers, and
// its fields fill it without padding: 4 + 4 + 8 bytes.
unsafe impl SizedStructure for ExampleOptions {
    fn with_defaults() -> Self {
        Self {
            struct_size: 0,
            first_setting: 0,
            later_setting: 0,
        }
    }
}

/// The size of the whole example structure, as its `struct_size` field holds it.
const FULL_SIZE: u32 = 16;

/// The size of the example structure as an older caller knows it: without `later_setting`.
const OLDER_SIZE: u32 = 8;

/// The size of the example structure as a newer caller knows it: with one more eight-byte field.
const NEWER_SIZE: u32 = 24;

/// What a call writes back in the tests of writing: a value in every field.
fn filled_result() -> ExampleOptions {
    ExampleOptions {
        struct_size: 0,
        first_setting: 3,
        later_setting: 9,
    }
}

/// The byte that fills a caller's memory where the test wrote nothing, to see what a call touched.
const UNTOUCHED: u8 = 0xAA;

/// Memory of a caller compiled against another version of the structure: 24 bytes, aligned like the
/// structure, that begin with the size the caller declares.
#[repr(C, align(8))]
struct CallersMemory {
    /// The bytes, in the layout of `ExampleOptions` followed by one more eight-byte field.
    bytes: [u8; 24],
}

impl CallersMemory {
    /// Memory that declares `declared_size` and holds `first_setting`; everything else is untouched.
    fn declaring(declared_size: u32, first_setting: u32) -> Self {
        let mut bytes = [UNTOUCHED; 24];
        bytes[..4].copy_from_slice(&declared_size.to_ne_bytes());
        bytes[4..8].copy_from_slice(&first_setting.to_ne_bytes());
        Self { bytes }
    }

    /// The memory as the pointer to a structure that C would pass.
    fn as_structure(&self) -> *const ExampleOptions {
        (&raw const *self).cast::<ExampleOptions>()
    }

    /// The memory as the pointer to a structure that C wants filled.
    fn as_structure_to_fill(&mut self) -> *mut ExampleOptions {
        (&raw mut *self).cast::<ExampleOptions>()
    }
}

/// The status and message of the failure a check returned.
fn refusal<Value>(checked: Result<Value, Failure>) -> (BitacoraStatus, String) {
    match checked {
        Ok(_) => panic!("the argument was accepted"),
        Err(failure) => (failure.status, failure.message),
    }
}

#[test]
fn null_handle_is_refused_by_name() {
    // SAFETY: a null pointer is allowed.
    let checked = unsafe { handle_reference(std::ptr::null::<u32>(), "writer") };

    assert_eq!(
        refusal(checked),
        (BitacoraStatus::InvalidArgument, "writer is null".to_owned())
    );
}

#[test]
fn handle_that_is_not_null_is_borrowed() {
    let handle = 7_u32;

    // SAFETY: the pointer names a live value for the whole call.
    let borrowed = unsafe { handle_reference(&raw const handle, "writer") };

    assert_eq!(borrowed.ok(), Some(&7));
}

#[test]
fn null_text_is_refused_by_name() {
    // SAFETY: a null pointer is allowed.
    let checked = unsafe { text_argument(std::ptr::null(), "stream_name") };

    assert_eq!(
        refusal(checked),
        (BitacoraStatus::InvalidArgument, "stream_name is null".to_owned())
    );
}

#[test]
fn text_is_read_up_to_its_nul() {
    let c_text = c"orders";

    // SAFETY: the pointer names a NUL-terminated string that outlives the call.
    let text = unsafe { text_argument(c_text.as_ptr(), "stream_name") };

    assert_eq!(text.ok(), Some("orders"));
}

#[test]
fn text_that_is_not_utf8_is_refused() {
    let not_utf8: [u8; 3] = [0xFF, 0xFE, 0x00];

    // SAFETY: the bytes end with a NUL and outlive the call.
    let checked = unsafe { text_argument(not_utf8.as_ptr().cast::<c_char>(), "stream_name") };

    assert_eq!(refusal(checked).0, BitacoraStatus::InvalidArgument);
}

#[test]
fn optional_text_that_is_null_was_left_out() {
    // SAFETY: a null pointer is allowed.
    let text = unsafe { optional_text_argument(std::ptr::null(), "writer_identity") };

    assert_eq!(text.ok(), Some(None));
}

#[test]
fn optional_text_that_is_given_is_read() {
    let c_text = c"compute-1";

    // SAFETY: the pointer names a NUL-terminated string that outlives the call.
    let text = unsafe { optional_text_argument(c_text.as_ptr(), "writer_identity") };

    assert_eq!(text.ok(), Some(Some("compute-1")));
}

#[test]
fn bytes_are_borrowed_with_their_length() {
    let payload = b"redo record";

    // SAFETY: the pointer names `payload.len()` readable bytes that outlive the call.
    let bytes = unsafe { bytes_argument(payload.as_ptr(), payload.len(), "payload") };

    assert_eq!(bytes.ok(), Some(&payload[..]));
}

#[test]
fn no_bytes_need_no_pointer() {
    // SAFETY: with a length of 0 the pointer is not used.
    let bytes = unsafe { bytes_argument(std::ptr::null(), 0, "payload") };

    assert_eq!(bytes.ok(), Some(&[][..]));
}

#[test]
fn null_bytes_with_a_length_are_refused() {
    // SAFETY: a null pointer is allowed; it is refused before it is used.
    let checked = unsafe { bytes_argument(std::ptr::null(), 5, "payload") };

    assert_eq!(
        refusal(checked),
        (
            BitacoraStatus::InvalidArgument,
            "payload is null but its length is 5".to_owned()
        )
    );
}

#[test]
fn null_output_is_refused_by_name() {
    // SAFETY: a null pointer is allowed.
    let checked = unsafe { output_slot(std::ptr::null_mut::<u64>(), "commit_sequence_out") };

    assert_eq!(
        refusal(checked),
        (
            BitacoraStatus::InvalidArgument,
            "commit_sequence_out is null".to_owned()
        )
    );
}

#[test]
fn output_slot_writes_to_the_callers_memory() {
    let mut commit_sequence = 0_u64;
    // SAFETY: the pointer names a live `u64` for the whole call.
    let slot = unsafe { output_slot(&raw mut commit_sequence, "commit_sequence_out") }.expect("not null");

    slot.write(42);

    assert_eq!(commit_sequence, 42);
}

#[test]
fn output_slot_can_be_written_twice() {
    let mut commit_sequence = 0_u64;
    // SAFETY: the pointer names a live `u64` for the whole call.
    let slot = unsafe { output_slot(&raw mut commit_sequence, "commit_sequence_out") }.expect("not null");
    slot.write(1);

    slot.write(2);

    assert_eq!(commit_sequence, 2);
}

#[test]
fn optional_output_that_is_null_was_left_out() {
    // SAFETY: a null pointer is allowed.
    let slot = unsafe { optional_output_slot(std::ptr::null_mut::<u64>()) };

    assert!(slot.is_none());
}

#[test]
fn handle_given_to_c_is_the_one_taken_back() {
    let handle = into_handle_pointer(String::from("a handle"));

    // SAFETY: the pointer was just made by `into_handle_pointer` and is not freed yet.
    let content = unsafe { handle_reference(handle.cast_const(), "handle") }.cloned();
    // SAFETY: the pointer came from `into_handle_pointer`, is freed once, and is not used afterwards.
    unsafe { free_handle(handle) };

    assert_eq!(content.ok(), Some("a handle".to_owned()));
}

#[test]
fn freeing_a_null_handle_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { free_handle(std::ptr::null_mut::<String>()) };
}

#[test]
fn null_structure_means_every_default() {
    // SAFETY: a null pointer is allowed.
    let options = unsafe { read_sized_structure(std::ptr::null::<ExampleOptions>(), "options") };

    assert_eq!(options.ok(), Some(ExampleOptions::with_defaults()));
}

#[test]
fn structure_of_this_version_is_read_whole() {
    let passed = ExampleOptions {
        struct_size: FULL_SIZE,
        first_setting: 3,
        later_setting: 9,
    };

    // SAFETY: the pointer names a live structure of the size it declares.
    let options = unsafe { read_sized_structure(&raw const passed, "options") };

    assert_eq!(options.ok(), Some(passed));
}

#[test]
fn shorter_structure_of_an_older_caller_gets_defaults_for_what_it_lacks() {
    // An older program's structure: eight bytes, then memory that is not part of it.
    let callers_memory = CallersMemory::declaring(OLDER_SIZE, 3);

    // SAFETY: the memory holds the eight bytes its first four bytes declare.
    let options = unsafe { read_sized_structure(callers_memory.as_structure(), "options") }.expect("the size is set");

    assert_eq!(options.first_setting, 3);
    assert_eq!(options.later_setting, 0);
}

#[test]
fn longer_structure_of_a_newer_caller_is_cut_to_what_is_known() {
    // A newer program's structure: the sixteen bytes this version knows, then a field it does not.
    let mut callers_memory = CallersMemory::declaring(NEWER_SIZE, 3);
    callers_memory.bytes[8..16].copy_from_slice(&9_u64.to_ne_bytes());

    // SAFETY: the memory holds the 24 bytes its first four bytes declare.
    let options = unsafe { read_sized_structure(callers_memory.as_structure(), "options") }.expect("the size is set");

    assert_eq!(options.first_setting, 3);
    assert_eq!(options.later_setting, 9);
}

#[test]
fn structure_whose_size_was_not_set_is_refused() {
    let passed = ExampleOptions {
        struct_size: 0,
        first_setting: 3,
        later_setting: 9,
    };

    // SAFETY: the pointer names a live structure of at least four bytes.
    let checked = unsafe { read_sized_structure(&raw const passed, "options") };

    assert_eq!(
        refusal(checked),
        (
            BitacoraStatus::InvalidArgument,
            "options.struct_size is 0: set it to the size of the structure".to_owned()
        )
    );
}

#[test]
fn structure_is_written_whole_to_a_caller_of_this_version() {
    let mut callers_structure = ExampleOptions {
        struct_size: FULL_SIZE,
        first_setting: 0,
        later_setting: 0,
    };
    // SAFETY: the pointer names a live structure of the size it declares.
    let slot = unsafe { sized_output_slot(&raw mut callers_structure, "description_out") }.expect("the size is set");

    slot.write(filled_result());

    assert_eq!(
        callers_structure,
        ExampleOptions {
            struct_size: FULL_SIZE,
            first_setting: 3,
            later_setting: 9,
        }
    );
}

#[test]
fn accepting_a_structure_to_fill_writes_nothing_yet() {
    let mut callers_memory = CallersMemory::declaring(NEWER_SIZE, 0);

    // SAFETY: the memory holds the 24 bytes its first four bytes declare.
    let _slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    assert_eq!(callers_memory.bytes[..4], NEWER_SIZE.to_ne_bytes());
    assert_eq!(callers_memory.bytes[8..], [UNTOUCHED; 16]);
}

#[test]
fn writing_to_an_older_caller_leaves_the_memory_after_its_structure_alone() {
    let mut callers_memory = CallersMemory::declaring(OLDER_SIZE, 0);
    // SAFETY: the memory holds the eight bytes its first four bytes declare.
    let slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    slot.write(filled_result());

    assert_eq!(callers_memory.bytes[8..], [UNTOUCHED; 16]);
}

#[test]
fn writing_to_an_older_caller_fills_the_fields_it_has() {
    let mut callers_memory = CallersMemory::declaring(OLDER_SIZE, 0);
    // SAFETY: the memory holds the eight bytes its first four bytes declare.
    let slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    slot.write(filled_result());

    assert_eq!(callers_memory.bytes[4..8], 3_u32.to_ne_bytes());
}

#[test]
fn writing_to_an_older_caller_keeps_the_size_it_declared() {
    let mut callers_memory = CallersMemory::declaring(OLDER_SIZE, 0);
    // SAFETY: the memory holds the eight bytes its first four bytes declare.
    let slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    slot.write(filled_result());

    assert_eq!(callers_memory.bytes[..4], OLDER_SIZE.to_ne_bytes());
}

#[test]
fn writing_to_a_newer_caller_says_how_much_was_filled() {
    let mut callers_memory = CallersMemory::declaring(NEWER_SIZE, 0);
    // SAFETY: the memory holds the 24 bytes its first four bytes declare.
    let slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    slot.write(filled_result());

    assert_eq!(callers_memory.bytes[..4], FULL_SIZE.to_ne_bytes());
}

#[test]
fn writing_to_a_newer_caller_leaves_the_field_it_does_not_know_alone() {
    let mut callers_memory = CallersMemory::declaring(NEWER_SIZE, 0);
    // SAFETY: the memory holds the 24 bytes its first four bytes declare.
    let slot = unsafe { sized_output_slot(callers_memory.as_structure_to_fill(), "description_out") }
        .expect("the size is set");

    slot.write(filled_result());

    assert_eq!(callers_memory.bytes[16..], [UNTOUCHED; 8]);
}

#[test]
fn null_structure_to_fill_is_refused_by_name() {
    // SAFETY: a null pointer is allowed.
    let checked = unsafe { sized_output_slot(std::ptr::null_mut::<ExampleOptions>(), "description_out") };

    assert_eq!(
        refusal(checked),
        (BitacoraStatus::InvalidArgument, "description_out is null".to_owned())
    );
}

#[test]
fn structure_to_fill_whose_size_was_not_set_is_refused() {
    let mut callers_structure = ExampleOptions::with_defaults();

    // SAFETY: the pointer names a live structure of at least four bytes.
    let checked = unsafe { sized_output_slot(&raw mut callers_structure, "description_out") };

    assert_eq!(
        refusal(checked),
        (
            BitacoraStatus::InvalidArgument,
            "description_out.struct_size is 0: set it to the size of the structure".to_owned()
        )
    );
}
