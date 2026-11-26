//! Tests of the reader and follower handles that need no cluster.
//!
//! # What is covered
//!
//! - Every argument check of every reader and follower function: null pointers, a name that is not
//!   UTF-8, a range that is not valid.
//! - What the caller is left with after a failure: null handles, and a low-water mark that is written
//!   only for a truncation.
//! - Opening and freeing a reader, which contact nobody.
//!
//! Reading, following, waiting for nothing and truncation are covered by `tests/c_programs_test.rs`,
//! which runs a C program against a cluster of real processes.
//!
//! # How the fixtures work
//!
//! `TestReader` opens a reader of a stream on a cluster that is not there. Opening sends nothing, and a
//! range that is not valid is refused before anything is sent, so those checks are reached without a
//! network.

use bitacora::ReadError;

use super::*;
use crate::test_support::{TestClient, last_message};

/// A reader of the stream `orders` on a cluster that is not there. It is freed when the value is dropped.
struct TestReader {
    /// The handle, as C would hold it.
    handle: *mut ReaderHandle,
    /// The client the reader was opened from. The reader keeps its own share; this one is freed last.
    _client: TestClient,
}

impl TestReader {
    /// Opens the reader.
    fn open() -> Self {
        let client = TestClient::connect();
        let mut handle = std::ptr::null_mut();
        // SAFETY: the client is live, the name is NUL-terminated, and the output is a live pointer.
        let status = unsafe { bitacora_reader_open(client.handle(), c"orders".as_ptr(), &raw mut handle) };
        assert_eq!(status, BitacoraStatus::Ok, "{}", last_message());
        Self {
            handle,
            _client: client,
        }
    }
}

impl Drop for TestReader {
    fn drop(&mut self) {
        // SAFETY: the handle was returned by the library, is freed here once, and is not used afterwards.
        unsafe { bitacora_reader_free(self.handle) };
    }
}

/// The status of reading a range through `reader`, with the records freed if any came back.
fn read_range_status(reader: &TestReader, first_sequence: u64, last_sequence: u64) -> BitacoraStatus {
    let mut records = std::ptr::null_mut();
    // SAFETY: the reader and the output are live for the call, and the low-water mark is left out.
    let status = unsafe {
        bitacora_reader_read_range(
            reader.handle,
            first_sequence,
            last_sequence,
            &raw mut records,
            std::ptr::null_mut(),
        )
    };
    // SAFETY: the handle is null or was just returned, and is freed once.
    unsafe { crate::records::bitacora_records_free(records) };
    status
}

#[test]
fn opening_a_reader_contacts_nobody_and_succeeds() {
    let reader = TestReader::open();

    assert!(!reader.handle.is_null());
}

#[test]
fn opening_without_a_client_is_refused_by_name() {
    let mut reader = std::ptr::null_mut();

    // SAFETY: a null client is allowed, the name is NUL-terminated, and the output is a live pointer.
    let status = unsafe { bitacora_reader_open(std::ptr::null(), c"orders".as_ptr(), &raw mut reader) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn opening_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();
    let mut reader = std::ptr::null_mut();

    // SAFETY: the client is live, a null name is allowed, and the output is a live pointer.
    let status = unsafe { bitacora_reader_open(client.handle(), std::ptr::null(), &raw mut reader) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn opening_with_a_name_that_is_not_utf8_is_an_invalid_argument() {
    let client = TestClient::connect();
    let not_utf8: [u8; 3] = [0xFF, 0xFE, 0x00];
    let mut reader = std::ptr::null_mut();

    // SAFETY: the client is live, the bytes end with a NUL, and the output is a live pointer.
    let status = unsafe { bitacora_reader_open(client.handle(), not_utf8.as_ptr().cast::<c_char>(), &raw mut reader) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn opening_without_a_place_for_the_reader_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, the name is NUL-terminated, and a null output is allowed.
    let status = unsafe { bitacora_reader_open(client.handle(), c"orders".as_ptr(), std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "reader_out is null");
}

#[test]
fn failed_open_leaves_a_null_reader() {
    let mut reader = std::ptr::NonNull::<ReaderHandle>::dangling().as_ptr();

    // SAFETY: a null client is allowed, the name is NUL-terminated, and the output is a live pointer whose
    // dangling content is overwritten, never read.
    unsafe { bitacora_reader_open(std::ptr::null(), c"orders".as_ptr(), &raw mut reader) };

    assert!(reader.is_null());
}

#[test]
fn reading_without_a_reader_is_refused_by_name() {
    let mut records = std::ptr::null_mut();

    // SAFETY: a null reader is allowed, the output is a live pointer, and the mark is left out.
    let status = unsafe { bitacora_reader_read_range(std::ptr::null(), 1, 10, &raw mut records, std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "reader is null");
}

#[test]
fn reading_without_a_place_for_the_records_is_refused_by_name() {
    let reader = TestReader::open();

    // SAFETY: the reader is live, and null outputs are allowed.
    let status =
        unsafe { bitacora_reader_read_range(reader.handle, 1, 10, std::ptr::null_mut(), std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "records_out is null");
}

#[test]
fn range_that_starts_at_zero_is_an_invalid_argument() {
    let reader = TestReader::open();

    let status = read_range_status(&reader, 0, 10);

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn range_that_ends_before_it_starts_is_an_invalid_argument() {
    let reader = TestReader::open();

    let status = read_range_status(&reader, 10, 9);

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn failed_read_leaves_null_records() {
    let reader = TestReader::open();
    let mut records = std::ptr::NonNull::<RecordsHandle>::dangling().as_ptr();

    // SAFETY: the reader is live, the output is a live pointer whose dangling content is overwritten,
    // never read, and the mark is left out.
    unsafe { bitacora_reader_read_range(reader.handle, 0, 10, &raw mut records, std::ptr::null_mut()) };

    assert!(records.is_null());
}

#[test]
fn failed_read_that_is_not_a_truncation_leaves_the_mark_untouched() {
    let reader = TestReader::open();
    let mut records = std::ptr::null_mut();
    let mut low_water_mark = 77_u64;

    // SAFETY: the reader and both outputs are live for the call.
    unsafe { bitacora_reader_read_range(reader.handle, 0, 10, &raw mut records, &raw mut low_water_mark) };

    assert_eq!(low_water_mark, 77);
}

#[test]
fn truncation_is_reported_with_its_low_water_mark() {
    let mut low_water_mark = 0_u64;
    // SAFETY: the pointer names a live `u64` for the whole test.
    let low_water_mark_out = unsafe { optional_output_slot(&raw mut low_water_mark) };
    let truncated = Failure::from(ReadError::Truncated {
        low_water_mark: SequenceNumber::new(40),
    });

    let reported = report_low_water_mark(truncated, low_water_mark_out);

    assert_eq!(reported.status, BitacoraStatus::Truncated);
    assert_eq!(low_water_mark, 40);
}

#[test]
fn truncation_is_reported_to_a_caller_that_does_not_want_the_mark() {
    let truncated = Failure::from(ReadError::Truncated {
        low_water_mark: SequenceNumber::new(40),
    });

    let reported = report_low_water_mark(truncated, None);

    assert_eq!(reported.status, BitacoraStatus::Truncated);
}

#[test]
fn failure_without_a_mark_writes_none() {
    let mut low_water_mark = 77_u64;
    // SAFETY: the pointer names a live `u64` for the whole test.
    let low_water_mark_out = unsafe { optional_output_slot(&raw mut low_water_mark) };
    let not_found = Failure::new(BitacoraStatus::NotFound, "stream \"orders\" does not exist");

    let reported = report_low_water_mark(not_found, low_water_mark_out);

    assert_eq!(reported.status, BitacoraStatus::NotFound);
    assert_eq!(low_water_mark, 77);
}

#[test]
fn low_water_mark_without_a_reader_is_refused_by_name() {
    let mut low_water_mark = 0_u64;

    // SAFETY: a null reader is allowed, and the output is a live `u64`.
    let status = unsafe { bitacora_reader_low_water_mark(std::ptr::null(), &raw mut low_water_mark) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "reader is null");
}

#[test]
fn low_water_mark_without_a_place_for_it_is_refused_by_name() {
    let reader = TestReader::open();

    // SAFETY: the reader is live, and a null output is allowed.
    let status = unsafe { bitacora_reader_low_water_mark(reader.handle, std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "low_water_mark_out is null");
}

#[test]
fn following_without_a_reader_is_refused_by_name() {
    let mut follower = std::ptr::null_mut();

    // SAFETY: a null reader is allowed, and the output is a live pointer.
    let status = unsafe { bitacora_reader_follow(std::ptr::null(), 1, &raw mut follower) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "reader is null");
}

#[test]
fn following_without_a_place_for_the_follower_is_refused_by_name() {
    let reader = TestReader::open();

    // SAFETY: the reader is live, and a null output is allowed.
    let status = unsafe { bitacora_reader_follow(reader.handle, 1, std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "follower_out is null");
}

#[test]
fn failed_follow_leaves_a_null_follower() {
    let mut follower = std::ptr::NonNull::<FollowerHandle>::dangling().as_ptr();

    // SAFETY: a null reader is allowed, and the output is a live pointer whose dangling content is
    // overwritten, never read.
    unsafe { bitacora_reader_follow(std::ptr::null(), 1, &raw mut follower) };

    assert!(follower.is_null());
}

#[test]
fn waiting_without_a_follower_is_refused_by_name() {
    let mut records = std::ptr::null_mut();

    // SAFETY: a null follower is allowed, the output is a live pointer, and the mark is left out.
    let status = unsafe { bitacora_follower_next(std::ptr::null(), 10, &raw mut records, std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "follower is null");
}

#[test]
fn waiting_without_a_place_for_the_records_is_refused_by_name() {
    // SAFETY: null pointers are allowed.
    let status = unsafe { bitacora_follower_next(std::ptr::null(), 10, std::ptr::null_mut(), std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "records_out is null");
}

#[test]
fn freeing_a_null_reader_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { bitacora_reader_free(std::ptr::null_mut()) };
}

#[test]
fn freeing_a_null_follower_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { bitacora_follower_free(std::ptr::null_mut()) };
}
