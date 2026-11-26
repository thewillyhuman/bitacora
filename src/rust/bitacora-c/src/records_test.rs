//! Tests of the records handle and its accessors.
//!
//! # What is covered
//!
//! - Counting, and lending out each record's sequence number and bytes without copying them.
//! - A record without bytes.
//! - Every argument check: a null handle, an index past the end, null outputs, and the one output that
//!   may be left out.
//! - Freeing, with and without a handle.
//!
//! # How the fixtures work
//!
//! `two_records` builds a handle the way a read does: from records with their sequence numbers. The
//! second record is empty.

use bitacora::SequenceNumber;
use bytes::Bytes;

use super::*;
use crate::test_support::last_message;

/// A handle with record 40, `first`, and record 41, which is empty.
fn two_records() -> RecordsHandle {
    RecordsHandle::new(vec![
        LogRecord {
            sequence: SequenceNumber::new(40),
            payload: Bytes::from_static(b"first"),
        },
        LogRecord {
            sequence: SequenceNumber::new(41),
            payload: Bytes::new(),
        },
    ])
}

/// What `bitacora_records_get` handed out for one record.
struct LentRecord {
    /// The status the call returned.
    status: BitacoraStatus,
    /// The sequence number it wrote.
    sequence: u64,
    /// The bytes behind the pointer and length it wrote, copied while the handle is alive.
    payload: Vec<u8>,
}

/// Asks `records` for the record at `record_index`, with every output wanted.
fn lend_record(records: &RecordsHandle, record_index: usize) -> LentRecord {
    let mut sequence = 0_u64;
    let mut payload = std::ptr::null();
    let mut payload_length = 0_usize;
    // SAFETY: the handle and the three outputs are live for the call.
    let status = unsafe {
        bitacora_records_get(
            records,
            record_index,
            &raw mut sequence,
            &raw mut payload,
            &raw mut payload_length,
        )
    };
    let payload = if status == BitacoraStatus::Ok && payload_length > 0 {
        // SAFETY: on success the pointer names `payload_length` bytes that live as long as the handle.
        unsafe { std::slice::from_raw_parts(payload, payload_length) }.to_vec()
    } else {
        Vec::new()
    };
    LentRecord {
        status,
        sequence,
        payload,
    }
}

#[test]
fn count_is_the_number_of_records_read() {
    let records = two_records();

    // SAFETY: the handle is live for the call.
    let count = unsafe { bitacora_records_count(&raw const records) };

    assert_eq!(count, 2);
}

#[test]
fn count_without_records_is_zero() {
    // SAFETY: a null handle is allowed.
    let count = unsafe { bitacora_records_count(std::ptr::null()) };

    assert_eq!(count, 0);
}

#[test]
fn record_is_lent_with_its_sequence_number() {
    let records = two_records();

    let lent = lend_record(&records, 0);

    assert_eq!(lent.status, BitacoraStatus::Ok);
    assert_eq!(lent.sequence, 40);
}

#[test]
fn record_is_lent_with_its_bytes() {
    let records = two_records();

    let lent = lend_record(&records, 0);

    assert_eq!(lent.payload, b"first");
}

#[test]
fn lent_bytes_are_the_handles_own_memory() {
    let records = two_records();
    let mut payload = std::ptr::null();
    let mut payload_length = 0_usize;

    // SAFETY: the handle and the outputs are live for the call; the sequence number is left out.
    unsafe {
        bitacora_records_get(
            &raw const records,
            0,
            std::ptr::null_mut(),
            &raw mut payload,
            &raw mut payload_length,
        )
    };

    assert_eq!(payload, records.records[0].payload.as_ptr());
}

#[test]
fn empty_record_is_lent_with_a_length_of_zero() {
    let records = two_records();
    let mut payload = std::ptr::null();
    let mut payload_length = 99_usize;

    // SAFETY: the handle and the outputs are live for the call; the sequence number is left out.
    let status = unsafe {
        bitacora_records_get(
            &raw const records,
            1,
            std::ptr::null_mut(),
            &raw mut payload,
            &raw mut payload_length,
        )
    };

    assert_eq!(status, BitacoraStatus::Ok);
    assert_eq!(payload_length, 0);
}

#[test]
fn index_past_the_last_record_is_refused() {
    let records = two_records();

    let lent = lend_record(&records, 2);

    assert_eq!(lent.status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "record_index is 2 but there are 2 records");
}

#[test]
fn record_of_a_null_handle_is_refused_by_name() {
    let mut payload = std::ptr::null();
    let mut payload_length = 0_usize;

    // SAFETY: a null handle is allowed, and the outputs are live for the call.
    let status = unsafe {
        bitacora_records_get(
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            &raw mut payload,
            &raw mut payload_length,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "records is null");
}

#[test]
fn record_without_a_place_for_its_bytes_is_refused_by_name() {
    let records = two_records();
    let mut payload_length = 0_usize;

    // SAFETY: the handle and the length output are live; a null output is allowed.
    let status = unsafe {
        bitacora_records_get(
            &raw const records,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut payload_length,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "payload_out is null");
}

#[test]
fn record_without_a_place_for_its_length_is_refused_by_name() {
    let records = two_records();
    let mut payload = std::ptr::null();

    // SAFETY: the handle and the pointer output are live; a null output is allowed.
    let status = unsafe {
        bitacora_records_get(
            &raw const records,
            0,
            std::ptr::null_mut(),
            &raw mut payload,
            std::ptr::null_mut(),
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "payload_length_out is null");
}

#[test]
fn freeing_records_releases_them() {
    let records = crate::arguments::into_handle_pointer(two_records());

    // SAFETY: the handle was just made by `into_handle_pointer`, is freed once, and is not used afterwards.
    unsafe { bitacora_records_free(records) };
}

#[test]
fn freeing_null_records_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { bitacora_records_free(std::ptr::null_mut()) };
}
