//! Tests of the writer handle that need no cluster.
//!
//! # What is covered
//!
//! - Every argument check of every writer function: null pointers, an unknown takeover policy, a payload
//!   with a length and no bytes, in a single append and inside a batch.
//! - What a closed writer answers.
//! - What the caller is left with after a failed open.
//!
//! Appending, taking over and closing against replicas are covered by `tests/c_programs_test.rs`, which
//! runs a C program against a cluster of real processes.
//!
//! # How the fixtures work
//!
//! `closed_writer` builds a writer handle in the state `bitacora_writer_close` leaves behind, without a
//! stream behind it. Every check that comes before the writer is used can be reached through it.

use super::*;
use crate::test_support::{TestClient, last_message};

/// A writer handle that was closed, owned by the test.
fn closed_writer(client: &TestClient) -> WriterHandle {
    WriterHandle {
        writer: Mutex::new(None),
        core: client.core(),
    }
}

/// A payload of a batch that names `bytes`.
fn payload_of(bytes: &[u8]) -> Payload {
    Payload {
        bytes: bytes.as_ptr(),
        length: bytes.len(),
    }
}

#[test]
fn both_takeover_policies_are_understood() {
    assert_eq!(
        takeover_policy_from(BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY).ok(),
        Some(TakeoverPolicy::WaitForLeaseExpiry)
    );
    assert_eq!(
        takeover_policy_from(BITACORA_TAKEOVER_FORCE).ok(),
        Some(TakeoverPolicy::Force)
    );
}

#[test]
fn unknown_takeover_policy_is_an_invalid_argument() {
    let refused = takeover_policy_from(7).expect_err("7 is not a policy");

    assert_eq!(refused.status, BitacoraStatus::InvalidArgument);
}

#[test]
fn opening_without_a_client_is_refused_by_name() {
    let mut writer = std::ptr::null_mut();

    // SAFETY: a null client is allowed, the name is NUL-terminated, and the output is a live pointer.
    let status = unsafe {
        bitacora_writer_open(
            std::ptr::null(),
            c"orders".as_ptr(),
            BITACORA_TAKEOVER_FORCE,
            &raw mut writer,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "client is null");
}

#[test]
fn opening_without_a_stream_name_is_refused_by_name() {
    let client = TestClient::connect();
    let mut writer = std::ptr::null_mut();

    // SAFETY: the client is live, a null name is allowed, and the output is a live pointer.
    let status = unsafe {
        bitacora_writer_open(
            client.handle(),
            std::ptr::null(),
            BITACORA_TAKEOVER_FORCE,
            &raw mut writer,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "stream_name is null");
}

#[test]
fn opening_with_an_unknown_policy_is_an_invalid_argument() {
    let client = TestClient::connect();
    let mut writer = std::ptr::null_mut();

    // SAFETY: the client is live, the name is NUL-terminated, and the output is a live pointer.
    let status = unsafe { bitacora_writer_open(client.handle(), c"orders".as_ptr(), 7, &raw mut writer) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
}

#[test]
fn opening_without_a_place_for_the_writer_is_refused_by_name() {
    let client = TestClient::connect();

    // SAFETY: the client is live, the name is NUL-terminated, and a null output is allowed.
    let status = unsafe {
        bitacora_writer_open(
            client.handle(),
            c"orders".as_ptr(),
            BITACORA_TAKEOVER_FORCE,
            std::ptr::null_mut(),
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "writer_out is null");
}

#[test]
fn failed_open_leaves_a_null_writer() {
    let mut writer = std::ptr::NonNull::<WriterHandle>::dangling().as_ptr();

    // SAFETY: a null client is allowed, the name is NUL-terminated, and the output is a live pointer whose
    // dangling content is overwritten, never read.
    unsafe {
        bitacora_writer_open(
            std::ptr::null(),
            c"orders".as_ptr(),
            BITACORA_TAKEOVER_FORCE,
            &raw mut writer,
        )
    };

    assert!(writer.is_null());
}

#[test]
fn appending_without_a_writer_is_refused_by_name() {
    let payload = b"redo record";
    let mut commit_sequence = 0_u64;

    // SAFETY: a null writer is allowed; the payload and the output are live for the call.
    let status = unsafe {
        bitacora_writer_append(
            std::ptr::null(),
            payload.as_ptr(),
            payload.len(),
            0,
            &raw mut commit_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "writer is null");
}

#[test]
fn appending_a_payload_with_a_length_and_no_bytes_is_refused() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let mut commit_sequence = 0_u64;

    // SAFETY: the writer and the output are live; the null payload is refused before it is used.
    let status = unsafe { bitacora_writer_append(&raw const writer, std::ptr::null(), 5, 0, &raw mut commit_sequence) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "payload is null but its length is 5");
}

#[test]
fn appending_without_a_place_for_the_sequence_is_refused_by_name() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payload = b"redo record";

    // SAFETY: the writer and the payload are live; a null output is allowed.
    let status = unsafe {
        bitacora_writer_append(
            &raw const writer,
            payload.as_ptr(),
            payload.len(),
            0,
            std::ptr::null_mut(),
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "commit_sequence_out is null");
}

#[test]
fn appending_to_a_closed_writer_is_closed() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payload = b"redo record";
    let mut commit_sequence = 0_u64;

    // SAFETY: the writer, the payload and the output are live for the call.
    let status = unsafe {
        bitacora_writer_append(
            &raw const writer,
            payload.as_ptr(),
            payload.len(),
            0,
            &raw mut commit_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::Closed);
}

#[test]
fn refused_append_leaves_the_sequence_untouched() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payload = b"redo record";
    let mut commit_sequence = 77_u64;

    // SAFETY: the writer, the payload and the output are live for the call.
    unsafe {
        bitacora_writer_append(
            &raw const writer,
            payload.as_ptr(),
            payload.len(),
            0,
            &raw mut commit_sequence,
        )
    };

    assert_eq!(commit_sequence, 77);
}

#[test]
fn batch_without_a_writer_is_refused_by_name() {
    let payloads = [payload_of(b"first"), payload_of(b"second")];
    let mut first_sequence = 0_u64;
    let mut last_sequence = 0_u64;

    // SAFETY: a null writer is allowed; the payloads and the outputs are live for the call.
    let status = unsafe {
        bitacora_writer_append_batch(
            std::ptr::null(),
            payloads.as_ptr(),
            payloads.len(),
            0,
            &raw mut first_sequence,
            &raw mut last_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "writer is null");
}

#[test]
fn batch_with_a_count_and_no_payloads_is_refused() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let mut first_sequence = 0_u64;
    let mut last_sequence = 0_u64;

    // SAFETY: the writer and the outputs are live; the null payloads are refused before they are used.
    let status = unsafe {
        bitacora_writer_append_batch(
            &raw const writer,
            std::ptr::null(),
            2,
            0,
            &raw mut first_sequence,
            &raw mut last_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "payloads is null but payload_count is 2");
}

#[test]
fn batch_entry_with_a_length_and_no_bytes_is_refused_by_its_position() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payloads = [
        payload_of(b"first"),
        Payload {
            bytes: std::ptr::null(),
            length: 3,
        },
    ];
    let mut first_sequence = 0_u64;
    let mut last_sequence = 0_u64;

    // SAFETY: the writer, the entries and the outputs are live; the null bytes are refused before use.
    let status = unsafe {
        bitacora_writer_append_batch(
            &raw const writer,
            payloads.as_ptr(),
            payloads.len(),
            0,
            &raw mut first_sequence,
            &raw mut last_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "payloads[1].bytes is null but its length is 3");
}

#[test]
fn batch_without_a_place_for_its_last_sequence_is_refused_by_name() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payloads = [payload_of(b"first")];
    let mut first_sequence = 0_u64;

    // SAFETY: the writer, the payloads and the first output are live; a null output is allowed.
    let status = unsafe {
        bitacora_writer_append_batch(
            &raw const writer,
            payloads.as_ptr(),
            payloads.len(),
            0,
            &raw mut first_sequence,
            std::ptr::null_mut(),
        )
    };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "last_sequence_out is null");
}

#[test]
fn batch_to_a_closed_writer_is_closed() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let payloads = [payload_of(b"first"), payload_of(b"")];
    let mut first_sequence = 0_u64;
    let mut last_sequence = 0_u64;

    // SAFETY: the writer, the payloads and the outputs are live for the call.
    let status = unsafe {
        bitacora_writer_append_batch(
            &raw const writer,
            payloads.as_ptr(),
            payloads.len(),
            0,
            &raw mut first_sequence,
            &raw mut last_sequence,
        )
    };

    assert_eq!(status, BitacoraStatus::Closed);
}

#[test]
fn durable_sequence_without_a_writer_is_refused_by_name() {
    let mut durable_sequence = 0_u64;

    // SAFETY: a null writer is allowed, and the output is a live `u64`.
    let status = unsafe { bitacora_writer_durable_sequence(std::ptr::null(), &raw mut durable_sequence) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "writer is null");
}

#[test]
fn durable_sequence_without_a_place_for_it_is_refused_by_name() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);

    // SAFETY: the writer is live, and a null output is allowed.
    let status = unsafe { bitacora_writer_durable_sequence(&raw const writer, std::ptr::null_mut()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "durable_sequence_out is null");
}

#[test]
fn durable_sequence_of_a_closed_writer_is_closed() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);
    let mut durable_sequence = 0_u64;

    // SAFETY: the writer and the output are live for the call.
    let status = unsafe { bitacora_writer_durable_sequence(&raw const writer, &raw mut durable_sequence) };

    assert_eq!(status, BitacoraStatus::Closed);
}

#[test]
fn closing_without_a_writer_is_refused_by_name() {
    // SAFETY: a null writer is allowed.
    let status = unsafe { bitacora_writer_close(std::ptr::null()) };

    assert_eq!(status, BitacoraStatus::InvalidArgument);
    assert_eq!(last_message(), "writer is null");
}

#[test]
fn closing_a_closed_writer_is_closed() {
    let client = TestClient::connect();
    let writer = closed_writer(&client);

    // SAFETY: the writer is live and no other call is using it.
    let status = unsafe { bitacora_writer_close(&raw const writer) };

    assert_eq!(status, BitacoraStatus::Closed);
}

#[test]
fn freeing_a_null_writer_does_nothing() {
    // SAFETY: a null pointer is allowed.
    unsafe { bitacora_writer_free(std::ptr::null_mut()) };
}

#[test]
fn freeing_a_closed_writer_releases_it() {
    let client = TestClient::connect();
    let writer = into_handle_pointer(closed_writer(&client));

    // SAFETY: the handle was just made by `into_handle_pointer`, is freed once, and is not used afterwards.
    unsafe { bitacora_writer_free(writer) };
}
