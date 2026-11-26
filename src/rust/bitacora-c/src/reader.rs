//! The reader and follower handles: reading a stream from C.
//!
//! # Why this file exists
//!
//! Whatever is derived from the log is built by reading it: a database replays it after a restart, a
//! replica of the database follows it as records commit. Both need the committed records in order, and
//! both need to be told plainly when what they ask for no longer exists. This file gives C the two ways
//! the Rust client reads: a range that is already there, and the tail as it grows.
//!
//! # What it contains
//!
//! - [`ReaderHandle`]: what C holds as `bitacora_reader`.
//! - [`FollowerHandle`]: what C holds as `bitacora_follower`, a position in the stream that moves forward.
//! - [`bitacora_reader_open`], [`bitacora_reader_read_range`], [`bitacora_reader_low_water_mark`],
//!   [`bitacora_reader_follow`], [`bitacora_reader_free`].
//! - [`bitacora_follower_next`], [`bitacora_follower_free`].
//!
//! # How it works
//!
//! A reader holds no position and no connection of its own; any number of them can read the same stream,
//! and none of them talks to the writer. A range read returns the committed records of the range that
//! exist at the moment of the call, so it is shorter than the range only at the end of the stream.
//!
//! A follower remembers where it is. Each call to `bitacora_follower_next` waits for records after that
//! position, hands back the ones that have arrived, and moves past them. Trouble that can pass, such as a
//! replica that does not answer, is retried inside the call. If nothing arrives within the wait the
//! caller allowed, the call says so with `BITACORA_STATUS_NOTHING_NEW`, which is not an error, and the
//! position stays where it was.
//!
//! A read below the low-water mark fails with `BITACORA_STATUS_TRUNCATED` and reports the mark, so that
//! the caller knows where reading can resume. It is never answered with silence or with fewer records.
//!
//! ```
//! use bitacora_c::{BitacoraStatus, bitacora_follower_next};
//!
//! // Without a follower there is nothing to wait for, and the outputs are left alone or cleared.
//! let mut records = std::ptr::null_mut();
//! // SAFETY: null pointers are allowed, and the output points to a live pointer.
//! let status = unsafe { bitacora_follower_next(std::ptr::null(), 10, &raw mut records, std::ptr::null_mut()) };
//! assert_eq!(status, BitacoraStatus::InvalidArgument);
//! assert!(records.is_null());
//! ```
//!
//! # Invariants
//!
//! - Records are handed out in sequence order without gaps, and a follower never hands out a record
//!   twice.
//! - A wait that ends without records loses none: the next call continues from the same position.
//! - The low-water mark output is written if and only if the status is `BITACORA_STATUS_TRUNCATED`.
//!
//! # See also
//!
//! - `records.rs`: the handle a read returns its records in.
//! - `client.rs`: how calls block and what a wait limit does.
//! - `docs/protocol/replication.md`: what a reader is promised.

use std::ffi::c_char;
use std::sync::Arc;

use bitacora::{RecordStream, SequenceNumber, SequenceRange, StreamReader};
use tokio::sync::Mutex;

use crate::arguments::{
    OutputSlot, free_handle, handle_reference, into_handle_pointer, optional_output_slot, output_slot, text_argument,
};
use crate::client::{ClientCore, ClientHandle};
use crate::guard::{run_guarded, run_guarded_value};
use crate::records::RecordsHandle;
use crate::status::{BitacoraStatus, Failure};

/// A reader of one stream.
///
/// Every function that takes a reader may be called from several threads at once with the same reader.
pub struct ReaderHandle {
    /// The reader. It holds no position; every call says where it reads.
    reader: StreamReader,
    /// The client and its threads.
    core: Arc<ClientCore>,
}

/// A position in a stream that follows the records as they commit.
///
/// A follower is for one thread at a time: calls from several threads take turns, each continuing where
/// the one before stopped.
pub struct FollowerHandle {
    /// The stream of records from the follower's position on, shared with the call that is using it.
    /// Locked for the length of a call, because taking the next records moves the position. Declared
    /// before the core so that it stops while the client's threads still exist.
    followed_records: Arc<Mutex<RecordStream>>,
    /// The client and its threads.
    core: Arc<ClientCore>,
}

/// Reports a failed read: writes the low-water mark if the records were truncated, and passes the
/// failure on.
///
/// # Arguments
///
/// * `failure` - Why the read failed.
/// * `low_water_mark_out` - Where the caller wants the low-water mark, if it wants it.
///
/// # Returns
///
/// The same failure, for the caller to return.
fn report_low_water_mark(failure: Failure, low_water_mark_out: Option<OutputSlot<u64>>) -> Failure {
    if let (Some(low_water_mark), Some(low_water_mark_out)) = (failure.low_water_mark, low_water_mark_out) {
        low_water_mark_out.write(low_water_mark.get());
    }
    failure
}

/// Opens a reader of a stream.
///
/// Opening contacts nobody and cannot tell whether the stream exists; the first read does. Any number of
/// readers may be open on a stream, in any number of programs.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `reader_out` - Where the new reader is written. It is set to null if the call fails. Free the reader
///   with `bitacora_reader_free`.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or `BITACORA_STATUS_INVALID_ARGUMENT` if a pointer is null or the name is not
/// UTF-8.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
/// `reader_out` must be null or point to a pointer that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_reader_open(
    client: *const ClientHandle,
    stream_name: *const c_char,
    reader_out: *mut *mut ReaderHandle,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `reader_out` is null or a writable pointer.
        let reader_out = unsafe { output_slot(reader_out, "reader_out") }?;
        reader_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;

        let core = client.core();
        let reader = core.client().open_reader(stream_name);
        reader_out.write(into_handle_pointer(ReaderHandle { reader, core }));
        Ok(())
    })
}

/// Reads the committed records of a range of sequence numbers.
///
/// The result holds the records from `first_sequence` to `last_sequence`, in order. It holds fewer only
/// if the stream does not reach `last_sequence` yet: then it ends at the last committed record, and may
/// be empty. Only committed records are ever returned.
///
/// # Arguments
///
/// * `reader` - The reader of the stream.
/// * `first_sequence` - The first sequence number wanted, 1 or more.
/// * `last_sequence` - The last sequence number wanted, not below `first_sequence`.
/// * `records_out` - Where the records are written on success. It is set to null if the call fails. Free
///   them with `bitacora_records_free`.
/// * `low_water_mark_out` - Where the low-water mark is written if the status is
///   `BITACORA_STATUS_TRUNCATED`: the first sequence number that can still be read. May be null.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_TRUNCATED` - The range starts below the low-water mark; those records were deleted.
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store, or every place that holds the records, could not
///   be reached. Try again later.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A required pointer is null, or the range is not valid.
///
/// # Safety
///
/// `reader` must be null or a live reader. `records_out` must be null or point to a pointer that can be
/// written. `low_water_mark_out` must be null or point to a `uint64_t` that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_reader_read_range(
    reader: *const ReaderHandle,
    first_sequence: u64,
    last_sequence: u64,
    records_out: *mut *mut RecordsHandle,
    low_water_mark_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `records_out` is null or a writable pointer.
        let records_out = unsafe { output_slot(records_out, "records_out") }?;
        records_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that the output is null or a writable `uint64_t`.
        let low_water_mark_out = unsafe { optional_output_slot(low_water_mark_out) };
        // SAFETY: the caller guarantees that `reader` is null or a live reader.
        let reader = unsafe { handle_reference(reader, "reader") }?;
        let sequence_range =
            match SequenceRange::new(SequenceNumber::new(first_sequence), SequenceNumber::new(last_sequence)) {
                Ok(sequence_range) => sequence_range,
                Err(invalid_range) => return Err(Failure::invalid_argument(invalid_range.to_string())),
            };

        let rust_reader = reader.reader.clone();
        match reader
            .core
            .wait_for(async move { rust_reader.read_range(sequence_range).await })
        {
            Ok(records) => {
                records_out.write(into_handle_pointer(RecordsHandle::new(records)));
                Ok(())
            }
            Err(read_error) => Err(report_low_water_mark(Failure::from(read_error), low_water_mark_out)),
        }
    })
}

/// Reports the first sequence number of the stream that can still be read.
///
/// Everything below it has been truncated. It is 1 for a stream that was never truncated, and it never
/// goes back.
///
/// # Arguments
///
/// * `reader` - The reader of the stream.
/// * `low_water_mark_out` - Where the sequence number is written on success.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
///
/// # Safety
///
/// `reader` must be null or a live reader. `low_water_mark_out` must be null or point to a `uint64_t`
/// that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_reader_low_water_mark(
    reader: *const ReaderHandle,
    low_water_mark_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `reader` is null or a live reader.
        let reader = unsafe { handle_reference(reader, "reader") }?;
        // SAFETY: the caller guarantees that the output is null or a writable `uint64_t`.
        let low_water_mark_out = unsafe { output_slot(low_water_mark_out, "low_water_mark_out") }?;

        let rust_reader = reader.reader.clone();
        let low_water_mark = reader
            .core
            .wait_for(async move { rust_reader.low_water_mark().await })?;
        low_water_mark_out.write(low_water_mark.get());
        Ok(())
    })
}

/// Starts following a stream from a sequence number.
///
/// The follower hands out every committed record from `from_sequence` on, through
/// `bitacora_follower_next`: first the ones that exist already, then new ones as they commit. It keeps
/// going across segment changes and writer changes.
///
/// # Arguments
///
/// * `reader` - The reader of the stream.
/// * `from_sequence` - The first sequence number wanted. 0 and 1 both mean the first record.
/// * `follower_out` - Where the new follower is written. It is set to null if the call fails. Free the
///   follower with `bitacora_follower_free`.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
///
/// # Safety
///
/// `reader` must be null or a live reader. `follower_out` must be null or point to a pointer that can be
/// written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_reader_follow(
    reader: *const ReaderHandle,
    from_sequence: u64,
    follower_out: *mut *mut FollowerHandle,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `follower_out` is null or a writable pointer.
        let follower_out = unsafe { output_slot(follower_out, "follower_out") }?;
        follower_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that `reader` is null or a live reader.
        let reader = unsafe { handle_reference(reader, "reader") }?;

        let rust_reader = reader.reader.clone();
        let followed_records = reader
            .core
            .wait_for(async move { rust_reader.follow_from(SequenceNumber::new(from_sequence)).await })?;
        follower_out.write(into_handle_pointer(FollowerHandle {
            followed_records: Arc::new(Mutex::new(followed_records)),
            core: Arc::clone(&reader.core),
        }));
        Ok(())
    })
}

/// Frees a reader. Followers started from it keep working. A null pointer is ignored.
///
/// # Arguments
///
/// * `reader` - The reader to free, or null.
///
/// # Safety
///
/// `reader` must be null or a reader returned by this library that has not been freed. No other call may
/// be using it, and it must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_reader_free(reader: *mut ReaderHandle) {
    run_guarded_value((), || {
        // SAFETY: the caller guarantees what `free_handle` asks for.
        unsafe { free_handle(reader) };
    });
}

/// Waits for the next committed records of a followed stream and moves the follower past them.
///
/// On success the result holds at least one record: the next one after the follower's position, and
/// whatever arrived with it. A wait that ends with nothing loses nothing; the next call continues from
/// the same position.
///
/// # Arguments
///
/// * `follower` - The follower.
/// * `timeout_milliseconds` - The longest to wait for a record, or `BITACORA_WAIT_WITHOUT_LIMIT`.
/// * `records_out` - Where the records are written on success. It is set to null otherwise. Free them
///   with `bitacora_records_free`.
/// * `low_water_mark_out` - Where the low-water mark is written if the status is
///   `BITACORA_STATUS_TRUNCATED`. May be null.
///
/// # Returns
///
/// `BITACORA_STATUS_OK` with records, or:
///
/// * `BITACORA_STATUS_NOTHING_NEW` - No record arrived within the wait. Not an error: call again.
/// * `BITACORA_STATUS_TRUNCATED` - The follower's position was truncated before it read it. The follower
///   has ended; start a new one at the low-water mark.
/// * `BITACORA_STATUS_NOT_FOUND` - The stream was deleted. The follower has ended.
/// * `BITACORA_STATUS_CLOSED` - The follower ended in an earlier call. Free it.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A required pointer is null.
///
/// # Safety
///
/// `follower` must be null or a live follower. `records_out` must be null or point to a pointer that can
/// be written. `low_water_mark_out` must be null or point to a `uint64_t` that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_follower_next(
    follower: *const FollowerHandle,
    timeout_milliseconds: u64,
    records_out: *mut *mut RecordsHandle,
    low_water_mark_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `records_out` is null or a writable pointer.
        let records_out = unsafe { output_slot(records_out, "records_out") }?;
        records_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that the output is null or a writable `uint64_t`.
        let low_water_mark_out = unsafe { optional_output_slot(low_water_mark_out) };
        // SAFETY: the caller guarantees that `follower` is null or a live follower.
        let follower = unsafe { handle_reference(follower, "follower") }?;

        // The wait covers the turn of this call as well: a call from another thread may hold the stream.
        let followed_records = Arc::clone(&follower.followed_records);
        let Some(next_records) = follower.core.wait_at_most(timeout_milliseconds, async move {
            let mut followed_records = followed_records.lock().await;
            followed_records.next_batch().await
        }) else {
            return Err(Failure::new(
                BitacoraStatus::NothingNew,
                format!("no record arrived within {timeout_milliseconds} ms"),
            ));
        };
        match next_records {
            Some(Ok(records)) => {
                records_out.write(into_handle_pointer(RecordsHandle::new(records)));
                Ok(())
            }
            Some(Err(read_error)) => Err(report_low_water_mark(Failure::from(read_error), low_water_mark_out)),
            None => Err(Failure::new(BitacoraStatus::Closed, "the follower has ended")),
        }
    })
}

/// Frees a follower. A null pointer is ignored.
///
/// # Arguments
///
/// * `follower` - The follower to free, or null.
///
/// # Safety
///
/// `follower` must be null or a follower returned by this library that has not been freed. No other call
/// may be using it, and it must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_follower_free(follower: *mut FollowerHandle) {
    run_guarded_value((), || {
        // SAFETY: the caller guarantees what `free_handle` asks for.
        unsafe { free_handle(follower) };
    });
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod tests;
