//! The writer handle: appending to a stream from C.
//!
//! # Why this file exists
//!
//! Appending is the reason a database links this library. A C caller needs to become the writer of a
//! stream, hand over bytes, and learn for each record exactly one of four things: it is committed, this
//! writer has been replaced, the outcome is unknown, or the record was refused. This file is that part of
//! the interface.
//!
//! # What it contains
//!
//! - [`WriterHandle`]: what C holds as `bitacora_writer`.
//! - [`Payload`]: one record of a batch, as `bitacora_payload`.
//! - [`bitacora_writer_open`], [`bitacora_writer_append`], [`bitacora_writer_append_batch`],
//!   [`bitacora_writer_durable_sequence`], [`bitacora_writer_close`], [`bitacora_writer_free`].
//! - [`BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY`], [`BITACORA_TAKEOVER_FORCE`]: how to treat a writer that
//!   may still be alive.
//!
//! # How it works
//!
//! Opening a writer runs the takeover protocol of the design: it raises the stream's epoch, fences and
//! seals what the previous writer left open, and opens a new segment. From then on this writer is the
//! only one whose appends can commit, until another one does the same.
//!
//! An append copies the payload, queues it and blocks the calling thread until the record is stored on a
//! write quorum. Several threads may append through one writer at once; records are numbered in the order
//! the writer accepts them, and records in flight together share network round trips and disk flushes.
//!
//! A wait limit bounds the wait, not the append. A record that was queued stays queued when the wait ends
//! and may still commit; that is why `BITACORA_STATUS_TIMED_OUT` from an append has to be treated like
//! `BITACORA_STATUS_INDETERMINATE`.
//!
//! Closing is the polite end: it waits for accepted records, seals the segment and gives up the lease, so
//! that the next writer starts without a recovery. Freeing without closing is the abrupt end, the same as
//! a crash as far as the cluster is concerned, and always safe.
//!
//! ```
//! use bitacora_c::{BitacoraStatus, bitacora_writer_append};
//!
//! // Without a writer nothing is appended, and the caller is told which argument was wrong.
//! let payload = b"redo record";
//! let mut commit_sequence = 0_u64;
//! // SAFETY: a null writer is allowed; the payload and the output are live for the call.
//! let status = unsafe {
//!     bitacora_writer_append(std::ptr::null(), payload.as_ptr(), payload.len(), 0, &raw mut commit_sequence)
//! };
//! assert_eq!(status, BitacoraStatus::InvalidArgument);
//! ```
//!
//! # Invariants
//!
//! - A payload is copied before the call returns to waiting: the caller's buffer is never read after the
//!   call returns.
//! - A closed writer accepts nothing; every call on it returns `BITACORA_STATUS_CLOSED` until it is freed.
//! - Closing and freeing need the writer to themselves. Closing while appends are in flight on other
//!   threads is refused instead of being raced.
//!
//! # See also
//!
//! - `client.rs`: where a writer's client comes from, and how calls block.
//! - `status.rs`: what each append outcome becomes.
//! - `docs/protocol/replication.md`: what opening a writer does to the stream.

use std::ffi::c_char;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitacora::{StreamWriter, TakeoverPolicy};
use bytes::Bytes;

use crate::arguments::{
    bytes_argument, free_handle, handle_reference, into_handle_pointer, output_slot, text_argument,
};
use crate::client::{ClientCore, ClientHandle};
use crate::guard::{run_guarded, run_guarded_value};
use crate::status::{BitacoraStatus, Failure};

/// Takeover policy: wait for the previous writer's lease to lapse before taking the stream. This is the
/// safe default: it cannot take a stream from a writer that is alive and well.
pub const BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY: u32 = 0;

/// Takeover policy: take the stream at once. Use it when the caller already knows that the previous
/// writer is gone, for example because its own failover logic decided so.
pub const BITACORA_TAKEOVER_FORCE: u32 = 1;

/// One record of a batch: where its bytes are and how many there are.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Payload {
    /// The first byte of the record. May be null if `length` is 0.
    pub bytes: *const u8,
    /// How many bytes the record has. A record may be empty.
    pub length: usize,
}

/// The writer of one stream.
///
/// Appending and reading the durable sequence may be done from several threads at once with the same
/// writer. Closing and freeing must be done by one thread while no other call on the writer is running.
pub struct WriterHandle {
    /// The writer, until it is closed. Shared with the appends in flight so that the lock is not held
    /// while they wait. Declared before the core so that the writer stops while its threads still exist.
    writer: Mutex<Option<Arc<StreamWriter>>>,
    /// The client and its threads.
    core: Arc<ClientCore>,
}

impl WriterHandle {
    /// Locks the slot that holds the writer.
    ///
    /// A poisoned lock is taken anyway: the slot holds an `Option` that is replaced in one step, so a
    /// panic elsewhere cannot have left it half changed.
    fn lock_writer(&self) -> MutexGuard<'_, Option<Arc<StreamWriter>>> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A share of the writer for one call.
    ///
    /// # Errors
    ///
    /// Returns a closed [`Failure`] if the writer was closed.
    fn open_writer(&self) -> Result<Arc<StreamWriter>, Failure> {
        match self.lock_writer().as_ref() {
            Some(writer) => Ok(Arc::clone(writer)),
            None => Err(Failure::new(BitacoraStatus::Closed, "the writer is closed")),
        }
    }

    /// Appends `payloads` as one unit and waits until they are committed.
    ///
    /// # Returns
    ///
    /// The first and the last sequence number the records were given.
    ///
    /// # Errors
    ///
    /// Returns the [`Failure`] for whatever kept the append from being reported as committed, or a
    /// timed-out one if the wait ended first.
    fn append(&self, payloads: Vec<Bytes>, timeout_milliseconds: u64) -> Result<(u64, u64), Failure> {
        let writer = self.open_writer()?;
        let Some(appended) = self
            .core
            .wait_at_most(timeout_milliseconds, async move { writer.append_batch(payloads).await })
        else {
            return Err(Failure::new(
                BitacoraStatus::TimedOut,
                format!(
                    "the append was not reported as committed within {timeout_milliseconds} ms; its outcome is \
                     not known yet, and the record may still be committed"
                ),
            ));
        };
        let sequence_range = appended?;
        Ok((sequence_range.first().get(), sequence_range.last().get()))
    }

    /// Closes the writer: waits for accepted records, seals the segment and releases the lease.
    ///
    /// # Errors
    ///
    /// Returns a closed [`Failure`] if the writer was closed before, an invalid-argument one if other
    /// calls are using the writer, and otherwise what the close itself reported.
    fn close(&self) -> Result<(), Failure> {
        let mut writer_slot = self.lock_writer();
        let Some(shared_writer) = writer_slot.take() else {
            return Err(Failure::new(BitacoraStatus::Closed, "the writer is closed"));
        };
        let writer = match Arc::try_unwrap(shared_writer) {
            Ok(writer) => writer,
            Err(still_shared_writer) => {
                *writer_slot = Some(still_shared_writer);
                return Err(Failure::invalid_argument(
                    "the writer has calls in flight on other threads; let them return before closing it",
                ));
            }
        };
        drop(writer_slot);
        self.core.wait_for(writer.close())?;
        Ok(())
    }
}

/// Turns the takeover policy C passed into the client's.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] for a number that is not one of the two policies.
fn takeover_policy_from(takeover_policy: u32) -> Result<TakeoverPolicy, Failure> {
    match takeover_policy {
        BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY => Ok(TakeoverPolicy::WaitForLeaseExpiry),
        BITACORA_TAKEOVER_FORCE => Ok(TakeoverPolicy::Force),
        unknown_policy => Err(Failure::invalid_argument(format!(
            "takeover_policy is {unknown_policy}; use BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY or \
             BITACORA_TAKEOVER_FORCE"
        ))),
    }
}

/// Copies the records of a batch out of the caller's memory.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if a record with a length has no bytes.
///
/// # Safety
///
/// Every entry of `payloads` must describe bytes that can be read: `bytes` null only if `length` is 0,
/// otherwise a pointer to `length` readable bytes.
unsafe fn copy_payloads(payloads: &[Payload]) -> Result<Vec<Bytes>, Failure> {
    let mut copied_payloads = Vec::with_capacity(payloads.len());
    for (payload_number, payload) in payloads.iter().enumerate() {
        let argument_name = format!("payloads[{payload_number}].bytes");
        // SAFETY: the caller guarantees that each entry describes readable bytes.
        let payload_bytes = unsafe { bytes_argument(payload.bytes, payload.length, &argument_name) }?;
        copied_payloads.push(Bytes::copy_from_slice(payload_bytes));
    }
    Ok(copied_payloads)
}

/// Becomes the writer of a stream.
///
/// This runs the takeover: it raises the stream's epoch, seals whatever segment the previous writer left
/// open, and opens a new one. When it returns, every record the previous writer committed is in the
/// stream, the previous writer can commit nothing more, and this writer can append. Whoever opens a
/// stream last is its writer; bitacora does not choose which program that should be.
///
/// The call blocks while it waits for a lease to lapse (with `BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY`,
/// about ten seconds after a writer that did not close) and while it recovers the previous segment.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `takeover_policy` - `BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY` or `BITACORA_TAKEOVER_FORCE`.
/// * `writer_out` - Where the new writer is written. It is set to null if the call fails. End the writer
///   with `bitacora_writer_close` and `bitacora_writer_free`.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the name is not UTF-8, or the policy is not
///   one of the two.
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_LEASE_HELD` - A writer that is alive still holds the stream, and the policy was to
///   wait. Try again later.
/// * `BITACORA_STATUS_FENCED` - Another program took the stream while this one was opening it. Open again
///   only if this program should still be the writer.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store or too many replicas could not be reached, or the
///   controller has not placed the stream's next segment. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
/// `writer_out` must be null or point to a pointer that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_open(
    client: *const ClientHandle,
    stream_name: *const c_char,
    takeover_policy: u32,
    writer_out: *mut *mut WriterHandle,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `writer_out` is null or a writable pointer.
        let writer_out = unsafe { output_slot(writer_out, "writer_out") }?;
        writer_out.write(std::ptr::null_mut());
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;
        let takeover_policy = takeover_policy_from(takeover_policy)?;

        let core = client.core();
        let rust_client = core.client().clone();
        let stream_name = stream_name.to_owned();
        let writer = core.wait_for(async move { rust_client.open_writer(&stream_name, takeover_policy).await })?;
        writer_out.write(into_handle_pointer(WriterHandle {
            writer: Mutex::new(Some(Arc::new(writer))),
            core,
        }));
        Ok(())
    })
}

/// Appends one record and waits until it is committed.
///
/// Committed means stored on a write quorum of replicas and flushed to their disks: the record will be
/// seen by every reader, at the sequence number this call reports, and will not be lost. The payload is
/// copied before the wait begins.
///
/// # Arguments
///
/// * `writer` - The writer of the stream.
/// * `payload` - The first byte of the record. May be null if `payload_length` is 0.
/// * `payload_length` - How many bytes the record has, at most 4 MiB.
/// * `timeout_milliseconds` - The longest to wait for the commit, or `BITACORA_WAIT_WITHOUT_LIMIT`.
/// * `commit_sequence_out` - Where the record's sequence number is written on success.
///
/// # Returns
///
/// `BITACORA_STATUS_OK` if the record is committed, or:
///
/// * `BITACORA_STATUS_FENCED` - Another writer has the stream. Stop writing. This record may or may not
///   have been committed.
/// * `BITACORA_STATUS_INDETERMINATE` - The writer lost contact before it learned the outcome. The record
///   may or may not have been committed; read the log to find out.
/// * `BITACORA_STATUS_TIMED_OUT` - The wait ended first. The record is still queued and may be committed
///   later; treat it like `BITACORA_STATUS_INDETERMINATE`.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or the record is larger than 4 MiB. The
///   record was not appended.
/// * `BITACORA_STATUS_CLOSED` - The writer was closed. The record was not appended.
///
/// # Safety
///
/// `writer` must be null or a live writer. `payload` must be null or point to `payload_length` readable
/// bytes. `commit_sequence_out` must be null or point to a `uint64_t` that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_append(
    writer: *const WriterHandle,
    payload: *const u8,
    payload_length: usize,
    timeout_milliseconds: u64,
    commit_sequence_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `writer` is null or a live writer.
        let writer = unsafe { handle_reference(writer, "writer") }?;
        // SAFETY: the caller guarantees `payload_length` readable bytes behind a payload that is not null.
        let payload = unsafe { bytes_argument(payload, payload_length, "payload") }?;
        // SAFETY: the caller guarantees that the output is null or a writable `uint64_t`.
        let commit_sequence_out = unsafe { output_slot(commit_sequence_out, "commit_sequence_out") }?;

        let (commit_sequence, _same_sequence) =
            writer.append(vec![Bytes::copy_from_slice(payload)], timeout_milliseconds)?;
        commit_sequence_out.write(commit_sequence);
        Ok(())
    })
}

/// Appends several records as one unit and waits until they are committed.
///
/// The records get consecutive sequence numbers and are committed together or not at all: no reader ever
/// sees some of them without the others. They travel and are checksummed as one batch, which is also
/// the cheapest way to append many small records. All payloads are copied before the wait begins.
///
/// # Arguments
///
/// * `writer` - The writer of the stream.
/// * `payloads` - The records, in the order they are to be appended.
/// * `payload_count` - How many records there are. At least 1.
/// * `timeout_milliseconds` - The longest to wait for the commit, or `BITACORA_WAIT_WITHOUT_LIMIT`.
/// * `first_sequence_out` - Where the sequence number of the first record is written on success.
/// * `last_sequence_out` - Where the sequence number of the last record is written on success.
///
/// # Returns
///
/// `BITACORA_STATUS_OK` if every record is committed, or a status as for `bitacora_writer_append`, which
/// then holds for all the records together. `BITACORA_STATUS_INVALID_ARGUMENT` is also returned for an
/// empty batch and for a batch larger than 8 MiB.
///
/// # Safety
///
/// `writer` must be null or a live writer. `payloads` must be null or point to `payload_count` entries,
/// each with `bytes` null or pointing to `length` readable bytes. The two outputs must each be null or
/// point to a `uint64_t` that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_append_batch(
    writer: *const WriterHandle,
    payloads: *const Payload,
    payload_count: usize,
    timeout_milliseconds: u64,
    first_sequence_out: *mut u64,
    last_sequence_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `writer` is null or a live writer.
        let writer = unsafe { handle_reference(writer, "writer") }?;
        // SAFETY: the caller guarantees that each output is null or a writable `uint64_t`.
        let first_sequence_out = unsafe { output_slot(first_sequence_out, "first_sequence_out") }?;
        // SAFETY: as above.
        let last_sequence_out = unsafe { output_slot(last_sequence_out, "last_sequence_out") }?;
        if payloads.is_null() && payload_count != 0 {
            return Err(Failure::invalid_argument(format!(
                "payloads is null but payload_count is {payload_count}"
            )));
        }
        let payloads = if payload_count == 0 {
            &[]
        } else {
            // SAFETY: the pointer is not null, so the caller guarantees `payload_count` entries behind it.
            unsafe { std::slice::from_raw_parts(payloads, payload_count) }
        };
        // SAFETY: the caller guarantees that every entry describes readable bytes.
        let copied_payloads = unsafe { copy_payloads(payloads) }?;

        let (first_sequence, last_sequence) = writer.append(copied_payloads, timeout_milliseconds)?;
        first_sequence_out.write(first_sequence);
        last_sequence_out.write(last_sequence);
        Ok(())
    })
}

/// Reports how far the stream is committed, as this writer knows it.
///
/// Every record at or below the reported sequence number is committed. The value never goes back. It is
/// 0 while nothing is committed in a new stream.
///
/// # Arguments
///
/// * `writer` - The writer of the stream.
/// * `durable_sequence_out` - Where the sequence number is written on success.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, `BITACORA_STATUS_INVALID_ARGUMENT` if a pointer is null, or
/// `BITACORA_STATUS_CLOSED` if the writer was closed. The call does not block.
///
/// # Safety
///
/// `writer` must be null or a live writer. `durable_sequence_out` must be null or point to a `uint64_t`
/// that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_durable_sequence(
    writer: *const WriterHandle,
    durable_sequence_out: *mut u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `writer` is null or a live writer.
        let writer = unsafe { handle_reference(writer, "writer") }?;
        // SAFETY: the caller guarantees that the output is null or a writable `uint64_t`.
        let durable_sequence_out = unsafe { output_slot(durable_sequence_out, "durable_sequence_out") }?;

        durable_sequence_out.write(writer.open_writer()?.durable_sequence().get());
        Ok(())
    })
}

/// Closes a writer in good order.
///
/// The writer stops accepting appends, waits for the ones it accepted to commit, seals its segment and
/// gives up its lease. The next writer can then open the stream at once and without a recovery. Whatever
/// this returns, the writer is closed afterwards and must still be freed with `bitacora_writer_free`.
///
/// # Arguments
///
/// * `writer` - The writer to close.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_FENCED` - Another writer took the stream before this one could close.
/// * `BITACORA_STATUS_INDETERMINATE` - Accepted records could not be committed, or the segment could not
///   be sealed, in time. The next writer recovers the stream as after a crash.
/// * `BITACORA_STATUS_CLOSED` - The writer was closed before.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - `writer` is null, or other threads still have calls in flight on
///   this writer. In the second case the writer is still open.
///
/// # Safety
///
/// `writer` must be null or a live writer. No other thread may be inside a call on this writer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_close(writer: *const WriterHandle) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `writer` is null or a live writer.
        let writer = unsafe { handle_reference(writer, "writer") }?;
        writer.close()
    })
}

/// Frees a writer.
///
/// A writer that was not closed is stopped as it is: appends in flight end with
/// `BITACORA_STATUS_INDETERMINATE`, its segment stays open and its lease runs out on its own. That is
/// what a crash looks like to the cluster, and the next writer recovers from it. A null pointer is
/// ignored.
///
/// # Arguments
///
/// * `writer` - The writer to free, or null.
///
/// # Safety
///
/// `writer` must be null or a writer returned by this library that has not been freed. No other call may
/// be using it, and it must not be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_writer_free(writer: *mut WriterHandle) {
    run_guarded_value((), || {
        // SAFETY: the caller guarantees what `free_handle` asks for.
        unsafe { free_handle(writer) };
    });
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod tests;
