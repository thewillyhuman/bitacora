//! Records handed to C: one owned buffer, read through accessors.
//!
//! # Why this file exists
//!
//! A read returns many records of different lengths. C has no type for that, and copying every record
//! into memory the caller allocated would cost a second copy and force the caller to guess sizes. The
//! library keeps the records it read and lends them out instead: the caller gets one handle, asks it for
//! each record's sequence number and bytes, and frees it when it is done.
//!
//! # What it contains
//!
//! - [`RecordsHandle`]: what C holds as `bitacora_records`.
//! - [`bitacora_records_count`], [`bitacora_records_get`], [`bitacora_records_free`].
//!
//! # How it works
//!
//! The handle owns the records exactly as the client returned them. An accessor returns a pointer into
//! that memory, which stays where it is until the handle is freed; nothing is copied.
//!
//! ```
//! use bitacora_c::bitacora_records_count;
//!
//! // No records and no handle look the same from the outside: there is nothing to read.
//! // SAFETY: a null handle is allowed.
//! assert_eq!(unsafe { bitacora_records_count(std::ptr::null()) }, 0);
//! ```
//!
//! # Invariants
//!
//! - Records are in sequence order, as the reader delivered them, and are never changed after the handle
//!   is made.
//! - A payload pointer handed out stays valid until the handle is freed, and not a moment longer.
//!
//! # See also
//!
//! - `reader.rs`: the calls that produce these handles.

use bitacora::LogRecord;

use crate::arguments::{free_handle, handle_reference, output_slot};
use crate::guard::{run_guarded, run_guarded_value};
use crate::status::{BitacoraStatus, Failure};

/// Records returned by one read.
///
/// The accessors may be called from several threads at once with the same handle. Freeing needs the
/// handle to itself.
pub struct RecordsHandle {
    /// The records, in sequence order.
    records: Vec<LogRecord>,
}

impl RecordsHandle {
    /// Wraps the records of one read for handing to C.
    ///
    /// # Arguments
    ///
    /// * `records` - The records, in sequence order.
    ///
    /// # Returns
    ///
    /// The handle that owns them.
    pub(crate) const fn new(records: Vec<LogRecord>) -> Self {
        Self { records }
    }
}

/// Reports how many records a read returned.
///
/// # Arguments
///
/// * `records` - The records of one read, or null.
///
/// # Returns
///
/// The number of records, 0 for a null pointer. Valid indexes for `bitacora_records_get` run from 0 to one
/// below it.
///
/// # Safety
///
/// `records` must be null or records returned by this library that have not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_records_count(records: *const RecordsHandle) -> usize {
    run_guarded_value(0, || {
        // SAFETY: the caller guarantees that `records` is null or live records.
        match unsafe { handle_reference(records, "records") } {
            Ok(records) => records.records.len(),
            Err(_null_handle) => 0,
        }
    })
}

/// Lends out one record of a read: its sequence number and its bytes.
///
/// Nothing is copied. The payload pointer leads into memory the records own.
///
/// # Arguments
///
/// * `records` - The records of one read.
/// * `record_index` - Which record: 0 for the first, up to one below `bitacora_records_count`.
/// * `sequence_out` - Where the record's sequence number is written, or null if it is not wanted.
/// * `payload_out` - Where the pointer to the record's first byte is written. It stays valid until
///   `bitacora_records_free`. Do not write through it, and do not read through it if the length is 0.
/// * `payload_length_out` - Where the number of bytes of the record is written.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or `BITACORA_STATUS_INVALID_ARGUMENT` if a required pointer is null or the index
/// is past the last record.
///
/// # Safety
///
/// `records` must be null or records returned by this library that have not been freed. Each output must
/// be null or point to memory of its type that can be written.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_records_get(
    records: *const RecordsHandle,
    record_index: usize,
    sequence_out: *mut u64,
    payload_out: *mut *const u8,
    payload_length_out: *mut usize,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `records` is null or live records.
        let records = unsafe { handle_reference(records, "records") }?;
        // SAFETY: the caller guarantees that each output is null or writable memory of its type.
        let sequence_out = unsafe { crate::arguments::optional_output_slot(sequence_out) };
        // SAFETY: as above.
        let payload_out = unsafe { output_slot(payload_out, "payload_out") }?;
        // SAFETY: as above.
        let payload_length_out = unsafe { output_slot(payload_length_out, "payload_length_out") }?;

        let Some(record) = records.records.get(record_index) else {
            return Err(Failure::invalid_argument(format!(
                "record_index is {record_index} but there are {} records",
                records.records.len()
            )));
        };
        if let Some(sequence_out) = sequence_out {
            sequence_out.write(record.sequence.get());
        }
        payload_out.write(record.payload.as_ptr());
        payload_length_out.write(record.payload.len());
        Ok(())
    })
}

/// Frees the records of a read. Every payload pointer obtained from them becomes invalid. A null pointer
/// is ignored.
///
/// # Arguments
///
/// * `records` - The records to free, or null.
///
/// # Safety
///
/// `records` must be null or records returned by this library that have not been freed. No other call may
/// be using them, and neither they nor any pointer obtained from them may be used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_records_free(records: *mut RecordsHandle) {
    run_guarded_value((), || {
        // SAFETY: the caller guarantees what `free_handle` asks for.
        unsafe { free_handle(records) };
    });
}

#[cfg(test)]
#[path = "records_test.rs"]
mod tests;
