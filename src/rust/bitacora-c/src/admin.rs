//! Stream administration from C: creating, describing, truncating and deleting streams.
//!
//! # Why this file exists
//!
//! A program that embeds the log has to create its stream before it can write to it, find out where the
//! stream stands when it starts, say how much of the log it no longer needs, and remove the stream when
//! the database it belongs to is dropped. Those are calls to the metadata store, not to replicas, and
//! they are rare. This file gives them to C.
//!
//! # What it contains
//!
//! - [`StreamOptions`]: how a new stream is replicated and retained, as `bitacora_stream_options`.
//! - [`StreamDescription`]: where a stream stands, as `bitacora_stream_description`.
//! - [`bitacora_stream_create`], [`bitacora_stream_describe`], [`bitacora_stream_truncate_before`],
//!   [`bitacora_stream_delete`], [`bitacora_stream_register_materializer`],
//!   [`bitacora_stream_unregister_materializer`].
//!
//! # How it works
//!
//! Each function is one call of the Rust client's stream administration, made on the client's threads
//! while the caller waits. Both structures carry their own size, so that fields can be added later: a
//! program compiled against today's header keeps working with a newer library, which fills only the
//! fields that program knows.
//!
//! A description is a snapshot. The stream moves on while the caller looks at it.
//!
//! ```
//! use bitacora_c::{BitacoraStatus, bitacora_stream_delete};
//!
//! // Without a client nothing is asked of the cluster.
//! // SAFETY: a null client is allowed, and the name is NUL-terminated.
//! let status = unsafe { bitacora_stream_delete(std::ptr::null(), c"orders".as_ptr()) };
//! assert_eq!(status, BitacoraStatus::InvalidArgument);
//! ```
//!
//! # Invariants
//!
//! - Nothing here is on the append path: these calls may be slow or fail while appends go on.
//! - Every call is safe to repeat. A call that failed as unavailable may or may not have taken effect, and
//!   doing it again leaves the same result.
//!
//! # See also
//!
//! - `client.rs`: the client these calls are made through.
//! - `docs/protocol/metadata.md`: the stream record a description is taken from.

use std::ffi::c_char;
use std::time::Duration;

use bitacora::{SequenceNumber, StreamDescription as ClientStreamDescription, StreamOptions as ClientStreamOptions};
use bitacora_meta::RetentionPolicy;
use bitacora_types::ReplicationSettings;

use crate::arguments::{SizedStructure, handle_reference, read_sized_structure, sized_output_slot, text_argument};
use crate::client::ClientHandle;
use crate::guard::run_guarded;
use crate::status::{BitacoraStatus, Failure};

/// How a new stream is replicated and how long it keeps records nothing needs. Pass a null pointer to
/// take every default: three replicas, two of which must store a record before it is committed, and no
/// minimum retention.
///
/// Set `struct_size` to `sizeof(bitacora_stream_options)` and every field that is not wanted to zero.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
    /// `sizeof(bitacora_stream_options)`, as the caller's compiler knows the structure.
    pub struct_size: u32,
    /// How many replicas hold each segment of the stream. 0, together with a `write_quorum` of 0, takes
    /// the default of 3.
    pub replica_count: u8,
    /// How many replicas must store a record before it is committed, from 1 to `replica_count`. 0,
    /// together with a `replica_count` of 0, takes the default of 2.
    pub write_quorum: u8,
    /// Not used. Set to 0.
    pub reserved: [u8; 2],
    /// Keep every sealed segment at least this many seconds, for point-in-time recovery. 0 keeps nothing
    /// longer than truncation allows.
    pub keep_sealed_for_seconds: u64,
}

// SAFETY: `StreamOptions` is `#[repr(C)]`, begins with the `u32` size, consists only of integers, and its
// fields fill it without padding: 4 + 1 + 1 + 2 + 8 bytes.
unsafe impl SizedStructure for StreamOptions {
    fn with_defaults() -> Self {
        Self {
            struct_size: 0,
            replica_count: 0,
            write_quorum: 0,
            reserved: [0; 2],
            keep_sealed_for_seconds: 0,
        }
    }
}

/// Where a stream stands: a snapshot of what the metadata store knows about it.
///
/// Set `struct_size` to `sizeof(bitacora_stream_description)` before the call. The library fills the
/// fields that fit and sets `struct_size` to the number of bytes it wrote.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamDescription {
    /// In: `sizeof(bitacora_stream_description)`. Out: how many bytes were filled.
    pub struct_size: u32,
    /// How many segments the stream has, the open one included.
    pub segment_count: u32,
    /// The stream's identifier. It is never reused, not even by a later stream of the same name.
    pub stream_id: u64,
    /// The stream's epoch: how many times a writer has taken it.
    pub epoch: u64,
    /// The first sequence number that can still be read.
    pub low_water_mark: u64,
    /// The last sequence number in a sealed segment, or 0 if no segment is sealed. Records after it are
    /// in the open segment; how many of those are committed is known to the writer and the replicas, not
    /// to the metadata store.
    pub last_sealed_sequence: u64,
    /// How many materializers are registered. Each one keeps the log from being truncated past its
    /// checkpoint.
    pub materializer_count: u32,
    /// How many replicas hold each segment.
    pub replica_count: u8,
    /// How many replicas must store a record before it is committed.
    pub write_quorum: u8,
    /// 1 if a writer holds the stream's lease, 0 if not. A lease that has lapsed counts as held until the
    /// next writer replaces it.
    pub has_writer: u8,
    /// Not used. Always 0.
    pub reserved: u8,
}

// SAFETY: `StreamDescription` is `#[repr(C)]`, begins with the `u32` size, consists only of integers, and
// its fields fill it without padding: 4 + 4 + 8 + 8 + 8 + 8 + 4 + 1 + 1 + 1 + 1 bytes.
unsafe impl SizedStructure for StreamDescription {
    fn with_defaults() -> Self {
        Self {
            struct_size: 0,
            segment_count: 0,
            stream_id: 0,
            epoch: 0,
            low_water_mark: 0,
            last_sealed_sequence: 0,
            materializer_count: 0,
            replica_count: 0,
            write_quorum: 0,
            has_writer: 0,
            reserved: 0,
        }
    }
}

/// Turns the options C passed into the client's.
///
/// # Errors
///
/// Returns an invalid-argument [`Failure`] if the replica count and the write quorum do not go together.
fn client_stream_options(options: StreamOptions) -> Result<ClientStreamOptions, Failure> {
    let takes_default_replication = options.replica_count == 0 && options.write_quorum == 0;
    let replication = if takes_default_replication {
        ReplicationSettings::DEFAULT
    } else {
        match ReplicationSettings::new(options.replica_count, options.write_quorum) {
            Ok(replication) => replication,
            Err(invalid_replication) => {
                return Err(Failure::invalid_argument(format!("options: {invalid_replication}")));
            }
        }
    };
    let retention = if options.keep_sealed_for_seconds == 0 {
        RetentionPolicy::default()
    } else {
        RetentionPolicy::keep_for(Duration::from_secs(options.keep_sealed_for_seconds))
    };
    Ok(ClientStreamOptions { replication, retention })
}

/// Picks out of the client's description what the C structure reports.
fn stream_description(description: &ClientStreamDescription) -> StreamDescription {
    let stream = &description.stream;
    let mut last_sealed_sequence = SequenceNumber::ZERO;
    for segment in &stream.segments {
        if let Some(sealed_end) = segment.sealed_end() {
            last_sealed_sequence = last_sealed_sequence.max(sealed_end);
        }
    }
    StreamDescription {
        struct_size: 0,
        segment_count: u32::try_from(stream.segments.len()).unwrap_or(u32::MAX),
        stream_id: stream.stream_id.get(),
        epoch: stream.epoch.get(),
        low_water_mark: stream.low_water_mark.get(),
        last_sealed_sequence: last_sealed_sequence.get(),
        materializer_count: u32::try_from(stream.materializers.len()).unwrap_or(u32::MAX),
        replica_count: u8::try_from(stream.replication.replica_count()).unwrap_or(u8::MAX),
        write_quorum: u8::try_from(stream.replication.write_quorum()).unwrap_or(u8::MAX),
        has_writer: u8::from(stream.writer.is_some()),
        reserved: 0,
    }
}

/// Creates a stream.
///
/// The stream is empty and has no writer. Its first segment is placed on replicas at once if enough are
/// registered, and by the controller as soon as there are, so a writer can usually open it right away.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the new stream: 1 to 255 ASCII letters, digits, `.`, `_` or `-`,
///   NUL-terminated.
/// * `options` - How the stream is replicated and retained, or null for every default.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_ALREADY_EXISTS` - A stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the name is not allowed, or the options do
///   not go together.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
/// `options` must be null or point to a `bitacora_stream_options` with `struct_size` set.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_create(
    client: *const ClientHandle,
    stream_name: *const c_char,
    options: *const StreamOptions,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;
        // SAFETY: the caller guarantees that options which are not null are a sized structure.
        let options = unsafe { read_sized_structure(options, "options") }?;
        let options = client_stream_options(options)?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        core.wait_for(async move { admin.create_stream(&stream_name, options).await })?;
        Ok(())
    })
}

/// Reports where a stream stands.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `description_out` - The structure to fill. Set its `struct_size` to
///   `sizeof(bitacora_stream_description)` before the call.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or `struct_size` was not set.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
/// `description_out` must be null or point to a `bitacora_stream_description` with `struct_size` set,
/// writable for that many bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_describe(
    client: *const ClientHandle,
    stream_name: *const c_char,
    description_out: *mut StreamDescription,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;
        // SAFETY: the caller guarantees that a description which is not null is a sized structure that
        // can be written for as many bytes as it declares.
        let description_out = unsafe { sized_output_slot(description_out, "description_out") }?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        let description = core.wait_for(async move { admin.describe_stream(&stream_name).await })?;
        description_out.write(stream_description(&description));
        Ok(())
    })
}

/// Allows every record below a sequence number to be deleted.
///
/// This is how the owner of a stream says what it no longer needs. Records are deleted by whole segments,
/// later, by the controller, and only as far as the checkpoints of the registered materializers and the
/// retention of the stream allow. The low-water mark follows; read it with
/// `bitacora_reader_low_water_mark`. The position only moves forward: asking for an earlier one changes
/// nothing.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `sequence` - The first sequence number that must be kept.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_truncate_before(
    client: *const ClientHandle,
    stream_name: *const c_char,
    sequence: u64,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        core.wait_for(async move { admin.truncate_before(&stream_name, SequenceNumber::new(sequence)).await })?;
        Ok(())
    })
}

/// Deletes a stream and everything in it.
///
/// The stream is gone at once for writers and readers. Its segments on replicas and in the object store
/// are removed afterwards by the controller. A stream created later under the same name is a new stream.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_delete(
    client: *const ClientHandle,
    stream_name: *const c_char,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        core.wait_for(async move { admin.delete_stream(&stream_name).await })?;
        Ok(())
    })
}

/// Registers a materializer on a stream.
///
/// From then on the log keeps every record above the materializer's checkpoint. A registered
/// materializer that stops checkpointing therefore keeps the whole log from being truncated, until it is
/// unregistered. Registering one that is registered already changes nothing.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `materializer` - The name of the materializer: 1 to 255 ASCII letters, digits, `.`, `_` or `-`,
///   NUL-terminated.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or the materializer's name is not allowed.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` and `materializer` must each be null or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_register_materializer(
    client: *const ClientHandle,
    stream_name: *const c_char,
    materializer: *const c_char,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;
        // SAFETY: as above.
        let materializer = unsafe { text_argument(materializer, "materializer") }?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        let materializer = materializer.to_owned();
        core.wait_for(async move { admin.register_materializer(&stream_name, &materializer).await })?;
        Ok(())
    })
}

/// Unregisters a materializer, so that it no longer keeps the log from being truncated.
///
/// Unregistering one that is not registered changes nothing.
///
/// # Arguments
///
/// * `client` - The client of the cluster.
/// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
/// * `materializer` - The name of the materializer, NUL-terminated UTF-8.
///
/// # Returns
///
/// `BITACORA_STATUS_OK`, or:
///
/// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
/// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
/// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
///
/// # Safety
///
/// `client` must be null or a live client. `stream_name` and `materializer` must each be null or a
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bitacora_stream_unregister_materializer(
    client: *const ClientHandle,
    stream_name: *const c_char,
    materializer: *const c_char,
) -> BitacoraStatus {
    run_guarded(|| {
        // SAFETY: the caller guarantees that `client` is null or a live client.
        let client = unsafe { handle_reference(client, "client") }?;
        // SAFETY: the caller guarantees that the name is null or a NUL-terminated string.
        let stream_name = unsafe { text_argument(stream_name, "stream_name") }?;
        // SAFETY: as above.
        let materializer = unsafe { text_argument(materializer, "materializer") }?;

        let core = client.core();
        let admin = core.client().admin();
        let stream_name = stream_name.to_owned();
        let materializer = materializer.to_owned();
        core.wait_for(async move { admin.unregister_materializer(&stream_name, &materializer).await })?;
        Ok(())
    })
}

#[cfg(test)]
#[path = "admin_test.rs"]
mod tests;
