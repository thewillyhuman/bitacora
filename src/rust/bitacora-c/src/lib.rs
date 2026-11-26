//! The C interface of the bitacora client.
//!
//! # Why this crate exists
//!
//! The write protocol of bitacora lives in the client library: assigning sequence numbers, replicating
//! to a quorum, rolling segments, taking over a stream. A database that wants to use the log has to link
//! that library, and most databases are not written in Rust. PostgreSQL is the first one the design has
//! in mind. Writing the protocol a second time in C would mean two implementations of the part of the
//! system where a mistake loses data. This crate gives C the one implementation there is, behind
//! functions a C program can call.
//!
//! # What it contains
//!
//! - Handles, all opaque to C: [`ClientHandle`], [`WriterHandle`], [`ReaderHandle`], [`FollowerHandle`],
//!   [`RecordsHandle`].
//! - Connecting: [`bitacora_client_connect_file`], [`bitacora_client_connect_text`].
//! - Writing: [`bitacora_writer_open`], [`bitacora_writer_append`], [`bitacora_writer_append_batch`],
//!   [`bitacora_writer_durable_sequence`], [`bitacora_writer_close`].
//! - Reading: [`bitacora_reader_open`], [`bitacora_reader_read_range`], [`bitacora_reader_follow`],
//!   [`bitacora_follower_next`], [`bitacora_reader_low_water_mark`], and the accessors of the records.
//! - Stream administration: [`bitacora_stream_create`], [`bitacora_stream_describe`],
//!   [`bitacora_stream_truncate_before`], [`bitacora_stream_delete`], and the two for materializers.
//! - [`BitacoraStatus`], what every call returns, and [`bitacora_last_error_message`].
//! - [`bitacora_version`] and [`bitacora_abi_version`].
//!
//! The header that declares all of it to C is `src/c/include/bitacora.h`. It is generated from this
//! crate's source and checked in; a test fails when the two differ.
//!
//! # How it works
//!
//! The Rust client is asynchronous; the C interface blocks. Each client owns a small Tokio runtime. A
//! call hands its work to the runtime's threads and blocks the calling thread until the answer is there.
//! The calling thread never runs the client's code, and nothing ever calls back into the caller's code,
//! so there are no rules about what a callback may do and no thread of the library that the caller did
//! not see being started by its own connect.
//!
//! Every function checks its pointers, catches panics before they could reach C, and reports what
//! happened as a status code with a message kept per thread. The documentation of each function is
//! written for a C programmer, because it is what the header shows.
//!
//! This is the only crate of the workspace that contains `unsafe` code. It cannot be avoided where
//! pointers arrive from C. The ways a pointer is checked and turned into something safe are all in
//! `arguments.rs`. Outside it there are the lines of each entry point that call those, and the three
//! structures that vouch for their own layout. Every use says what the caller of the C function
//! guarantees.
//!
//! # Invariants
//!
//! - No panic unwinds into C, and no pointer is dereferenced before it was checked for null.
//! - Every case of the client's errors has one status, assigned in `status.rs`.
//! - A handle is freed by exactly one call of its `free` function. The library cannot detect a second
//!   one, or a use after it; no C library can.
//! - This crate adds no behaviour to the protocol. What a call does is what the Rust client does.
//!
//! # See also
//!
//! - `status.rs`: the status codes and the mapping of errors.
//! - `guard.rs`, `arguments.rs`, `last_error.rs`: the boundary every call passes through.
//! - `client.rs`, `writer.rs`, `reader.rs`, `records.rs`, `admin.rs`, `version.rs`: the functions.
//! - `docs/clients/c.md`: how to build against the library and how to use it.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]
// The workspace denies `unsafe` code. This crate is where C calls in: its arguments are raw pointers, and
// a raw pointer cannot be used without `unsafe`. Every block states what makes it sound.
#![allow(unsafe_code)]

mod admin;
mod arguments;
mod client;
mod guard;
mod last_error;
mod reader;
mod records;
mod status;
#[cfg(test)]
mod test_support;
mod version;
mod writer;

pub use admin::{
    StreamDescription, StreamOptions, bitacora_stream_create, bitacora_stream_delete, bitacora_stream_describe,
    bitacora_stream_register_materializer, bitacora_stream_truncate_before, bitacora_stream_unregister_materializer,
};
pub use client::{
    BITACORA_WAIT_WITHOUT_LIMIT, ClientHandle, ClientOptions, bitacora_client_connect_file,
    bitacora_client_connect_text, bitacora_client_free,
};
pub use last_error::bitacora_last_error_message;
pub use reader::{
    FollowerHandle, ReaderHandle, bitacora_follower_free, bitacora_follower_next, bitacora_reader_follow,
    bitacora_reader_free, bitacora_reader_low_water_mark, bitacora_reader_open, bitacora_reader_read_range,
};
pub use records::{RecordsHandle, bitacora_records_count, bitacora_records_free, bitacora_records_get};
pub use status::BitacoraStatus;
pub use version::{BITACORA_ABI_VERSION, bitacora_abi_version, bitacora_version};
pub use writer::{
    BITACORA_TAKEOVER_FORCE, BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY, Payload, WriterHandle, bitacora_writer_append,
    bitacora_writer_append_batch, bitacora_writer_close, bitacora_writer_durable_sequence, bitacora_writer_free,
    bitacora_writer_open,
};
