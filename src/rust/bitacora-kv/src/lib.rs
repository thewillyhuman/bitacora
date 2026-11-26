//! bitacora-kv: a small versioned key-value store on one stream.
//!
//! # Why this crate exists
//!
//! bitacora-kv is the reference materializer (design, section 12). It is not meant to compete with real
//! databases. It exists for two reasons: to keep the materializer framework honest, by being a real
//! workload built on the page adapter, and to give the test suite something whose correctness can be
//! checked mechanically, because "a key has the value last written to it" can be judged from the outside.
//!
//! # What it contains
//!
//! - [`KvStore`]: the front end. `put`, `delete`, `write`, `get`, `get_as_of`, `scan`.
//! - [`KvRecord`] and [`KvOperation`]: the log records, and their bytes.
//! - [`KvPages`], [`bucket_of`], [`decode_bucket`], [`value_in_bucket`]: keys on bucket pages.
//! - [`KvDependencies`], [`KvSettings`], [`KvError`], [`KV_MATERIALIZER`].
//!
//! # How it works
//!
//! ```text
//! put / delete --> KvRecord --> StreamWriter --> log
//!                                                 |
//!                                    MaterializerRunner<PageStore<KvPages>>
//!                                                 |
//! get ---- barrier record, then read as of it --> bucket pages
//! ```
//!
//! A store is the writer of its stream. Every change is one record of the stream. A materializer follows the stream
//! and keeps the keys on 64 bucket pages, of which the page adapter keeps every version. A read looks a key up in
//! its bucket as of a sequence number: one the caller names, or, for the latest value, the sequence of a barrier
//! record the read appends first, which proves that the store was still the writer.
//!
//! # Invariants
//!
//! - A value returned by [`KvStore::get`] was the committed value of the key at some moment between the call and
//!   its return.
//! - A write reported as done is in the log once, at the returned sequence number.
//! - Reads are answered only from records the log has committed.
//! - The record format, the bucket count and the hash never change for an existing store.
//! - The crate depends on `bitacora-types`, `bitacora-artifact`, `bitacora-meta`, `bitacora-client` and
//!   `bitacora-materializer` only.
//!
//! # See also
//!
//! - `README.md` in this crate: the design, and which file holds what.
//! - `docs/decisions/0004-materializer-framework.md`: how reads are made linearizable.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod pages;
mod record;
mod store;

pub use pages::{BUCKET_COUNT, DamagedBucket, KvPages, bucket_of, decode_bucket, value_in_bucket};
pub use record::{KvOperation, KvRecord, KvRecordError};
pub use store::{KV_MATERIALIZER, KvDependencies, KvError, KvSettings, KvStore};
