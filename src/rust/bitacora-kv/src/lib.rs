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
//! # How it fits together
//!
//! ```text
//! put / delete --> KvRecord --> StreamWriter --> log
//!                                                 |
//!                                    MaterializerRunner<PageStore<KvPages>>
//!                                                 |
//! get ---- barrier record, then read as of it --> bucket pages
//! ```
//!
//! # See also
//!
//! The `README.md` of this crate, and `docs/decisions/0004-materializer-framework.md` for how reads are
//! made linearizable.

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
