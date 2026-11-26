//! Tests of the etcd backend against a real etcd.
//!
//! # Why these tests exist
//!
//! The store is a thin adapter, so almost everything that can be wrong with it is an assumption about etcd: what a
//! revision is, what a comparison on a missing key does, how a dead server shows up. A fake etcd would only repeat
//! those assumptions. These tests therefore start the real program and talk to it over the loopback interface.
//!
//! # What is covered
//!
//! - `key_value_contract.rs`: the behaviours every backend must have, the same ones the in-memory store is tested for,
//!   plus the corners that are specific to etcd.
//! - `metadata_store.rs`: a representative set of the typed operations of `bitacora_meta::MetadataStore`, to show both
//!   backends behave the same under the layer that everything else uses.
//! - `unavailability.rs`: what a caller sees when etcd is unreachable, gone or silent.
//!
//! # How the fixtures work
//!
//! `etcd_process.rs` starts etcd from the program named by the environment variable `BITA_ETCD_BINARY`. Bazel sets it
//! to the release pinned in `MODULE.bazel`. Under Cargo it is set by hand; when it is not, the tests that need etcd
//! pass without running and say so once on standard error.
//!
//! All tests of this binary share one etcd, and each works under a key prefix of its own, so they run in parallel
//! without seeing each other. `writes.rs` holds the shorthand tests use to put data in place.

mod etcd_process;
mod key_value_contract;
mod metadata_store;
mod unavailability;
mod writes;
