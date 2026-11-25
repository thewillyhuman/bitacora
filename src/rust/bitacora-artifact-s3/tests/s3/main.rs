//! The S3 store against a real S3 server.
//!
//! # What is covered
//!
//! - `contract.rs`: the behaviour every artifact store must have, on a real server.
//! - `objects.rs`: what only a real server can show: ranges at the edges of a large object, keys with
//!   unusual characters, key prefixes sharing one bucket.
//! - `failures.rs`: what an operator gets wrong or what breaks: a bucket that does not exist, wrong
//!   credentials, a server that is down, one that stops, one that never answers.
//!
//! # How the fixtures work
//!
//! `s3_server.rs` starts the Versity S3 Gateway on a free local port, with its data in the test's temporary
//! directory, and stops it when the last test using it ends. Bazel fetches the pinned program and passes
//! its path in `BITA_S3_SERVER_BINARY`. Under Cargo the variable is set by hand; when it is not, every test
//! here passes without running and says so once.
//!
//! Every test starts with the same two lines: get the shared server or return, then open a store under a
//! key prefix of its own.

mod contract;
mod failures;
mod objects;
mod s3_server;
