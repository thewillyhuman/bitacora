//! An executable model of bitacora's append, roll and failover protocols, checked exhaustively.
//!
//! # Why this crate exists
//!
//! The failover protocol is new code that has to earn trust, and the cheapest place to find an expensive
//! bug in it is before the bug is implemented. The design asks for the protocol to be written down as an
//! executable model and checked against its invariants over every interleaving, with crashes and message
//! loss, before and alongside the implementation (section 15, layer 1; exit gate of milestone M0). This
//! crate is that model and its checker.
//!
//! It depends on no implementation crate on purpose, so that a mistake in one cannot hide a mistake in the
//! other.
//!
//! # What it contains
//!
//! - [`ProtocolModel`]: the protocol under a set of [`Rules`], within some [`Bounds`].
//! - [`ProtocolModel::check`]: explores every reachable state and reports a [`CheckOutcome`]: the
//!   invariants hold, or here is a shortest [`Counterexample`].
//! - [`Rules`]: the protocol as designed ([`Rules::DESIGN`]) and five variants broken on purpose, which the
//!   tests use to show that the checker finds a counterexample when there is one.
//! - [`Invariant`]: I1 to I4 of the design.
//! - [`Action`] and [`State`]: the steps and states a counterexample is made of.
//!
//! # How it works
//!
//! The model describes one stream with three replicas and a write quorum of two, a metadata store that
//! only changes through conditional writes, a first writer that appends and may roll, and a recovering
//! writer that runs the eight failover steps and then appends its own records. Messages can be lost,
//! delayed, duplicated and reordered; replicas can restart; writers can stop between any two steps.
//!
//! A small breadth-first explorer visits every state that system can reach and evaluates the invariants in
//! each. What the model covers, what it abstracts away and what it found are in
//! `docs/decisions/0001-protocol-model.md`.
//!
//! # Invariants
//!
//! - The crate has no dependencies, in the workspace or outside it.
//! - A check is deterministic: no threads, no hashing, no randomness.
//! - A change to a protocol step or to an invariant is made here first, then in the implementation.
//!
//! # See also
//!
//! - `protocol.rs`: the model as a whole and its check.
//! - `state.rs`, `transitions.rs`, `invariants.rs`: what a state is, which steps exist, what must hold.
//! - `rules.rs`: the rules and the bounds.
//! - `explorer.rs`: the search.

#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]
#![warn(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![warn(rustdoc::broken_intra_doc_links)]

mod explorer;
mod invariants;
mod protocol;
mod rules;
mod state;
mod transitions;

pub use invariants::Invariant;
pub use protocol::{CheckOutcome, Counterexample, ProtocolModel};
pub use rules::{Bounds, RecoveryEnd, Rules};
pub use state::{Epoch, FIRST_EPOCH, RECOVERY_QUORUM, REPLICA_COUNT, SECOND_EPOCH, Sequence, State, WRITE_QUORUM};
pub use transitions::Action;
