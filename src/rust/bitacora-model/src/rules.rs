//! The knobs of the protocol model: which rules the actors follow, and how large a system is explored.
//!
//! # Why this file exists
//!
//! A model checker that reports "no violation" is only worth something if it would have reported one. So
//! the model must be able to run the protocol *wrongly* on purpose, with exactly one rule broken, and show
//! that the checker notices. And an exhaustive check only terminates on a small system, so how small must
//! be stated explicitly and next to the results it produced. This file holds both kinds of knob as plain
//! data, so that a test names what it checks in one line.
//!
//! # What it contains
//!
//! - [`Rules`]: the rules writers and replicas follow. [`Rules::DESIGN`] is the protocol; the other
//!   constants each break one rule.
//! - [`RecoveryEnd`]: which end a recovering writer seals at, one of the rules.
//! - [`Bounds`]: how many records, standby segments and kinds of event a run may have. Three named bounds
//!   cover the three protocols: failover, roll, and one racing the other.
//!
//! # How it works
//!
//! Both types are `Copy` structs of numbers and booleans, read by `transitions.rs` whenever it decides
//! whether a step can happen or what it does. Nothing here has behaviour. A broken variant is written as
//! "the design, except for one field", so the difference from the protocol is the one line that is there.
//!
//! # Invariants
//!
//! - [`Rules::DESIGN`] is the only value under which the model is expected to hold every invariant.
//! - Each broken constant differs from [`Rules::DESIGN`] in exactly one field.
//! - Bounds only limit what is explored. They never change what a step does.
//!
//! # See also
//!
//! - `transitions.rs`: where the rules and bounds are consulted.
//! - `protocol_test.rs`: where each broken rule is shown to be noticed.

use crate::state::{RECOVERY_QUORUM, Sequence, WRITE_QUORUM};

/// Which end a recovering writer seals the previous writer's segment at.
///
/// The design takes the longest copy among the replicas that answered the fence, because a committed
/// record is on at least one of them. The other choice exists to show that the checker notices it.
///
/// # Examples
///
/// ```
/// use bitacora_model::{RecoveryEnd, Rules};
///
/// assert_eq!(Rules::DESIGN.recovery_end, RecoveryEnd::LongestResponder);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryEnd {
    /// The highest local end among the replicas that answered the fence (design, section 9, step 5).
    LongestResponder,
    /// The lowest one. A deliberate mistake: it can cut off committed records.
    ShortestResponder,
}

/// The rules the writers and replicas of the model follow.
///
/// Use [`Rules::DESIGN`] to check the protocol as designed. Every other constant breaks one rule on
/// purpose; checking the model under it must produce a counterexample, which is how the checker itself is
/// tested.
///
/// # Examples
///
/// ```
/// use bitacora_model::{Bounds, CheckOutcome, ProtocolModel, Rules};
///
/// let model_with_a_broken_rule = ProtocolModel {
///     rules: Rules::REPLICAS_IGNORE_FENCING,
///     bounds: Bounds::FAILOVER,
/// };
///
/// let outcome = model_with_a_broken_rule.check(1_000_000);
///
/// assert!(matches!(outcome, CheckOutcome::Violated { .. }));
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rules {
    /// How many replicas must acknowledge a record before its writer reports it as committed. The design
    /// says the write quorum.
    pub commit_quorum: usize,
    /// Whether a replica refuses appends from an epoch below the one it was fenced at. The design says
    /// yes: it is what stops the old writer after a takeover.
    pub replicas_enforce_fencing: bool,
    /// Whether a replica flushes a record to disk before acknowledging it. The design says yes: an
    /// acknowledged record must survive a restart.
    pub replicas_flush_before_acknowledging: bool,
    /// How many replicas a recovering writer must hear from before it chooses the recovery end. The design
    /// says the recovery quorum, which overlaps every write quorum.
    pub recovery_quorum: usize,
    /// Which end the recovering writer seals at. The design says the longest responder's.
    pub recovery_end: RecoveryEnd,
}

impl Rules {
    /// The protocol as the design specifies it.
    pub const DESIGN: Self = Self {
        commit_quorum: WRITE_QUORUM,
        replicas_enforce_fencing: true,
        replicas_flush_before_acknowledging: true,
        recovery_quorum: RECOVERY_QUORUM,
        recovery_end: RecoveryEnd::LongestResponder,
    };

    /// Broken on purpose: a record is reported committed one acknowledgement short of the write quorum.
    pub const COMMITS_BELOW_THE_WRITE_QUORUM: Self = Self {
        commit_quorum: WRITE_QUORUM - 1,
        ..Self::DESIGN
    };

    /// Broken on purpose: fenced replicas keep accepting the old writer.
    pub const REPLICAS_IGNORE_FENCING: Self = Self {
        replicas_enforce_fencing: false,
        ..Self::DESIGN
    };

    /// Broken on purpose: replicas acknowledge records that are still only in memory.
    pub const REPLICAS_ACKNOWLEDGE_BEFORE_FLUSHING: Self = Self {
        replicas_flush_before_acknowledging: false,
        ..Self::DESIGN
    };

    /// Broken on purpose: recovery seals at the shortest copy it finds.
    pub const RECOVERY_TAKES_THE_SHORTEST_RESPONDER: Self = Self {
        recovery_end: RecoveryEnd::ShortestResponder,
        ..Self::DESIGN
    };

    /// Broken on purpose: recovery proceeds after hearing from one replica fewer than the recovery quorum.
    pub const RECOVERY_SKIPS_ITS_QUORUM: Self = Self {
        recovery_quorum: RECOVERY_QUORUM - 1,
        ..Self::DESIGN
    };
}

/// How large a system the model explores.
///
/// An exhaustive check visits every reachable state, and the number of states grows very fast with every
/// record and every kind of event allowed. Larger bounds find deeper interleavings and take longer. Use
/// one of the three named bounds, or start from one and change a field.
///
/// # Examples
///
/// A takeover in a cluster where the controller has placed no standby segment:
///
/// ```
/// use bitacora_model::Bounds;
///
/// let takeover_without_a_standby = Bounds {
///     standby_segments: 0,
///     ..Bounds::FAILOVER
/// };
///
/// assert!(takeover_without_a_standby.recovering_writer_takes_over);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// How many records the first writer appends. Together with `recovering_writer_records` it must stay
    /// below 5, the room a state has for sequence numbers.
    pub first_writer_records: Sequence,
    /// How many records the recovering writer appends after taking over.
    pub recovering_writer_records: Sequence,
    /// How many standby segments the controller has placed. A roll and a takeover each consume one. At
    /// most 2: a state has room for three segments.
    pub standby_segments: u8,
    /// Whether the first writer may roll to a new segment.
    pub first_writer_may_roll: bool,
    /// Whether a second writer may take the stream over.
    pub recovering_writer_takes_over: bool,
    /// Whether replicas may crash and restart.
    pub replicas_may_restart: bool,
}

impl Bounds {
    /// A takeover at any point of two appends, after which the new writer appends a record of its own,
    /// with replica restarts throughout.
    pub const FAILOVER: Self = Self {
        first_writer_records: 2,
        recovering_writer_records: 1,
        standby_segments: 1,
        first_writer_may_roll: false,
        recovering_writer_takes_over: true,
        replicas_may_restart: true,
    };

    /// A roll at any point of three appends by a writer nobody replaces, with replica restarts
    /// throughout.
    pub const ROLL: Self = Self {
        first_writer_records: 3,
        recovering_writer_records: 0,
        standby_segments: 1,
        first_writer_may_roll: true,
        recovering_writer_takes_over: false,
        replicas_may_restart: true,
    };

    /// A roll raced against a takeover: either may happen first, and each writer appends one record.
    /// Replicas do not restart under this bound; the other two cover restarts.
    pub const ROLL_RACING_FAILOVER: Self = Self {
        first_writer_records: 1,
        recovering_writer_records: 1,
        standby_segments: 2,
        first_writer_may_roll: true,
        recovering_writer_takes_over: true,
        replicas_may_restart: false,
    };
}
