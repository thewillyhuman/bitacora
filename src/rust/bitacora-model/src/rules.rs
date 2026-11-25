//! The knobs of the protocol model: which rules the actors follow, and how
//! large a system is explored.

use crate::state::{RECOVERY_QUORUM, Sequence, WRITE_QUORUM};

/// Which end a recovering writer seals the previous writer's segment at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryEnd {
    /// The highest local end among the replicas that answered the fence
    /// (design, section 9, step 5).
    LongestResponder,
    /// The lowest one. A deliberate mistake: it can cut off committed
    /// records.
    ShortestResponder,
}

/// The rules the writers and replicas of the model follow.
///
/// [`Rules::DESIGN`] is the protocol as designed. Every other value breaks
/// one rule on purpose, to show that the model checker would notice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rules {
    /// How many replicas must acknowledge a record before its writer
    /// reports it as committed.
    pub commit_quorum: usize,
    /// Whether a replica refuses appends from an epoch below the one it was
    /// fenced at.
    pub replicas_enforce_fencing: bool,
    /// Whether a replica flushes a record to disk before acknowledging it.
    pub replicas_flush_before_acknowledging: bool,
    /// How many replicas a recovering writer must hear from before it
    /// chooses the recovery end.
    pub recovery_quorum: usize,
    /// Which end the recovering writer seals at.
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

    /// Broken on purpose: a record is reported committed one
    /// acknowledgement short of the write quorum.
    pub const COMMITS_BELOW_THE_WRITE_QUORUM: Self = Self {
        commit_quorum: WRITE_QUORUM - 1,
        ..Self::DESIGN
    };

    /// Broken on purpose: fenced replicas keep accepting the old writer.
    pub const REPLICAS_IGNORE_FENCING: Self = Self {
        replicas_enforce_fencing: false,
        ..Self::DESIGN
    };

    /// Broken on purpose: replicas acknowledge records still in memory.
    pub const REPLICAS_ACKNOWLEDGE_BEFORE_FLUSHING: Self = Self {
        replicas_flush_before_acknowledging: false,
        ..Self::DESIGN
    };

    /// Broken on purpose: recovery seals at the shortest copy it finds.
    pub const RECOVERY_TAKES_THE_SHORTEST_RESPONDER: Self = Self {
        recovery_end: RecoveryEnd::ShortestResponder,
        ..Self::DESIGN
    };

    /// Broken on purpose: recovery proceeds after hearing from one replica
    /// fewer than the recovery quorum.
    pub const RECOVERY_SKIPS_ITS_QUORUM: Self = Self {
        recovery_quorum: RECOVERY_QUORUM - 1,
        ..Self::DESIGN
    };
}

/// How large a system the model explores. Larger bounds find deeper
/// interleavings and take longer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// How many records the first writer appends.
    pub first_writer_records: Sequence,
    /// How many records the recovering writer appends after taking over.
    pub recovering_writer_records: Sequence,
    /// How many standby segments the controller has placed. A roll and a
    /// takeover each consume one.
    pub standby_segments: u8,
    /// Whether the first writer may roll to a new segment.
    pub first_writer_may_roll: bool,
    /// Whether a second writer may take the stream over.
    pub recovering_writer_takes_over: bool,
    /// Whether replicas may crash and restart.
    pub replicas_may_restart: bool,
}

impl Bounds {
    /// A takeover at any point of two appends, after which the new writer
    /// appends a record of its own, with replica restarts throughout.
    pub const FAILOVER: Self = Self {
        first_writer_records: 2,
        recovering_writer_records: 1,
        standby_segments: 1,
        first_writer_may_roll: false,
        recovering_writer_takes_over: true,
        replicas_may_restart: true,
    };

    /// A roll at any point of three appends by a writer nobody replaces,
    /// with replica restarts throughout.
    pub const ROLL: Self = Self {
        first_writer_records: 3,
        recovering_writer_records: 0,
        standby_segments: 1,
        first_writer_may_roll: true,
        recovering_writer_takes_over: false,
        replicas_may_restart: true,
    };

    /// A roll raced against a takeover: either may happen first, and each
    /// writer appends one record.
    pub const ROLL_RACING_FAILOVER: Self = Self {
        first_writer_records: 1,
        recovering_writer_records: 1,
        standby_segments: 2,
        first_writer_may_roll: true,
        recovering_writer_takes_over: true,
        replicas_may_restart: false,
    };
}
