//! Fixtures shared by the tests of this crate.
//!
//! # Why this file exists
//!
//! The tests of the checker are spread over one file per invariant, and all of them judge histories written by hand.
//! The tests of the nemesis are spread over one file per kind of fault, and all of them need a small world to break and
//! a way to read what was recorded about it. Written once per test file, these fixtures would drift apart. Here they
//! are written once.
//!
//! # What it contains
//!
//! - [`Script`]: a history written by hand, one append or one reading session at a time, the way a broken log would
//!   produce it. [`committed`] is the outcome of an append that was reported committed.
//! - [`start_world`]: a small world with nothing running in it but the cluster.
//! - [`fault_descriptions`]: what the simulator recorded that it did to the cluster.
//! - [`store_one_record_on`] and [`try_to_store_one_record_on`]: make a node hold a segment file, for the faults that
//!   damage stored data, or show that its disk refuses to.
//!
//! # How it works
//!
//! A script hands out operation identifiers as a history does, and stamps every event with time zero: the checkers
//! order events by their position, never by their time.
//!
//! The worlds have three nodes, the smallest cluster a stream with the default replication can live on. They run under
//! the paused runtime of the test that starts them, so a test that advances time by a fault's duration sees the fault
//! undone exactly then.
//!
//! # Invariants
//!
//! - Fixtures build values; they assert nothing about the behaviour under test. An `expect` in a fixture says why the
//!   step cannot fail in a healthy fixture.
//! - This module exists in test builds only.
//!
//! # See also
//!
//! - The `_test.rs` file next to each source file: the tests that use these fixtures.

use std::sync::Arc;
use std::time::Duration;

use bitacora_segment::format::Batch;
use bitacora_types::replica_api::{AppendRequest, ReplicaApi, ReplicaError, SegmentDescriptor};
use bitacora_types::{DeterministicRandom, Epoch, SegmentAddress, SegmentId, SequenceNumber, StreamId};
use bytes::Bytes;

use crate::history::{AppendOutcome, Event, History, TimedEvent};
use crate::world::World;

/// How many storage nodes the worlds of the tests have.
pub(crate) const WORLD_NODE_COUNT: usize = 3;

/// A segment no stream of the world owns, for tests that only need a node to hold a segment file.
const STRAY_SEGMENT: SegmentAddress = SegmentAddress::new(StreamId::new(900), SegmentId::new(1));

/// The epoch the stray segment is written in.
const STRAY_SEGMENT_EPOCH: Epoch = Epoch::new(1);

/// A history written by hand, the way a broken log would produce it.
#[derive(Default)]
pub(crate) struct Script {
    /// The events so far.
    pub(crate) events: Vec<TimedEvent>,
    /// The identifier the last append got.
    last_operation: u64,
}

impl Script {
    /// Appends an event to the script.
    ///
    /// # Arguments
    ///
    /// * `event` - The event. It is stamped with time zero; the checkers order events by position.
    pub(crate) fn push(&mut self, event: Event) {
        self.events.push(TimedEvent {
            at: Duration::ZERO,
            event,
        });
    }

    /// Adds an append of `payload` that ends with `outcome`.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload of the record, unique within the script.
    /// * `outcome` - What the caller was told, or `None` for a caller that crashed before any answer.
    ///
    /// # Returns
    ///
    /// The identifier of the append.
    pub(crate) fn append(&mut self, payload: &'static str, outcome: Option<AppendOutcome>) -> u64 {
        self.last_operation += 1;
        let operation = self.last_operation;
        self.push(Event::AppendInvoked {
            operation,
            payload: Bytes::from(payload),
        });
        if let Some(outcome) = outcome {
            self.push(Event::AppendFinished { operation, outcome });
        }
        operation
    }

    /// Adds a reading session that starts at `from_sequence` and is handed `records`.
    ///
    /// # Arguments
    ///
    /// * `session` - The identifier of the session.
    /// * `from_sequence` - The sequence number the session asked for first.
    /// * `records` - What the session was handed, in order: each record's sequence number and payload.
    pub(crate) fn read(&mut self, session: u64, from_sequence: u64, records: &[(u64, &'static str)]) {
        self.push(Event::ReaderStarted {
            session,
            from_sequence: SequenceNumber::new(from_sequence),
        });
        for (sequence, payload) in records {
            self.push(Event::RecordRead {
                session,
                sequence: SequenceNumber::new(*sequence),
                payload: Bytes::from(*payload),
            });
        }
    }

    /// Declares `payload` a filler.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload every such record has.
    pub(crate) fn declare_filler(&mut self, payload: &'static str) {
        self.push(Event::FillerDeclared {
            payload: Bytes::from(payload),
        });
    }
}

/// Builds the outcome of an append that was reported committed.
///
/// # Arguments
///
/// * `sequence` - The sequence number the writer reported.
/// * `epoch` - The epoch of the writer that reported it.
///
/// # Returns
///
/// The outcome, for [`Script::append`].
pub(crate) fn committed(sequence: u64, epoch: u64) -> AppendOutcome {
    AppendOutcome::Committed {
        sequence: SequenceNumber::new(sequence),
        epoch: Epoch::new(epoch),
    }
}

/// Builds the history of a correct log: appends of every outcome, read by two sessions.
///
/// Record `a` is committed at 1 in epoch 1; `b` has an unknown outcome and is in the stream at 2; `c` is committed at 3
/// in epoch 2; one append never returned and one was refused, and neither is in the stream. Session 1 reads records 1
/// to 3 from the start, session 2 reads 2 and 3.
///
/// # Returns
///
/// The script, to which a test adds what makes the history wrong.
pub(crate) fn correct_history() -> Script {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));
    script.append("b", Some(AppendOutcome::Unknown));
    script.append("c", Some(committed(3, 2)));
    script.append("crashed before any answer", None);
    script.append("refused", Some(AppendOutcome::NotAppended));
    script.read(1, 1, &[(1, "a"), (2, "b"), (3, "c")]);
    script.read(2, 2, &[(2, "b"), (3, "c")]);
    script
}

/// Starts a world of [`WORLD_NODE_COUNT`] nodes whose unlucky node follows `seed`.
///
/// Nothing runs in the world but its cluster: no client, no controller, no nemesis.
///
/// # Arguments
///
/// * `seed` - Seeds the random source the unlucky node is drawn from.
///
/// # Returns
///
/// The world, shared as the tasks of a run share it.
pub(crate) async fn start_world(seed: u64) -> Arc<World> {
    let mut random = DeterministicRandom::from_seed(seed);
    Arc::new(World::start(WORLD_NODE_COUNT, &mut random).await)
}

/// Lists what the simulator recorded that it did to the cluster, in order.
///
/// # Arguments
///
/// * `history` - The history of a world.
///
/// # Returns
///
/// The description of every fault event; other events are left out.
pub(crate) fn fault_descriptions(history: &History) -> Vec<String> {
    let mut descriptions = Vec::new();
    for timed in history.events() {
        if let Event::Fault { description } = timed.event {
            descriptions.push(description);
        }
    }
    descriptions
}

/// Appends one record of a stray segment to node `node_number`, so that the node holds a segment file.
///
/// The request goes to the replica directly, not through the node's link.
///
/// # Arguments
///
/// * `world` - The world the node is in.
/// * `node_number` - The node that is to hold the record. It must be running.
///
/// # Panics
///
/// Panics if the node is down or refuses the append, which a healthy node does not.
pub(crate) async fn store_one_record_on(world: &World, node_number: usize) {
    try_to_store_one_record_on(world, node_number)
        .await
        .expect("a healthy replica accepts the first batch of a segment");
}

/// Tries to append one record of a stray segment to node `node_number`, and reports how the replica answered.
///
/// The request goes to the replica directly, not through the node's link.
///
/// # Arguments
///
/// * `world` - The world the node is in.
/// * `node_number` - The node the record is sent to. It must be running.
///
/// # Errors
///
/// Returns the replica's error if it refuses the append, for example because its disk refuses the flush.
///
/// # Panics
///
/// Panics if the node is down.
pub(crate) async fn try_to_store_one_record_on(world: &World, node_number: usize) -> Result<(), ReplicaError> {
    let batch = Batch::encode(STRAY_SEGMENT, SequenceNumber::FIRST, &[Bytes::from("stored")])
        .expect("one short record fits in a batch");
    let request = AppendRequest {
        segment: SegmentDescriptor {
            address: STRAY_SEGMENT,
            epoch: STRAY_SEGMENT_EPOCH,
            first_sequence: SequenceNumber::FIRST,
        },
        writer_epoch: STRAY_SEGMENT_EPOCH,
        batch_frames: batch.frame().clone(),
        durable_sequence: SequenceNumber::ZERO,
    };
    let replica = world.cluster.nodes()[node_number]
        .replica()
        .expect("the node is running");
    replica.append(request).await?;
    Ok(())
}
