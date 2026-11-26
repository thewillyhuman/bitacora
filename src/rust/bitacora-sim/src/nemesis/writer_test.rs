//! Tests of the faults of the writer.
//!
//! # What is covered
//!
//! - A writer crash is sent to the designated compute node, and to no other, and is recorded.
//! - A writer switch designates the next compute node, wraps around after the last, and is recorded.
//!
//! # How the fixtures work
//!
//! The tests build the channels the simulation would hand the nemesis: one that names the designated writer, and one
//! crash counter per compute node. No compute node runs; the tests read the channels themselves. A small world of the
//! test support provides the history the faults are recorded in.

use super::*;
use crate::test_support::{fault_descriptions, start_world};

/// How many compute nodes the tests pretend the run has.
const COMPUTE_NODE_COUNT: usize = 3;

/// The channels the nemesis reaches the compute nodes through.
struct WriterChannels {
    /// Names the compute node that should be the writer.
    designated_writer: watch::Sender<usize>,
    /// One crash counter per compute node, by node number.
    crash_signals: Vec<watch::Sender<u64>>,
}

impl WriterChannels {
    /// Builds the channels of [`COMPUTE_NODE_COUNT`] compute nodes, with node `designated` as the writer.
    fn with_designated_writer(designated: usize) -> Self {
        let (designated_writer, _) = watch::channel(designated);
        let mut crash_signals = Vec::new();
        for _compute_node in 0..COMPUTE_NODE_COUNT {
            let (crash_signal, _) = watch::channel(0_u64);
            crash_signals.push(crash_signal);
        }
        Self {
            designated_writer,
            crash_signals,
        }
    }

    /// How often each compute node was told to crash, by node number.
    fn crash_counts(&self) -> Vec<u64> {
        let mut crash_counts = Vec::new();
        for crash_signal in &self.crash_signals {
            crash_counts.push(*crash_signal.borrow());
        }
        crash_counts
    }
}

#[tokio::test(start_paused = true)]
async fn writer_crash_is_sent_to_the_designated_node_only() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(1);

    crash_writer(&world, &channels.designated_writer, &channels.crash_signals);

    assert_eq!(channels.crash_counts(), [0, 1, 0]);
}

#[tokio::test(start_paused = true)]
async fn every_writer_crash_changes_the_signal_again() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(1);

    crash_writer(&world, &channels.designated_writer, &channels.crash_signals);
    crash_writer(&world, &channels.designated_writer, &channels.crash_signals);

    assert_eq!(channels.crash_counts(), [0, 2, 0]);
}

#[tokio::test(start_paused = true)]
async fn writer_crash_leaves_the_designation_as_it_is() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(1);

    crash_writer(&world, &channels.designated_writer, &channels.crash_signals);

    assert_eq!(*channels.designated_writer.borrow(), 1);
}

#[tokio::test(start_paused = true)]
async fn writer_crash_is_recorded_with_the_node_that_crashes() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(1);

    crash_writer(&world, &channels.designated_writer, &channels.crash_signals);

    assert_eq!(fault_descriptions(&world.history), ["compute node 1 crashes"]);
}

#[tokio::test(start_paused = true)]
async fn writer_switch_designates_the_next_compute_node() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(0);

    switch_writer(&world, &channels.designated_writer, COMPUTE_NODE_COUNT);

    assert_eq!(*channels.designated_writer.borrow(), 1);
}

#[tokio::test(start_paused = true)]
async fn writer_switch_wraps_around_after_the_last_compute_node() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(COMPUTE_NODE_COUNT - 1);

    switch_writer(&world, &channels.designated_writer, COMPUTE_NODE_COUNT);

    assert_eq!(*channels.designated_writer.borrow(), 0);
}

/// The writer that is replaced is not told: it must find out by being fenced.
#[tokio::test(start_paused = true)]
async fn writer_switch_tells_no_node_to_crash() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(0);

    switch_writer(&world, &channels.designated_writer, COMPUTE_NODE_COUNT);

    assert_eq!(channels.crash_counts(), [0, 0, 0]);
}

#[tokio::test(start_paused = true)]
async fn writer_switch_is_recorded_with_the_node_that_takes_over() {
    let world = start_world(1).await;
    let channels = WriterChannels::with_designated_writer(0);

    switch_writer(&world, &channels.designated_writer, COMPUTE_NODE_COUNT);

    assert_eq!(
        fault_descriptions(&world.history),
        ["compute node 1 takes the stream from a writer still running"]
    );
}
