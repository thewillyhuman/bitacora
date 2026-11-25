//! The workload of a simulated run: compute nodes that serve a key-value store.
//!
//! # Why this file exists
//!
//! A simulation needs clients that do something whose result can be judged. Until the materializer
//! framework existed, they appended opaque records, and only the log could be judged. Now each compute
//! node is a bitacora-kv front end: it becomes the writer of the stream, runs the materializer, and
//! serves puts, deletes and linearizable reads. Every call is recorded twice: as a call to the store, for
//! the linearizability check, and, for puts, as an append to the log, for the log checker. One workload
//! thereby exercises the log, the materializer runner, the page adapter, checkpoints, truncation and
//! failover together (design, section 18, the exit gate of M4).
//!
//! # What it contains
//!
//! - [`ComputeNode`]: one compute node, which opens a store whenever it is the designated writer and
//!   keeps a few client calls in flight on it.
//! - [`InjectedBug`]: deliberate mistakes in how the node uses the store, to prove the checks catch them.
//! - [`commit_final_record`] and [`read_every_key`]: what the audit does through a store after healing.
//! - [`declare_fillers`], [`store_settings`]: setup shared with `simulation.rs`.
//!
//! # How it works
//!
//! All compute nodes work on the same few keys, so that their calls collide. A put writes a value no
//! other put writes; that is what lets the checks tell which write a read saw. The node draws its calls
//! from its own seeded random source, so a seed replays the same calls.
//!
//! A put is one log record whose payload the node can compute beforehand, so it is recorded as an append
//! too. A delete of a key, and the barrier a read appends, have the same payload every time; they are
//! declared to the history as fillers, which the log checker does not try to attribute to an append.
//!
//! A node stops using its store, and opens a new one if it is still the designated writer, when a call
//! shows that the store was replaced: a write that was not made or whose outcome is unknown, or a read
//! that could not be proven current. A node that is told to crash drops everything in flight; the history
//! then never learns how those calls ended, exactly as a real client would not.
//!
//! # Invariants
//!
//! - Every call is recorded before it is made and after it returns, never the other way round.
//! - The node reports to the history what the store told it. Only an [`InjectedBug`] makes it lie.
//!
//! # See also
//!
//! - `simulation.rs`: the run these nodes are part of.
//! - `linearizability.rs`: the check of the calls recorded here.

use std::sync::Arc;
use std::time::Duration;

use bitacora_client::TakeoverPolicy;
use bitacora_kv::{KvDependencies, KvError, KvRecord, KvSettings, KvStore};
use bitacora_materializer::RunnerError;
use bitacora_types::{DeterministicRandom, SequenceNumber};
use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

use crate::history::{AppendOutcome, History, KvCall, KvOutcome};

/// How many keys the compute nodes work on. Few, so that calls on the same key overlap often.
const KEY_COUNT: u64 = 6;

/// How many client calls a compute node keeps in flight.
const CALLS_IN_FLIGHT: usize = 4;

/// The longest pause, in milliseconds, a compute node makes after a call returned before it starts the
/// next ones. It sets how many calls fit in a run, and with that how long a run takes to simulate: a
/// run makes a few thousand calls, which is plenty for the checks and keeps a seed to a few seconds.
const LONGEST_PAUSE_BETWEEN_CALLS_MILLIS: u64 = 20;

/// The key the audit writes its final record under. No compute node writes it.
const AUDIT_KEY: &[u8] = b"audit";

/// How long the audit waits for its final record to commit before it tries with a new store.
const FINAL_RECORD_TIMEOUT: Duration = Duration::from_secs(20);

/// A deliberate mistake in how the simulated compute nodes use the store.
///
/// A correct run uses [`InjectedBug::None`]. The others exist so that the simulator's own tests can show
/// that a store used wrongly produces a history the checks reject.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InjectedBug {
    /// The nodes use the store as documented.
    #[default]
    None,
    /// Reads return what the node's pages hold right now, without first proving that the node is still
    /// the writer and has caught up with the log. A node that was replaced, or has just started, then
    /// serves stale values.
    ReadsSkipTheBarrier,
    /// A write whose outcome the store does not know is reported to its caller as done. If it then
    /// never takes effect, a write the caller counts on is lost.
    UnknownWritesReportedDone,
}

/// The key number `key_number` of the simulated key space.
fn key(key_number: u64) -> Bytes {
    Bytes::from(format!("key-{key_number}"))
}

/// Tells the history which records of the stream nobody will be recorded as appending: the barrier of
/// a read, and the delete of each key, which have the same payload every time they are written.
pub(crate) fn declare_fillers(history: &History) {
    history.declare_filler(KvRecord::barrier().encode());
    for key_number in 0..KEY_COUNT {
        history.declare_filler(KvRecord::delete(key(key_number)).encode());
    }
}

/// The settings of every store a simulation opens: checkpoints and timeouts far shorter than in
/// production, so that checkpoints, truncation and read timeouts all happen many times in a minute.
pub(crate) fn store_settings(takeover: TakeoverPolicy) -> KvSettings {
    KvSettings {
        takeover,
        checkpoint_after: Duration::from_secs(2),
        retry_interval: Duration::from_millis(500),
        read_wait: Duration::from_secs(5),
        ..KvSettings::default()
    }
}

/// One simulated compute node.
pub(crate) struct ComputeNode {
    /// Which compute node this is. It is the writer while the nemesis designates this number.
    pub(crate) node_number: usize,
    /// The stream the store lives on.
    pub(crate) stream: &'static str,
    /// How the node's stores reach the cluster.
    pub(crate) dependencies: KvDependencies,
    /// Where the node records its calls.
    pub(crate) history: History,
    /// The mistake the node makes on purpose, if any.
    pub(crate) injected_bug: InjectedBug,
    /// The node's own source of randomness.
    pub(crate) random: DeterministicRandom,
    /// How many puts the node has made, to give each a value of its own.
    pub(crate) puts_made: u64,
}

impl ComputeNode {
    /// Runs the node until the simulation ends: while it is the designated writer it keeps a store open
    /// and client calls in flight; when it is told to crash it drops everything and starts over.
    ///
    /// # Arguments
    ///
    /// * `designated_writer` - Which compute node should be the writer. A node that loses the
    ///   designation is not told: it finds out by being replaced.
    /// * `crashes` - Changes whenever this node must crash.
    pub(crate) async fn run(
        mut self,
        mut designated_writer: watch::Receiver<usize>,
        mut crashes: watch::Receiver<u64>,
    ) {
        loop {
            while *designated_writer.borrow_and_update() != self.node_number {
                if designated_writer.changed().await.is_err() {
                    return;
                }
            }
            let takeover = if self.random.chance(1, 2) {
                TakeoverPolicy::Force
            } else {
                TakeoverPolicy::WaitForLeaseExpiry
            };
            let opened = KvStore::open(self.dependencies.clone(), self.stream, store_settings(takeover)).await;
            let Ok(store) = opened else {
                sleep(Duration::from_millis(self.random.between(100, 1_000))).await;
                continue;
            };
            crashes.borrow_and_update();

            let was_crashed = self.serve_clients(Arc::new(store), &mut crashes).await;
            let restart_delay = if was_crashed {
                self.random.between(100, 5_000)
            } else {
                self.random.between(10, 300)
            };
            sleep(Duration::from_millis(restart_delay)).await;
        }
    }

    /// Keeps client calls in flight on `store` until the node crashes or a call shows that the store
    /// can no longer be used. The calls still in flight are then dropped: nobody learns how they ended.
    ///
    /// # Returns
    ///
    /// Whether the node was told to crash.
    async fn serve_clients(&mut self, store: Arc<KvStore>, crashes: &mut watch::Receiver<u64>) -> bool {
        let mut calls: JoinSet<bool> = JoinSet::new();
        let was_crashed = loop {
            while calls.len() < CALLS_IN_FLIGHT {
                let call = self.next_call();
                calls.spawn(make_call(
                    Arc::clone(&store),
                    self.history.clone(),
                    call,
                    self.injected_bug,
                ));
            }
            tokio::select! {
                biased;
                () = next_crash(crashes) => break true,
                Some(finished) = calls.join_next() => {
                    let can_go_on = finished.unwrap_or(false);
                    if !can_go_on {
                        break false;
                    }
                }
            }
            sleep(Duration::from_millis(
                self.random.between(0, LONGEST_PAUSE_BETWEEN_CALLS_MILLIS),
            ))
            .await;
        };
        calls.abort_all();
        while calls.join_next().await.is_some() {}
        was_crashed
    }

    /// Draws the next client call: mostly puts and reads, some deletes, on a random key.
    fn next_call(&mut self) -> KvCall {
        let key = key(self.random.between(0, KEY_COUNT - 1));
        let kind = self.random.between(0, 99);
        if kind < 45 {
            self.puts_made += 1;
            let value = Bytes::from(format!("compute-{}-value-{}", self.node_number, self.puts_made));
            return KvCall::Put { key, value };
        }
        if kind < 55 {
            return KvCall::Delete { key };
        }
        KvCall::Get { key }
    }
}

/// Resolves when the compute node is told to crash. If nobody is left to tell it, it never resolves.
async fn next_crash(crashes: &mut watch::Receiver<u64>) {
    if crashes.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Makes one client call on `store` and records it in `history`.
///
/// # Returns
///
/// Whether the store can still be used: `false` once a call shows that it was replaced or stopped.
async fn make_call(store: Arc<KvStore>, history: History, call: KvCall, injected_bug: InjectedBug) -> bool {
    match &call {
        KvCall::Put { key, value } => {
            let payload = KvRecord::put(key.clone(), value.clone()).encode();
            let append = history.append_invoked(payload);
            let operation = history.kv_invoked(call.clone());
            let written = store.put(key.clone(), value.clone()).await;
            let append_outcome = match &written {
                Ok(sequence) => AppendOutcome::Committed {
                    sequence: *sequence,
                    epoch: store.writer_epoch(),
                },
                Err(KvError::NotWritten { .. }) => AppendOutcome::NotAppended,
                Err(_outcome_unknown) => AppendOutcome::Unknown,
            };
            history.append_finished(append, append_outcome);
            history.kv_returned(operation, outcome_of_a_write(&written, injected_bug));
            written.is_ok()
        }
        KvCall::Delete { key } => {
            let operation = history.kv_invoked(call.clone());
            let written = store.delete(key.clone()).await;
            history.kv_returned(operation, outcome_of_a_write(&written, injected_bug));
            written.is_ok()
        }
        KvCall::Get { key } => {
            let operation = history.kv_invoked(call.clone());
            let read = if injected_bug == InjectedBug::ReadsSkipTheBarrier {
                store.get_as_of(key, store.applied_sequence()).await
            } else {
                store.get(key).await
            };
            let can_go_on = match &read {
                Ok(_value) => true,
                Err(KvError::Materializer(RunnerError::LogTruncated {
                    next_sequence,
                    low_water_mark,
                })) => {
                    // The log only promises to keep what lies above the committed checkpoint. A
                    // materializer that holds the token and finds its next record gone was betrayed.
                    history.truncated_past_checkpoint(next_sequence.preceding(), *low_water_mark);
                    false
                }
                Err(KvError::NotCurrent { .. } | KvError::Materializer(_)) => false,
                // The materializer is behind, or a layer could not be fetched: the next read may work.
                Err(_no_answer_this_time) => true,
            };
            let outcome = match read {
                Ok(value) => KvOutcome::Read { value },
                Err(_no_answer) => KvOutcome::ReadFailed,
            };
            history.kv_returned(operation, outcome);
            can_go_on
        }
    }
}

/// What the history is told about a write, given what the store returned.
fn outcome_of_a_write(written: &Result<SequenceNumber, KvError>, injected_bug: InjectedBug) -> KvOutcome {
    match written {
        Ok(_sequence) => KvOutcome::Written,
        Err(KvError::NotWritten { .. }) => KvOutcome::NotWritten,
        Err(_outcome_unknown) if injected_bug == InjectedBug::UnknownWritesReportedDone => KvOutcome::Written,
        Err(_outcome_unknown) => KvOutcome::WriteUnknown,
    }
}

/// Writes the final record of a run through `store`: a put under a key of its own.
///
/// # Arguments
///
/// * `store` - The auditor's store, opened after healing.
/// * `history` - Where the put is recorded, as a call and as an append.
/// * `attempt` - Which attempt this is, so that every attempt writes a value of its own.
///
/// # Returns
///
/// The sequence number of the final record, or `None` if it could not be committed in time; the audit
/// then tries again with a new store.
pub(crate) async fn commit_final_record(store: &KvStore, history: &History, attempt: u64) -> Option<SequenceNumber> {
    let key = Bytes::from_static(AUDIT_KEY);
    let value = Bytes::from(format!("final-record-{attempt}"));
    let append = history.append_invoked(KvRecord::put(key.clone(), value.clone()).encode());
    let operation = history.kv_invoked(KvCall::Put {
        key: key.clone(),
        value: value.clone(),
    });
    let written = timeout(FINAL_RECORD_TIMEOUT, store.put(key, value)).await;
    if let Ok(Ok(sequence)) = written {
        history.append_finished(
            append,
            AppendOutcome::Committed {
                sequence,
                epoch: store.writer_epoch(),
            },
        );
        history.kv_returned(operation, KvOutcome::Written);
        return Some(sequence);
    }
    history.append_finished(append, AppendOutcome::Unknown);
    history.kv_returned(operation, KvOutcome::WriteUnknown);
    None
}

/// Reads every key of the simulated key space, and the audit's own, through `store`, and records what
/// it saw. These reads come after every write of the run, so they pin down which of the writes of
/// unknown outcome took effect.
///
/// # Returns
///
/// Whether every read was answered. If not, the audit waits and reads all keys again.
pub(crate) async fn read_every_key(store: &KvStore, history: &History) -> bool {
    let mut keys = vec![Bytes::from_static(AUDIT_KEY)];
    for key_number in 0..KEY_COUNT {
        keys.push(key(key_number));
    }
    for key in keys {
        let operation = history.kv_invoked(KvCall::Get { key: key.clone() });
        match store.get(&key).await {
            Ok(value) => history.kv_returned(operation, KvOutcome::Read { value }),
            Err(_no_answer) => {
                history.kv_returned(operation, KvOutcome::ReadFailed);
                return false;
            }
        }
    }
    true
}
