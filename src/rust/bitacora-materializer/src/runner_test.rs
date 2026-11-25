//! Tests of the materializer runner against an in-process cluster.
//!
//! # What is covered
//!
//! - Applying: every committed record once, in order, including those from before the runner started.
//! - Checkpointing: by time, by bytes, on the materializer's request, and not at all when nothing changed.
//! - Restarting: a new runner resumes from the committed checkpoint and ends in the same state.
//! - Truncation: the log is only cut up to the committed checkpoint (invariant I5), and a runner whose
//!   next record is gone says so.
//! - Fencing: a second runner takes over and the first stops without committing.
//! - Outages of the artifact store and of the metadata store: the checkpoint is late, nothing is lost.
//! - Orphans: a crash between writing and committing leaves artifacts that the sweeper removes.
//!
//! # How the fixtures work
//!
//! [`Recorder`] is the materializer under test. Its state is a count and a checksum of the payloads it
//! applied, which is enough to tell whether two instances saw the same records. The runner creates the
//! instance, so a test cannot hand it anything; instead every instance carries a [`Probe`], which the test
//! takes from the restored instance before the runner starts. Through the probe the test sees what was
//! applied and switches the instance's behaviour.

use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bitacora_meta::MemoryKeyValueStore;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use super::*;
use crate::namespace::token_of_artifact;
use crate::sweeper::sweep_orphans;
use crate::test_harness::{Harness, MATERIALIZER, STREAM, append_numbered, numbered_payload};

/// The payload [`Recorder`] refuses to apply.
const POISON_PAYLOAD: &[u8] = b"poison";

/// How long a test waits for something that must happen. Simulated time, so it costs nothing.
const PATIENCE: Duration = Duration::from_secs(300);

/// The time limit the tests give the runner for a checkpoint.
const CHECKPOINT_AFTER: Duration = Duration::from_secs(10);

/// What a test sees of the instance the runner owns, and the switches it can flip on it.
#[derive(Clone, Default)]
struct Probe {
    /// Shared between the instance and the test.
    shared: Arc<Mutex<ProbeState>>,
}

/// The state behind a [`Probe`].
#[derive(Default)]
struct ProbeState {
    /// The sequence number of every record this instance applied, in the order it applied them.
    applied_sequences: Vec<u64>,
    /// How many records the instance's state covers, restored ones included.
    record_count: u64,
    /// The checksum of the payloads the instance's state covers.
    payload_checksum: u32,
    /// What the instance answers to `wants_checkpoint`.
    wants_checkpoint: bool,
    /// How many times the instance wrote a checkpoint.
    checkpoints_written: usize,
    /// A metadata store to make unreachable as soon as a checkpoint is written: the commit then fails.
    metadata_to_cut: Option<MemoryKeyValueStore>,
}

impl Probe {
    /// Locks the shared state. A panicking test cannot leave it half updated.
    fn lock(&self) -> MutexGuard<'_, ProbeState> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The sequence numbers this instance applied, in order.
    fn applied_sequences(&self) -> Vec<u64> {
        self.lock().applied_sequences.clone()
    }

    /// The instance's state: how many records it covers and the checksum of their payloads.
    fn state(&self) -> (u64, u32) {
        let probe = self.lock();
        (probe.record_count, probe.payload_checksum)
    }

    /// How many checkpoints the instance has written.
    fn checkpoints_written(&self) -> usize {
        self.lock().checkpoints_written
    }

    /// Makes the instance ask for a checkpoint after every batch, or stop asking.
    fn ask_for_checkpoints(&self, wants_checkpoint: bool) {
        self.lock().wants_checkpoint = wants_checkpoint;
    }

    /// Makes the instance cut `metadata` off right after it has written a checkpoint.
    fn cut_metadata_after_a_checkpoint_is_written(&self, metadata: MemoryKeyValueStore) {
        self.lock().metadata_to_cut = Some(metadata);
    }
}

/// The materializer under test: counts the records it applied and checksums their payloads.
struct Recorder {
    /// How many records the state covers.
    record_count: u64,
    /// The CRC-32C of the payloads the state covers, in order.
    payload_checksum: u32,
    /// What the test sees and switches.
    probe: Probe,
}

impl Recorder {
    /// The probe of this instance.
    fn probe(&self) -> Probe {
        self.probe.clone()
    }
}

#[async_trait]
impl Materializer for Recorder {
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        for record in committed_records {
            if record.payload == POISON_PAYLOAD {
                return Err(MaterializerError::InvalidRecord {
                    sequence: record.sequence,
                    reason: "the payload is poison".to_owned(),
                });
            }
            self.record_count += 1;
            self.payload_checksum = crc32c::crc32c_append(self.payload_checksum, &record.payload);
            let mut probe = self.probe.lock();
            probe.applied_sequences.push(record.sequence.get());
            probe.record_count = self.record_count;
            probe.payload_checksum = self.payload_checksum;
        }
        Ok(())
    }

    fn wants_checkpoint(&self) -> bool {
        self.probe.lock().wants_checkpoint
    }

    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError> {
        let mut snapshot = BytesMut::new();
        snapshot.put_u64_le(self.record_count);
        snapshot.put_u32_le(self.payload_checksum);
        let snapshot_key = artifacts
            .put_artifact("snapshot", snapshot.freeze())
            .await?;

        let mut probe = self.probe.lock();
        probe.checkpoints_written += 1;
        if let Some(metadata) = &probe.metadata_to_cut {
            metadata.set_unavailable(true);
        }
        Ok(CheckpointManifest {
            artifact_keys: vec![snapshot_key],
            details: Bytes::new(),
        })
    }

    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let mut record_count = 0;
        let mut payload_checksum = 0;
        if let Some(manifest) = last_checkpoint {
            let mut snapshot = artifacts.get_artifact(&manifest.artifact_keys[0]).await?;
            record_count = snapshot.get_u64_le();
            payload_checksum = snapshot.get_u32_le();
        }
        let probe = Probe::default();
        {
            let mut shared = probe.lock();
            shared.record_count = record_count;
            shared.payload_checksum = payload_checksum;
        }
        Ok(Self {
            record_count,
            payload_checksum,
            probe,
        })
    }
}

/// The state a [`Recorder`] has after applying the records whose numbers are in `record_numbers`.
fn state_after(record_numbers: RangeInclusive<u64>) -> (u64, u32) {
    let mut record_count = 0;
    let mut payload_checksum = 0;
    for record_number in record_numbers {
        record_count += 1;
        payload_checksum =
            crc32c::crc32c_append(payload_checksum, &numbered_payload(record_number));
    }
    (record_count, payload_checksum)
}

/// Settings under which only time makes a checkpoint due: every [`CHECKPOINT_AFTER`].
fn checkpoint_by_time() -> RunnerSettings {
    RunnerSettings {
        checkpoint_after_bytes: u64::MAX,
        checkpoint_after: CHECKPOINT_AFTER,
        ..RunnerSettings::for_materializer(MATERIALIZER)
    }
}

/// Settings under which nothing makes a checkpoint due unless the materializer asks.
fn checkpoint_only_on_request() -> RunnerSettings {
    RunnerSettings {
        checkpoint_after_bytes: u64::MAX,
        checkpoint_after: Duration::from_hours(100_000),
        ..RunnerSettings::for_materializer(MATERIALIZER)
    }
}

/// Takes the materializer over and restores it, without running it yet.
async fn start(harness: &Harness, settings: RunnerSettings) -> MaterializerRunner<Recorder> {
    MaterializerRunner::start(harness.dependencies(), STREAM, settings)
        .await
        .expect("the runner starts")
}

/// A runner at work in its own task.
struct Running {
    /// Where the runner is.
    progress: RunnerProgress,
    /// The probe of the instance it runs.
    probe: Probe,
    /// The task; it ends with the reason the runner stopped.
    task: JoinHandle<RunnerError>,
}

impl Running {
    /// Waits until the runner has applied the record at `sequence`.
    async fn applied(&mut self, sequence: u64) {
        timeout(
            PATIENCE,
            self.progress
                .wait_until_applied(SequenceNumber::new(sequence)),
        )
        .await
        .expect("the record is applied in time")
        .expect("the runner is still running");
    }

    /// Waits until the runner has committed a checkpoint at or above `sequence`.
    async fn committed(&mut self, sequence: u64) {
        timeout(
            PATIENCE,
            self.progress
                .wait_until_committed(SequenceNumber::new(sequence)),
        )
        .await
        .expect("the checkpoint is committed in time")
        .expect("the runner is still running");
    }

    /// Waits for the runner to stop and returns why.
    async fn stopped(self) -> RunnerError {
        timeout(PATIENCE, self.task)
            .await
            .expect("the runner stops in time")
            .expect("the runner does not panic")
    }

    /// Stops the runner where it is, as a crash would.
    async fn crash(self) {
        self.task.abort();
        drop(self.task.await);
    }
}

/// Runs a started runner in its own task.
fn spawn(runner: MaterializerRunner<Recorder>) -> Running {
    Running {
        progress: runner.progress(),
        probe: runner.materializer().probe(),
        task: tokio::spawn(runner.run()),
    }
}

/// Starts a runner and runs it.
async fn start_and_spawn(harness: &Harness, settings: RunnerSettings) -> Running {
    spawn(start(harness, settings).await)
}

// ---- starting and applying ----

#[tokio::test(start_paused = true)]
async fn materializer_never_checkpointed_is_restored_empty() {
    let harness = Harness::start().await;

    let runner = start(&harness, checkpoint_by_time()).await;

    assert_eq!(runner.materializer().probe().state(), (0, 0));
}

#[tokio::test(start_paused = true)]
async fn first_runner_holds_the_first_fencing_token() {
    let harness = Harness::start().await;

    let runner = start(&harness, checkpoint_by_time()).await;

    assert_eq!(runner.fencing_token(), 1);
}

#[tokio::test(start_paused = true)]
async fn materializer_that_is_not_registered_cannot_be_run() {
    let harness = Harness::start().await;
    let unregistered = RunnerSettings::for_materializer("never-registered");

    let started =
        MaterializerRunner::<Recorder>::start(harness.dependencies(), STREAM, unregistered).await;

    assert!(matches!(
        started.err(),
        Some(RunnerError::Metadata(
            MetaError::MaterializerNotRegistered { .. }
        ))
    ));
}

#[tokio::test(start_paused = true)]
async fn records_are_applied_once_each_and_in_order() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;

    append_numbered(&writer, 20).await;
    running.applied(20).await;

    assert_eq!(
        running.probe.applied_sequences(),
        (1..=20).collect::<Vec<u64>>()
    );
}

#[tokio::test(start_paused = true)]
async fn records_committed_before_the_runner_started_are_applied() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    append_numbered(&writer, 7).await;

    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    running.applied(7).await;

    assert_eq!(running.probe.state(), state_after(1..=7));
}

#[tokio::test(start_paused = true)]
async fn record_that_cannot_be_applied_stops_the_runner() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let running = start_and_spawn(&harness, checkpoint_by_time()).await;

    writer
        .append(Bytes::from_static(POISON_PAYLOAD))
        .await
        .expect("the record commits");

    assert_eq!(
        running.stopped().await,
        RunnerError::Materializer(MaterializerError::InvalidRecord {
            sequence: SequenceNumber::new(1),
            reason: "the payload is poison".to_owned(),
        })
    );
}

#[tokio::test(start_paused = true)]
async fn progress_shows_everything_applied_as_no_lag() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;

    append_numbered(&writer, 5).await;
    running.applied(5).await;

    assert_eq!(running.progress.latest().lag_records(), 0);
}

#[tokio::test(start_paused = true)]
async fn progress_of_a_stopped_runner_reports_that_it_stopped() {
    let harness = Harness::start().await;
    let running = start_and_spawn(&harness, checkpoint_by_time()).await;
    let mut progress = running.progress.clone();

    running.crash().await;

    assert_eq!(
        progress.wait_until_applied(SequenceNumber::new(1)).await,
        Err(RunnerStopped)
    );
}

// ---- checkpointing ----

#[tokio::test(start_paused = true)]
async fn checkpoint_is_committed_once_enough_time_has_passed() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;

    running.committed(5).await;

    assert_eq!(
        harness.committed_checkpoint().await.checkpoint,
        SequenceNumber::new(5)
    );
}

#[tokio::test(start_paused = true)]
async fn checkpoint_is_not_committed_before_its_time() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.applied(5).await;

    sleep(CHECKPOINT_AFTER / 2).await;

    assert_eq!(
        harness.committed_checkpoint().await.checkpoint,
        SequenceNumber::ZERO
    );
}

#[tokio::test(start_paused = true)]
async fn checkpoint_is_committed_once_enough_bytes_were_applied() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let by_bytes = RunnerSettings {
        checkpoint_after_bytes: 64,
        ..checkpoint_only_on_request()
    };
    let mut running = start_and_spawn(&harness, by_bytes).await;

    writer
        .append(Bytes::from(vec![7; 100]))
        .await
        .expect("the record commits");

    running.committed(1).await;
}

#[tokio::test(start_paused = true)]
async fn checkpoint_is_committed_when_the_materializer_asks() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let runner = start(&harness, checkpoint_only_on_request()).await;
    runner.materializer().probe().ask_for_checkpoints(true);
    let mut running = spawn(runner);

    append_numbered(&writer, 3).await;

    running.committed(3).await;
}

#[tokio::test(start_paused = true)]
async fn committed_checkpoint_points_at_its_manifest() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.committed(5).await;

    let manifest_key = harness
        .committed_checkpoint()
        .await
        .manifest
        .expect("a committed checkpoint has a manifest");

    assert_eq!(token_of_artifact(&manifest_key), Some(1));
}

#[tokio::test(start_paused = true)]
async fn no_checkpoint_is_written_while_nothing_new_was_applied() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.committed(5).await;

    sleep(CHECKPOINT_AFTER * 10).await;

    assert_eq!(running.probe.checkpoints_written(), 1);
}

#[tokio::test(start_paused = true)]
async fn artifacts_of_the_previous_checkpoint_are_deleted_after_the_next_commit() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let runner = start(&harness, checkpoint_only_on_request()).await;
    runner.materializer().probe().ask_for_checkpoints(true);
    let mut running = spawn(runner);
    append_numbered(&writer, 2).await;
    running.committed(2).await;

    append_numbered(&writer, 2).await;
    running.committed(4).await;
    // The deletion follows the commit within the same step of the runner.
    sleep(Duration::from_secs(1)).await;

    // One snapshot and its manifest.
    assert_eq!(harness.stored_object_count().await, 2);
}

// ---- restarting ----

#[tokio::test(start_paused = true)]
async fn restarted_runner_applies_only_the_records_above_the_checkpoint() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut first_run = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 10).await;
    first_run.committed(10).await;
    append_numbered(&writer, 5).await;
    first_run.applied(15).await;
    first_run.crash().await;

    let mut second_run = start_and_spawn(&harness, checkpoint_by_time()).await;
    second_run.applied(15).await;

    assert_eq!(
        second_run.probe.applied_sequences(),
        (11..=15).collect::<Vec<u64>>()
    );
}

#[tokio::test(start_paused = true)]
async fn restarted_runner_ends_in_the_state_of_one_that_never_stopped() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut first_run = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 10).await;
    first_run.committed(10).await;
    append_numbered(&writer, 5).await;
    first_run.applied(15).await;
    first_run.crash().await;

    let mut second_run = start_and_spawn(&harness, checkpoint_by_time()).await;
    second_run.applied(15).await;

    assert_eq!(second_run.probe.state(), state_after(1..=15));
}

#[tokio::test(start_paused = true)]
async fn manifest_of_another_checkpoint_is_refused_at_start() {
    let harness = Harness::start().await;
    let stream_id = harness.stream().await.stream_id;
    let metadata = harness.cluster.metadata();
    let acquired = metadata
        .acquire_checkpoint(stream_id, MATERIALIZER)
        .await
        .expect("the materializer is registered");
    let artifacts = ArtifactNamespace::for_runner(
        Arc::new(harness.artifact_store.clone()),
        stream_id,
        MATERIALIZER,
        acquired.fencing_token,
    );
    let manifest_of_checkpoint_7 =
        encode_manifest(SequenceNumber::new(7), &CheckpointManifest::default());
    let manifest_key = artifacts
        .put_artifact("manifest", manifest_of_checkpoint_7)
        .await
        .expect("the manifest is stored");
    metadata
        .commit_checkpoint(
            stream_id,
            MATERIALIZER,
            acquired.fencing_token,
            SequenceNumber::new(3),
            Some(manifest_key),
        )
        .await
        .expect("the checkpoint is committed");

    let started =
        MaterializerRunner::<Recorder>::start(harness.dependencies(), STREAM, checkpoint_by_time())
            .await;

    assert!(matches!(
        started.err(),
        Some(RunnerError::Materializer(
            MaterializerError::InvalidManifest { .. }
        ))
    ));
}

// ---- truncation ----

#[tokio::test(start_paused = true)]
async fn log_is_not_truncated_before_a_checkpoint_is_committed() {
    let harness = Harness::start().await;
    let writer = harness.open_writer_of_small_segments().await;
    let mut running = start_and_spawn(&harness, checkpoint_only_on_request()).await;
    harness.append_one_segment_each(&writer, 3).await;
    running.applied(3).await;

    harness.run_controller_pass().await;

    assert_eq!(harness.stream().await.low_water_mark, SequenceNumber::FIRST);
}

#[tokio::test(start_paused = true)]
async fn log_is_truncated_up_to_the_committed_checkpoint_and_no_further() {
    let harness = Harness::start().await;
    let writer = harness.open_writer_of_small_segments().await;
    let runner = start(&harness, checkpoint_only_on_request()).await;
    let probe = runner.materializer().probe();
    probe.ask_for_checkpoints(true);
    let mut running = spawn(runner);
    harness.append_one_segment_each(&writer, 3).await;
    running.committed(3).await;
    probe.ask_for_checkpoints(false);
    harness.append_one_segment_each(&writer, 2).await;
    running.applied(5).await;

    harness.run_controller_pass().await;

    // Records 4 and 5 are applied but not checkpointed: their segments must stay.
    assert_eq!(
        harness.stream().await.low_water_mark,
        SequenceNumber::new(4)
    );
}

#[tokio::test(start_paused = true)]
async fn runner_keeps_applying_after_the_log_below_its_checkpoint_is_gone() {
    let harness = Harness::start().await;
    let writer = harness.open_writer_of_small_segments().await;
    let runner = start(&harness, checkpoint_only_on_request()).await;
    runner.materializer().probe().ask_for_checkpoints(true);
    let mut running = spawn(runner);
    harness.append_one_segment_each(&writer, 3).await;
    running.committed(3).await;
    harness.run_controller_pass().await;

    harness.append_one_segment_each(&writer, 2).await;
    running.applied(5).await;

    assert_eq!(running.probe.applied_sequences(), [1, 2, 3, 4, 5]);
}

/// Deletes the first two records of the stream although no checkpoint covers them, as only a bug or an
/// operator overriding the materializer could.
async fn truncate_above_the_checkpoint(harness: &Harness) {
    harness
        .cluster
        .metadata()
        .update_stream(STREAM, |stream| {
            stream.low_water_mark = SequenceNumber::new(3);
            stream
                .segments
                .retain(|segment| segment.first_sequence >= SequenceNumber::new(3));
            Ok(())
        })
        .await
        .expect("the stream is updated");
}

#[tokio::test(start_paused = true)]
async fn runner_whose_next_record_was_truncated_stops_and_says_so() {
    let harness = Harness::start().await;
    let writer = harness.open_writer_of_small_segments().await;
    harness.append_one_segment_each(&writer, 3).await;
    truncate_above_the_checkpoint(&harness).await;

    let running = start_and_spawn(&harness, checkpoint_by_time()).await;

    assert_eq!(
        running.stopped().await,
        RunnerError::LogTruncated {
            next_sequence: SequenceNumber::new(1),
            low_water_mark: SequenceNumber::new(3),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn replaced_runner_left_behind_by_truncation_reports_that_it_was_replaced() {
    let harness = Harness::start().await;
    let writer = harness.open_writer_of_small_segments().await;
    harness.append_one_segment_each(&writer, 3).await;
    let replaced_runner = start(&harness, checkpoint_by_time()).await;
    let _successor = start(&harness, checkpoint_by_time()).await;
    truncate_above_the_checkpoint(&harness).await;

    let running = spawn(replaced_runner);

    assert_eq!(
        running.stopped().await,
        RunnerError::Fenced {
            held_token: 1,
            current_token: 2,
        }
    );
}

// ---- fencing ----

#[tokio::test(start_paused = true)]
async fn runner_that_was_replaced_stops_at_its_next_checkpoint() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let replaced = start_and_spawn(&harness, checkpoint_by_time()).await;
    let _successor = start_and_spawn(&harness, checkpoint_by_time()).await;

    append_numbered(&writer, 5).await;

    assert_eq!(
        replaced.stopped().await,
        RunnerError::Fenced {
            held_token: 1,
            current_token: 2,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn only_the_successor_commits_after_a_takeover() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let replaced = start_and_spawn(&harness, checkpoint_by_time()).await;
    let mut successor = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    replaced.stopped().await;
    successor.committed(5).await;

    let manifest_key = harness
        .committed_checkpoint()
        .await
        .manifest
        .expect("a committed checkpoint has a manifest");

    assert_eq!(token_of_artifact(&manifest_key), Some(2));
}

// ---- outages ----

#[tokio::test(start_paused = true)]
async fn artifact_store_outage_commits_nothing() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.applied(5).await;

    harness.artifact_store.set_unavailable(true);
    sleep(CHECKPOINT_AFTER * 2).await;

    assert_eq!(
        running.progress.latest().committed_checkpoint,
        SequenceNumber::ZERO
    );
}

#[tokio::test(start_paused = true)]
async fn records_are_applied_during_an_artifact_store_outage() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    harness.artifact_store.set_unavailable(true);
    sleep(CHECKPOINT_AFTER * 2).await;

    append_numbered(&writer, 3).await;
    running.applied(8).await;

    assert_eq!(running.probe.state(), state_after(1..=8));
}

#[tokio::test(start_paused = true)]
async fn checkpoint_put_off_by_an_artifact_store_outage_covers_everything_applied_since() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    harness.artifact_store.set_unavailable(true);
    sleep(CHECKPOINT_AFTER * 2).await;
    append_numbered(&writer, 3).await;
    running.applied(8).await;

    harness.artifact_store.set_unavailable(false);
    running.committed(8).await;

    assert_eq!(
        harness.committed_checkpoint().await.checkpoint,
        SequenceNumber::new(8)
    );
}

#[tokio::test(start_paused = true)]
async fn metadata_outage_commits_nothing_and_stops_nothing() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.applied(5).await;

    harness.cluster.key_value_store().set_unavailable(true);
    sleep(CHECKPOINT_AFTER * 2).await;

    assert_eq!(
        (
            running.progress.latest().committed_checkpoint,
            running.task.is_finished()
        ),
        (SequenceNumber::ZERO, false)
    );
}

#[tokio::test(start_paused = true)]
async fn checkpoint_put_off_by_a_metadata_outage_is_committed_afterwards() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = start_and_spawn(&harness, checkpoint_by_time()).await;
    append_numbered(&writer, 5).await;
    running.applied(5).await;
    harness.cluster.key_value_store().set_unavailable(true);
    sleep(CHECKPOINT_AFTER * 2).await;

    harness.cluster.key_value_store().set_unavailable(false);
    running.committed(5).await;

    assert_eq!(
        harness.committed_checkpoint().await.checkpoint,
        SequenceNumber::new(5)
    );
}

// ---- orphans ----

/// Runs a runner that writes a checkpoint for records 1 to 5 and crashes before it can commit it.
/// Returns how many objects the crash left in the artifact store.
async fn crash_between_writing_and_committing(harness: &Harness) -> usize {
    let writer = harness.open_writer().await;
    append_numbered(&writer, 5).await;
    let runner = start(harness, checkpoint_by_time()).await;
    let probe = runner.materializer().probe();
    probe.cut_metadata_after_a_checkpoint_is_written(harness.cluster.key_value_store().clone());
    let mut running = spawn(runner);
    running.applied(5).await;
    while probe.checkpoints_written() == 0 {
        sleep(Duration::from_secs(1)).await;
    }

    running.crash().await;
    harness.cluster.key_value_store().set_unavailable(false);
    harness.stored_object_count().await
}

#[tokio::test(start_paused = true)]
async fn crash_before_the_commit_leaves_artifacts_and_no_checkpoint() {
    let harness = Harness::start().await;

    let orphan_count = crash_between_writing_and_committing(&harness).await;

    let committed = harness.committed_checkpoint().await;
    assert!(orphan_count >= 2, "only {orphan_count} objects were left");
    assert_eq!(
        (committed.checkpoint, committed.manifest),
        (SequenceNumber::ZERO, None)
    );
}

#[tokio::test(start_paused = true)]
async fn runner_after_a_crash_before_the_commit_applies_the_records_again() {
    let harness = Harness::start().await;
    crash_between_writing_and_committing(&harness).await;

    let mut second_run = start_and_spawn(&harness, checkpoint_only_on_request()).await;
    second_run.applied(5).await;

    assert_eq!(second_run.probe.applied_sequences(), [1, 2, 3, 4, 5]);
}

#[tokio::test(start_paused = true)]
async fn sweeper_removes_what_a_crash_before_the_commit_left() {
    let harness = Harness::start().await;
    let orphan_count = crash_between_writing_and_committing(&harness).await;
    let _second_run = start_and_spawn(&harness, checkpoint_only_on_request()).await;
    let stream_id = harness.stream().await.stream_id;

    let report = sweep_orphans(
        harness.cluster.metadata(),
        Arc::new(harness.artifact_store.clone()),
        stream_id,
        MATERIALIZER,
    )
    .await
    .expect("the sweep runs");

    assert_eq!(report.artifacts_deleted, orphan_count);
    assert_eq!(harness.stored_object_count().await, 0);
}
