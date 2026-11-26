//! Tests of the artifact catalog adapter.
//!
//! # What is covered
//!
//! - Sealing: a checkpoint cuts one artifact that covers exactly the records since the last one.
//! - The invariant: the catalog is gapless and without overlap, also after a crash at any point and the
//!   replay that follows, and the artifacts hold exactly the records their ranges say.
//! - Compaction: runs of small neighbours are merged, tiers are respected, one merge per checkpoint.
//! - Restoring, the view, and the catalog as a process that only queries reads it.
//! - Failures: a seal that fails keeps the records; a catalog with a gap is refused.
//!
//! # How the fixtures work
//!
//! [`Sequencer`] is the builder under test. Its artifacts list the sequence number and payload of every
//! record they were built from, so a test can read all artifacts in catalog order and see which records
//! are really there, not just which ranges are claimed. [`Fixture`] plays the runner by hand: it applies
//! records, writes checkpoints, and restarts from a chosen manifest, which is how a crash is staged.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_types::StreamId;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use super::*;
use crate::runner::{MaterializerRunner, RunnerError, RunnerProgress, RunnerSettings};
use crate::test_harness::{Harness, MATERIALIZER, STREAM as STREAM_NAME, append_numbered};

/// The stream the fixture pretends to materialize.
const STREAM: StreamId = StreamId::new(1);

/// How many bytes the builder under test holds before it asks to be sealed.
const SEAL_AT_BYTES: usize = 1_000;

/// A payload size that puts a one-record artifact in the lowest tier and three of them merged in the
/// next: with the twelve bytes of sequence and length in front of each payload, 42 bytes and 126 bytes
/// around the tier boundary of 100.
const SMALL_PAYLOAD_BYTES: usize = 30;

/// The builder under test: an artifact lists the records it was built from.
#[derive(Default)]
struct Sequencer {
    /// The records added since the last seal, already in artifact form.
    pending: BytesMut,
    /// How many records were added since the last seal, where other tasks can read it.
    pending_records: Arc<AtomicU64>,
}

impl Sequencer {
    /// A handle on the number of records added since the last seal.
    fn pending_records(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.pending_records)
    }
}

#[async_trait]
impl ArtifactBuilder for Sequencer {
    fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        for record in committed_records {
            self.pending.put_u64_le(record.sequence.get());
            self.pending
                .put_u32_le(u32::try_from(record.payload.len()).expect("test payloads are small"));
            self.pending.put_slice(&record.payload);
        }
        self.pending_records
            .fetch_add(committed_records.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn should_seal(&self) -> bool {
        self.pending.len() >= SEAL_AT_BYTES
    }

    async fn seal_artifact(
        &mut self,
        _covered_range: SequenceRange,
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let contents = Bytes::copy_from_slice(&self.pending);
        let size_bytes = contents.len() as u64;
        let artifact_key = artifacts.put_artifact("sealed", contents).await?;
        self.pending.clear();
        self.pending_records.store(0, Ordering::Relaxed);
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
        })
    }

    async fn merge_artifacts(
        &mut self,
        inputs: &[CatalogEntry],
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let mut merged = BytesMut::new();
        for input in inputs {
            let contents = artifacts.get_artifact(&input.artifact.artifact_key).await?;
            merged.put_slice(&contents);
        }
        let size_bytes = merged.len() as u64;
        let artifact_key = artifacts.put_artifact("merged", merged.freeze()).await?;
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
        })
    }

    /// Three neighbours are merged; the lowest tier ends at 100 bytes.
    fn compaction_policy(&self) -> CompactionPolicy {
        CompactionPolicy {
            merge_fan_in: 3,
            smallest_tier_bytes: 100,
        }
    }
}

/// An artifact catalog with someone playing the runner.
struct Fixture {
    /// The artifact store, also switched off by the tests.
    artifact_store: MemoryArtifactStore,
    /// The view the current instance writes through.
    artifacts: ArtifactNamespace,
    /// The fencing token of the current instance.
    fencing_token: u64,
    /// The adapter under test.
    catalog: ArtifactCatalog<Sequencer>,
    /// A view of the adapter under test.
    view: CatalogView,
    /// The sequence number the next record gets.
    next_sequence: u64,
}

impl Fixture {
    /// A catalog that has never been checkpointed.
    async fn new() -> Self {
        Self::restored(MemoryArtifactStore::new(), 1, None, 1).await
    }

    /// A catalog restored from `last_checkpoint` by the instance that holds `fencing_token`, about to be
    /// handed the record at `next_sequence`.
    async fn restored(
        artifact_store: MemoryArtifactStore,
        fencing_token: u64,
        last_checkpoint: Option<CheckpointManifest>,
        next_sequence: u64,
    ) -> Self {
        let artifacts =
            ArtifactNamespace::for_runner(Arc::new(artifact_store.clone()), STREAM, "sequencer", fencing_token);
        let catalog = ArtifactCatalog::restore_from_checkpoint(last_checkpoint, &artifacts)
            .await
            .expect("the checkpoint is restorable");
        let view = catalog.view();
        Self {
            artifact_store,
            artifacts,
            fencing_token,
            catalog,
            view,
            next_sequence,
        }
    }

    /// Applies `record_count` records with payloads of `payload_bytes` bytes.
    fn apply(&mut self, record_count: u64, payload_bytes: usize) {
        let mut records = Vec::new();
        for _record in 0..record_count {
            records.push(LogRecord {
                sequence: SequenceNumber::new(self.next_sequence),
                payload: Bytes::from(vec![0xAB; payload_bytes]),
            });
            self.next_sequence += 1;
        }
        self.catalog
            .apply_records(&records)
            .expect("the builder takes every record");
    }

    /// Writes a checkpoint, as the runner does when one is due.
    async fn checkpoint(&mut self) -> CheckpointManifest {
        self.catalog
            .write_checkpoint(&self.artifacts)
            .await
            .expect("the checkpoint is written")
    }

    /// Applies one small record and writes a checkpoint: one more artifact of the lowest tier.
    async fn seal_one_small_artifact(&mut self) -> CheckpointManifest {
        self.apply(1, SMALL_PAYLOAD_BYTES);
        self.checkpoint().await
    }

    /// Starts over as the next instance would after a crash: from the last committed checkpoint, with
    /// the next fencing token, and with the records after that checkpoint still to come.
    async fn restart_from(self, committed: Option<(CheckpointManifest, u64)>) -> Self {
        let (last_checkpoint, next_sequence) = match committed {
            Some((manifest, checkpoint)) => (Some(manifest), checkpoint + 1),
            None => (None, 1),
        };
        Self::restored(
            self.artifact_store,
            self.fencing_token + 1,
            last_checkpoint,
            next_sequence,
        )
        .await
    }

    /// The ranges of the catalog, in order, as pairs of first and last sequence.
    fn ranges(&self) -> Vec<(u64, u64)> {
        let mut ranges = Vec::new();
        for entry in self.view.entries() {
            ranges.push((entry.covered_range.first().get(), entry.covered_range.last().get()));
        }
        ranges
    }

    /// The sequence numbers of the records the artifacts of the catalog really hold, read in catalog
    /// order.
    async fn sequences_in_the_artifacts(&self) -> Vec<u64> {
        let mut sequences = Vec::new();
        for entry in self.view.entries() {
            let mut unread = self
                .artifacts
                .get_artifact(&entry.artifact.artifact_key)
                .await
                .expect("a catalogued artifact exists");
            while unread.has_remaining() {
                sequences.push(unread.get_u64_le());
                let payload_bytes = unread.get_u32_le() as usize;
                unread.advance(payload_bytes);
            }
        }
        sequences
    }
}

/// The sequence numbers from 1 to `last`.
fn sequences_through(last: u64) -> Vec<u64> {
    (1..=last).collect()
}

// ---- sealing ----

#[tokio::test]
async fn catalog_without_records_is_empty() {
    let mut fixture = Fixture::new().await;

    let manifest = fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), []);
    assert_eq!(manifest.artifact_keys, Vec::<String>::new());
}

#[tokio::test]
async fn checkpoint_seals_the_records_applied_into_one_artifact() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);

    fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [(1, 5)]);
}

#[tokio::test]
async fn sealed_artifact_holds_exactly_the_records_of_its_range() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);

    fixture.checkpoint().await;

    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(5));
}

#[tokio::test]
async fn next_artifact_starts_right_after_the_previous_one() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);

    fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [(1, 5), (6, 8)]);
}

#[tokio::test]
async fn checkpoint_without_new_records_adds_no_artifact() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    let first_manifest = fixture.checkpoint().await;

    let second_manifest = fixture.checkpoint().await;

    assert_eq!(second_manifest, first_manifest);
}

#[tokio::test]
async fn manifest_names_every_artifact_of_the_catalog() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);

    let manifest = fixture.checkpoint().await;

    let mut catalogued_keys = Vec::new();
    for entry in fixture.view.entries() {
        catalogued_keys.push(entry.artifact.artifact_key);
    }
    assert_eq!(manifest.artifact_keys, catalogued_keys);
}

#[tokio::test]
async fn builder_with_enough_records_asks_for_a_checkpoint() {
    let mut fixture = Fixture::new().await;

    fixture.apply(1, SEAL_AT_BYTES);

    assert!(fixture.catalog.wants_checkpoint());
}

#[tokio::test]
async fn builder_with_few_records_does_not_ask_for_a_checkpoint() {
    let mut fixture = Fixture::new().await;

    fixture.apply(1, 0);

    assert!(!fixture.catalog.wants_checkpoint());
}

#[tokio::test]
async fn sealed_builder_no_longer_asks_for_a_checkpoint() {
    let mut fixture = Fixture::new().await;
    fixture.apply(1, SEAL_AT_BYTES);

    fixture.checkpoint().await;

    assert!(!fixture.catalog.wants_checkpoint());
}

// ---- crash and replay ----

#[tokio::test]
async fn crash_before_the_first_commit_replays_everything_into_a_gapless_catalog() {
    let mut fixture = Fixture::new().await;
    fixture.apply(4, 0);
    fixture.checkpoint().await;

    let mut replayed = fixture.restart_from(None).await;
    replayed.apply(6, 0);
    replayed.checkpoint().await;

    assert_eq!(replayed.ranges(), [(1, 6)]);
    assert_eq!(replayed.sequences_in_the_artifacts().await, sequences_through(6));
}

#[tokio::test]
async fn crash_after_an_uncommitted_checkpoint_replays_into_a_gapless_catalog() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    let committed = fixture.checkpoint().await;
    fixture.apply(4, 0);
    // This checkpoint is written, and lost: the runner crashes before it commits it.
    fixture.checkpoint().await;

    let mut replayed = fixture.restart_from(Some((committed, 5))).await;
    replayed.apply(7, 0);
    replayed.checkpoint().await;

    assert_eq!(replayed.ranges(), [(1, 5), (6, 12)]);
    assert_eq!(replayed.sequences_in_the_artifacts().await, sequences_through(12));
}

#[tokio::test]
async fn crash_with_records_never_sealed_replays_them() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    let committed = fixture.checkpoint().await;
    fixture.apply(4, 0);

    let mut replayed = fixture.restart_from(Some((committed, 5))).await;
    replayed.apply(4, 0);
    replayed.checkpoint().await;

    assert_eq!(replayed.sequences_in_the_artifacts().await, sequences_through(9));
}

#[tokio::test]
async fn restored_catalog_is_the_one_that_was_checkpointed() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);
    let manifest = fixture.checkpoint().await;
    let entries_before = fixture.view.entries();

    let restored = fixture.restart_from(Some((manifest, 8))).await;

    assert_eq!(restored.view.entries(), entries_before);
}

// ---- compaction ----

#[tokio::test]
async fn two_small_neighbours_are_left_alone() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(fixture.ranges(), [(1, 1), (2, 2)]);
}

#[tokio::test]
async fn enough_small_neighbours_are_merged_into_one_artifact() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(fixture.ranges(), [(1, 3)]);
}

#[tokio::test]
async fn merged_artifact_holds_the_records_of_all_its_inputs_in_order() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(3));
}

#[tokio::test]
async fn manifest_after_a_merge_no_longer_names_the_inputs() {
    let mut fixture = Fixture::new().await;
    let first_manifest = fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    let merged_manifest = fixture.seal_one_small_artifact().await;

    assert_eq!(merged_manifest.artifact_keys.len(), 1);
    assert!(!merged_manifest.artifact_keys.contains(&first_manifest.artifact_keys[0]));
}

#[tokio::test]
async fn inputs_of_a_merge_stay_in_the_store_for_the_framework_to_delete() {
    let mut fixture = Fixture::new().await;
    let first_manifest = fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    let input = fixture.artifacts.get_artifact(&first_manifest.artifact_keys[0]).await;
    assert!(input.is_ok());
}

#[tokio::test]
async fn merged_artifact_is_not_merged_with_smaller_neighbours() {
    let mut fixture = Fixture::new().await;
    for _artifact in 0..3 {
        fixture.seal_one_small_artifact().await;
    }

    fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    // One artifact of the second tier, then two of the first: no run of three.
    assert_eq!(fixture.ranges(), [(1, 3), (4, 4), (5, 5)]);
}

#[tokio::test]
async fn at_most_one_run_is_merged_per_checkpoint() {
    let mut fixture = Fixture::new().await;
    for _artifact in 0..8 {
        fixture.seal_one_small_artifact().await;
    }
    // Two artifacts of the second tier and two of the first so far.
    let ranges_before = fixture.ranges();

    fixture.seal_one_small_artifact().await;

    // The ninth small artifact completes a run of the first tier, and merging that run completes a run
    // of the second tier. Only the first merge happens now.
    assert_eq!(ranges_before, [(1, 3), (4, 6), (7, 7), (8, 8)]);
    assert_eq!(fixture.ranges(), [(1, 3), (4, 6), (7, 9)]);
}

#[tokio::test]
async fn merged_artifacts_are_merged_again_once_enough_of_them_exist() {
    let mut fixture = Fixture::new().await;
    for _artifact in 0..9 {
        fixture.seal_one_small_artifact().await;
    }

    fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [(1, 9)]);
    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(9));
}

#[tokio::test]
async fn catalog_stays_gapless_through_many_checkpoints_and_merges() {
    let mut fixture = Fixture::new().await;

    for round in 0..40_u64 {
        fixture.apply(round % 3 + 1, SMALL_PAYLOAD_BYTES);
        fixture.checkpoint().await;
    }

    let last_sequence = fixture.next_sequence - 1;
    assert!(is_gapless(&fixture.view.entries()));
    assert_eq!(
        fixture.sequences_in_the_artifacts().await,
        sequences_through(last_sequence)
    );
}

// ---- the view and the stored catalog ----

#[tokio::test]
async fn builder_shows_what_it_shares_about_the_records_not_sealed_yet() {
    let mut fixture = Fixture::new().await;
    let pending_records = fixture.catalog.builder().pending_records();

    fixture.apply(5, 0);

    assert_eq!(pending_records.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn what_the_builder_shares_follows_a_seal() {
    let mut fixture = Fixture::new().await;
    let pending_records = fixture.catalog.builder().pending_records();
    fixture.apply(5, 0);

    fixture.checkpoint().await;

    assert_eq!(pending_records.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn view_finds_the_artifact_that_covers_a_record() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);
    fixture.checkpoint().await;

    let covering = fixture.view.entry_covering(SequenceNumber::new(6));

    let covered_range = covering.map(|entry| entry.covered_range);
    assert_eq!(
        covered_range,
        SequenceRange::new(SequenceNumber::new(6), SequenceNumber::new(8)).ok()
    );
}

#[tokio::test]
async fn view_finds_no_artifact_for_a_record_not_sealed_yet() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);

    assert_eq!(fixture.view.entry_covering(SequenceNumber::new(6)), None);
}

#[tokio::test]
async fn stored_catalog_is_read_back_by_a_process_that_only_queries() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.checkpoint().await;
    fixture.apply(3, 0);
    let manifest = fixture.checkpoint().await;

    let read_by_a_query_node = decode_catalog(&manifest.details);

    assert_eq!(read_by_a_query_node, Ok(fixture.view.entries()));
}

/// A catalog entry that covers `first` to `last` with a made-up artifact.
fn entry(first: u64, last: u64) -> CatalogEntry {
    CatalogEntry {
        covered_range: SequenceRange::new(SequenceNumber::new(first), SequenceNumber::new(last))
            .expect("a valid range"),
        artifact: ArtifactDescriptor {
            artifact_key: format!("00000000000000000001/0000000000-{first}-{last}"),
            size_bytes: 10,
        },
    }
}

#[tokio::test]
async fn catalog_with_a_gap_is_refused_at_restore() {
    let with_a_gap = encode_catalog(&[entry(1, 5), entry(7, 9)]);

    assert!(matches!(
        decode_catalog(&with_a_gap),
        Err(MaterializerError::InvalidManifest { .. })
    ));
}

#[tokio::test]
async fn catalog_with_an_overlap_is_refused_at_restore() {
    let with_an_overlap = encode_catalog(&[entry(1, 5), entry(5, 9)]);

    assert!(matches!(
        decode_catalog(&with_an_overlap),
        Err(MaterializerError::InvalidManifest { .. })
    ));
}

#[tokio::test]
async fn stored_catalog_that_is_cut_short_is_refused() {
    let stored = encode_catalog(&[entry(1, 5), entry(6, 9)]);

    for kept_bytes in 0..stored.len() {
        let decoded = decode_catalog(&stored.slice(..kept_bytes));

        assert!(
            matches!(decoded, Err(MaterializerError::InvalidManifest { .. })),
            "a catalog cut to {kept_bytes} bytes gave {decoded:?}"
        );
    }
}

// ---- failures ----

#[tokio::test]
async fn seal_that_fails_keeps_the_records_for_the_next_checkpoint() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    fixture.artifact_store.set_unavailable(true);
    let failed = fixture.catalog.write_checkpoint(&fixture.artifacts).await;
    fixture.artifact_store.set_unavailable(false);
    fixture.apply(2, 0);

    fixture.checkpoint().await;

    assert!(failed.is_err_and(|error| error.is_transient()));
    assert_eq!(fixture.ranges(), [(1, 7)]);
    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(7));
}

// ---- under the runner ----

/// How long a test waits for the runner to get somewhere. Simulated time.
const PATIENCE: Duration = Duration::from_secs(300);

/// A catalog of [`Sequencer`] artifacts run by the real runner on the test cluster.
struct RunningCatalog {
    /// Where the runner is.
    progress: RunnerProgress,
    /// The catalog the runner maintains.
    view: CatalogView,
    /// The runner's task.
    task: JoinHandle<RunnerError>,
}

impl RunningCatalog {
    /// Starts a runner that checkpoints every ten seconds.
    async fn start(harness: &Harness) -> Self {
        let settings = RunnerSettings {
            checkpoint_after: Duration::from_secs(10),
            ..RunnerSettings::for_materializer(MATERIALIZER)
        };
        let runner =
            MaterializerRunner::<ArtifactCatalog<Sequencer>>::start(harness.dependencies(), STREAM_NAME, settings)
                .await
                .expect("the runner starts");
        Self {
            progress: runner.progress(),
            view: runner.materializer().view(),
            task: tokio::spawn(runner.run()),
        }
    }

    /// Waits until a checkpoint at or above `sequence` is committed.
    async fn committed(&mut self, sequence: u64) {
        timeout(
            PATIENCE,
            self.progress.wait_until_committed(SequenceNumber::new(sequence)),
        )
        .await
        .expect("the checkpoint is committed in time")
        .expect("the runner is still running");
    }

    /// Stops the runner where it is, as a crash would.
    async fn crash(self) {
        self.task.abort();
        drop(self.task.await);
    }
}

#[tokio::test(start_paused = true)]
async fn catalog_run_by_the_runner_is_gapless_across_a_crash() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut first_run = RunningCatalog::start(&harness).await;
    append_numbered(&writer, 10).await;
    first_run.committed(10).await;
    append_numbered(&writer, 5).await;
    first_run.crash().await;

    let mut second_run = RunningCatalog::start(&harness).await;
    append_numbered(&writer, 5).await;
    second_run.committed(20).await;

    let entries = second_run.view.entries();
    let first_covered = entries.first().map(|entry| entry.covered_range.first());
    let last_covered = entries.last().map(|entry| entry.covered_range.last());
    assert!(is_gapless(&entries));
    assert_eq!(first_covered, Some(SequenceNumber::new(1)));
    assert_eq!(last_covered, Some(SequenceNumber::new(20)));
}

#[tokio::test(start_paused = true)]
async fn inputs_of_a_merge_are_deleted_once_the_merging_checkpoint_is_committed() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = RunningCatalog::start(&harness).await;

    for checkpoint in 1..=3 {
        append_numbered(&writer, 1).await;
        running.committed(checkpoint).await;
    }
    // The deletion follows the commit within the same step of the runner.
    sleep(Duration::from_secs(1)).await;

    // The merged artifact and the manifest that names it.
    assert_eq!(running.view.entries().len(), 1);
    assert_eq!(harness.stored_object_count().await, 2);
}

// ---- the compaction rule on its own ----

#[test]
fn artifacts_below_the_first_boundary_are_in_the_lowest_tier() {
    let policy = CompactionPolicy {
        merge_fan_in: 3,
        smallest_tier_bytes: 100,
    };

    assert_eq!(policy.tier_of(0), 0);
    assert_eq!(policy.tier_of(99), 0);
}

#[test]
fn each_tier_is_as_many_times_wider_as_the_fan_in() {
    let policy = CompactionPolicy {
        merge_fan_in: 3,
        smallest_tier_bytes: 100,
    };

    assert_eq!(policy.tier_of(100), 1);
    assert_eq!(policy.tier_of(299), 1);
    assert_eq!(policy.tier_of(300), 2);
    assert_eq!(policy.tier_of(u64::MAX), policy.tier_of(u64::MAX - 1));
}
