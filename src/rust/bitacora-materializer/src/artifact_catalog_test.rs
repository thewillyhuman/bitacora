//! Tests of the artifact catalog adapter.
//!
//! # What is covered
//!
//! - Sealing: a checkpoint cuts one artifact that covers exactly the records since the last one.
//! - The invariant: the catalog is gapless and without overlap, also after a crash at any point and the
//!   replay that follows, and the artifacts hold exactly the records their ranges say.
//! - Compaction: runs of small neighbours are merged, tiers are respected, one merge per checkpoint.
//! - Restoring, the view, and the catalog as a process that only queries reads it, with what the
//!   builder says about each artifact. The stored form by itself is tested in `artifact_catalog/stored_test.rs`,
//!   and the rule that picks what is merged in `artifact_catalog/compaction_test.rs`.
//! - A builder that cuts at boundaries of its own and saves the records beyond them: what is sealed, what
//!   is saved, that the artifacts do not depend on when checkpoints happen, and restarts.
//! - Failures: a seal or a save that fails keeps the records; a catalog with a gap is refused.
//!
//! # How the fixtures work
//!
//! [`Sequencer`] is the builder under test. It summarizes an artifact as the number of records in it. Its artifacts
//! list the sequence number and payload of every record they were built from, so a test can read all artifacts in
//! catalog order and see which records are really there, not just which ranges are claimed. [`FixedCuts`] is a
//! second builder, which cuts an artifact after every fourth record and saves the rest. [`CatalogFixture`] plays the
//! runner by hand: it applies records, writes checkpoints, and restarts from a chosen manifest, which is how a crash
//! is staged; [`Committed`] is the checkpoint such a restart resumes from.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bitacora_artifact::MemoryArtifactStore;
use bitacora_types::StreamId;
use bytes::{Buf, BufMut, BytesMut};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use super::stored::is_gapless;
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
        let record_count = self.pending_records.swap(0, Ordering::Relaxed);
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary: record_count_summary(record_count),
        })
    }

    async fn merge_artifacts(
        &mut self,
        inputs: &[CatalogEntry],
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let mut merged = BytesMut::new();
        let mut record_count = 0;
        for input in inputs {
            let contents = artifacts.get_artifact(&input.artifact.artifact_key).await?;
            merged.put_slice(&contents);
            record_count += summarized_record_count(&input.artifact);
        }
        let size_bytes = merged.len() as u64;
        let artifact_key = artifacts.put_artifact("merged", merged.freeze()).await?;
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary: record_count_summary(record_count),
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

/// The summary the builder under test gives an artifact: how many records it holds.
fn record_count_summary(record_count: u64) -> Bytes {
    Bytes::copy_from_slice(&record_count.to_le_bytes())
}

/// The number of records an artifact's summary states.
fn summarized_record_count(artifact: &ArtifactDescriptor) -> u64 {
    let stated: [u8; 8] = artifact.summary[..].try_into().expect("a summary of eight bytes");
    u64::from_le_bytes(stated)
}

/// The records one artifact covers, as numbers a test can write down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CoveredRecords {
    /// The first record covered.
    first: u64,
    /// The last record covered.
    last: u64,
}

impl CoveredRecords {
    /// The records `entry` says its artifact covers.
    fn of(entry: &CatalogEntry) -> Self {
        records(entry.covered_range.first().get(), entry.covered_range.last().get())
    }
}

/// The records from `first` to `last`, both included.
const fn records(first: u64, last: u64) -> CoveredRecords {
    CoveredRecords { first, last }
}

/// A checkpoint that was committed before a crash: what the next instance restores from.
struct Committed {
    /// The manifest of the checkpoint.
    manifest: CheckpointManifest,
    /// The last record the checkpoint covers.
    checkpoint: u64,
}

/// A catalog of [`Sequencer`] artifacts with someone playing the runner.
type Fixture = CatalogFixture<Sequencer>;

/// A catalog of [`FixedCuts`] artifacts with someone playing the runner.
type CutFixture = CatalogFixture<FixedCuts>;

/// An artifact catalog with someone playing the runner.
struct CatalogFixture<Builder: ArtifactBuilder> {
    /// The artifact store, also switched off by the tests.
    artifact_store: MemoryArtifactStore,
    /// The view the current instance writes through.
    artifacts: ArtifactNamespace,
    /// The fencing token of the current instance.
    fencing_token: u64,
    /// The adapter under test.
    catalog: ArtifactCatalog<Builder>,
    /// A view of the adapter under test.
    view: CatalogView,
    /// The sequence number the next record gets.
    next_sequence: u64,
}

impl<Builder: ArtifactBuilder> CatalogFixture<Builder> {
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
    async fn restart_from(self, committed: Option<Committed>) -> Self {
        let next_fencing_token = self.fencing_token + 1;
        match committed {
            Some(Committed { manifest, checkpoint }) => {
                Self::restored(self.artifact_store, next_fencing_token, Some(manifest), checkpoint + 1).await
            }
            None => Self::restored(self.artifact_store, next_fencing_token, None, 1).await,
        }
    }

    /// The records each artifact of the catalog covers, in catalog order.
    fn ranges(&self) -> Vec<CoveredRecords> {
        let mut ranges = Vec::new();
        for entry in self.view.entries() {
            ranges.push(CoveredRecords::of(&entry));
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

    /// The bytes of every artifact of the catalog, in catalog order.
    async fn artifact_contents(&self) -> Vec<Bytes> {
        let mut contents = Vec::new();
        for entry in self.view.entries() {
            let artifact = self
                .artifacts
                .get_artifact(&entry.artifact.artifact_key)
                .await
                .expect("a catalogued artifact exists");
            contents.push(artifact);
        }
        contents
    }
}

/// How many records one artifact of [`FixedCuts`] holds.
const RECORDS_PER_CUT: usize = 4;

/// A builder that cuts an artifact after every fourth record, whenever checkpoints happen, and saves
/// the records beyond the last cut. Its artifacts have the layout of [`Sequencer`]'s.
#[derive(Default)]
struct FixedCuts {
    /// The records added and not sealed yet, in order.
    pending: Vec<LogRecord>,
}

impl FixedCuts {
    /// The first `record_count` pending records in artifact form.
    fn encode_pending(&self, record_count: usize) -> Bytes {
        let mut contents = BytesMut::new();
        for record in &self.pending[..record_count] {
            contents.put_u64_le(record.sequence.get());
            contents.put_u32_le(u32::try_from(record.payload.len()).expect("test payloads are small"));
            contents.put_slice(&record.payload);
        }
        contents.freeze()
    }
}

#[async_trait]
impl ArtifactBuilder for FixedCuts {
    fn add_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        self.pending.extend_from_slice(committed_records);
        Ok(())
    }

    fn should_seal(&self) -> bool {
        self.pending.len() >= RECORDS_PER_CUT
    }

    fn seal_boundary(&self) -> Option<SequenceNumber> {
        self.pending.get(RECORDS_PER_CUT - 1).map(|record| record.sequence)
    }

    async fn seal_artifact(
        &mut self,
        covered_range: SequenceRange,
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let record_count = usize::try_from(covered_range.record_count()).expect("a small range");
        let contents = self.encode_pending(record_count);
        let size_bytes = contents.len() as u64;
        let artifact_key = artifacts.put_artifact("sealed", contents).await?;
        drop(self.pending.drain(..record_count));
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary: record_count_summary(covered_range.record_count()),
        })
    }

    async fn save_pending_records(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<Option<ArtifactDescriptor>, MaterializerError> {
        let contents = self.encode_pending(self.pending.len());
        let size_bytes = contents.len() as u64;
        let artifact_key = artifacts.put_artifact("pending", contents).await?;
        Ok(Some(ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary: record_count_summary(self.pending.len() as u64),
        }))
    }

    async fn restore_pending_records(
        &mut self,
        saved: &CatalogEntry,
        artifacts: &ArtifactNamespace,
    ) -> Result<(), MaterializerError> {
        let mut unread = artifacts.get_artifact(&saved.artifact.artifact_key).await?;
        while unread.has_remaining() {
            let sequence = SequenceNumber::new(unread.get_u64_le());
            let payload_bytes = unread.get_u32_le() as usize;
            let payload = unread.split_to(payload_bytes);
            self.pending.push(LogRecord { sequence, payload });
        }
        Ok(())
    }

    async fn merge_artifacts(
        &mut self,
        inputs: &[CatalogEntry],
        artifacts: &ArtifactNamespace,
    ) -> Result<ArtifactDescriptor, MaterializerError> {
        let mut merged = BytesMut::new();
        for input in inputs {
            merged.put_slice(&artifacts.get_artifact(&input.artifact.artifact_key).await?);
        }
        let size_bytes = merged.len() as u64;
        let artifact_key = artifacts.put_artifact("merged", merged.freeze()).await?;
        Ok(ArtifactDescriptor {
            artifact_key,
            size_bytes,
            summary: Bytes::new(),
        })
    }

    /// Nothing is merged, so that the cuts stay visible in the catalog.
    fn compaction_policy(&self) -> CompactionPolicy {
        CompactionPolicy {
            merge_fan_in: usize::MAX,
            smallest_tier_bytes: 100,
        }
    }
}

/// The records the saved pending artifact of `manifest` covers, if the manifest names one.
fn saved_pending_records(manifest: &CheckpointManifest) -> Option<CoveredRecords> {
    let stored = decode_catalog(&manifest.details).expect("a valid catalog");
    let pending = stored.pending?;
    Some(CoveredRecords::of(&pending))
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

    assert_eq!(fixture.ranges(), [records(1, 5)]);
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

    assert_eq!(fixture.ranges(), [records(1, 5), records(6, 8)]);
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

    assert_eq!(replayed.ranges(), [records(1, 6)]);
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

    let mut replayed = fixture
        .restart_from(Some(Committed {
            manifest: committed,
            checkpoint: 5,
        }))
        .await;
    replayed.apply(7, 0);
    replayed.checkpoint().await;

    assert_eq!(replayed.ranges(), [records(1, 5), records(6, 12)]);
    assert_eq!(replayed.sequences_in_the_artifacts().await, sequences_through(12));
}

#[tokio::test]
async fn crash_with_records_never_sealed_replays_them() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);
    let committed = fixture.checkpoint().await;
    fixture.apply(4, 0);

    let mut replayed = fixture
        .restart_from(Some(Committed {
            manifest: committed,
            checkpoint: 5,
        }))
        .await;
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

    let restored = fixture
        .restart_from(Some(Committed {
            manifest,
            checkpoint: 8,
        }))
        .await;

    assert_eq!(restored.view.entries(), entries_before);
}

// ---- compaction ----

#[tokio::test]
async fn two_small_neighbours_are_left_alone() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(fixture.ranges(), [records(1, 1), records(2, 2)]);
}

#[tokio::test]
async fn enough_small_neighbours_are_merged_into_one_artifact() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(fixture.ranges(), [records(1, 3)]);
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
    assert_eq!(fixture.ranges(), [records(1, 3), records(4, 4), records(5, 5)]);
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
    assert_eq!(
        ranges_before,
        [records(1, 3), records(4, 6), records(7, 7), records(8, 8)]
    );
    assert_eq!(fixture.ranges(), [records(1, 3), records(4, 6), records(7, 9)]);
}

#[tokio::test]
async fn merged_artifacts_are_merged_again_once_enough_of_them_exist() {
    let mut fixture = Fixture::new().await;
    for _artifact in 0..9 {
        fixture.seal_one_small_artifact().await;
    }

    fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [records(1, 9)]);
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

    let expected = StoredCatalog {
        entries: fixture.view.entries(),
        pending: None,
    };
    assert_eq!(read_by_a_query_node, Ok(expected));
}

#[tokio::test]
async fn catalog_entry_carries_what_the_builder_says_about_its_artifact() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);

    fixture.checkpoint().await;

    assert_eq!(summarized_record_count(&fixture.view.entries()[0].artifact), 5);
}

#[tokio::test]
async fn summary_of_an_artifact_is_in_the_stored_catalog() {
    let mut fixture = Fixture::new().await;
    fixture.apply(5, 0);

    let manifest = fixture.checkpoint().await;

    let stored_entries = decode_catalog(&manifest.details).expect("a valid catalog");
    assert_eq!(summarized_record_count(&stored_entries.entries[0].artifact), 5);
}

#[tokio::test]
async fn merged_artifact_carries_the_summary_its_merge_gave_it() {
    let mut fixture = Fixture::new().await;
    fixture.seal_one_small_artifact().await;
    fixture.seal_one_small_artifact().await;

    fixture.seal_one_small_artifact().await;

    assert_eq!(summarized_record_count(&fixture.view.entries()[0].artifact), 3);
}

#[tokio::test]
async fn builder_that_names_a_boundary_has_only_the_records_up_to_it_sealed() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);

    fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [records(1, 4)]);
}

#[tokio::test]
async fn sealed_artifact_holds_the_records_up_to_the_boundary_and_no_more() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);

    fixture.checkpoint().await;

    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(4));
}

#[tokio::test]
async fn records_beyond_the_boundary_are_saved() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);

    let manifest = fixture.checkpoint().await;

    assert_eq!(saved_pending_records(&manifest), Some(records(5, 6)));
}

#[tokio::test]
async fn manifest_names_the_saved_pending_records_after_the_artifacts() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);

    let manifest = fixture.checkpoint().await;

    let stored = decode_catalog(&manifest.details).expect("a valid catalog");
    let saved_key = stored.pending.expect("saved records").artifact.artifact_key;
    assert_eq!(
        manifest.artifact_keys,
        [stored.entries[0].artifact.artifact_key.clone(), saved_key]
    );
}

#[tokio::test]
async fn checkpoint_seals_every_artifact_that_is_due() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(9, 0);

    let manifest = fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), [records(1, 4), records(5, 8)]);
    assert_eq!(saved_pending_records(&manifest), Some(records(9, 9)));
}

#[tokio::test]
async fn builder_that_is_not_full_is_saved_and_nothing_is_sealed() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(3, 0);

    let manifest = fixture.checkpoint().await;

    assert_eq!(fixture.ranges(), []);
    assert_eq!(saved_pending_records(&manifest), Some(records(1, 3)));
}

#[tokio::test]
async fn nothing_is_saved_when_the_last_record_ends_an_artifact() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(8, 0);

    let manifest = fixture.checkpoint().await;

    assert_eq!(saved_pending_records(&manifest), None);
}

#[tokio::test]
async fn builder_that_saves_asks_for_a_checkpoint_only_when_an_artifact_is_due() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    fixture.checkpoint().await;

    assert!(!fixture.catalog.wants_checkpoint());
}

#[tokio::test]
async fn artifacts_are_cut_at_the_same_records_whenever_checkpoints_happen() {
    let mut checkpointed_often = CutFixture::new().await;
    for _record in 0..10 {
        checkpointed_often.apply(1, SMALL_PAYLOAD_BYTES);
        checkpointed_often.checkpoint().await;
    }
    let mut checkpointed_once = CutFixture::new().await;
    checkpointed_once.apply(10, SMALL_PAYLOAD_BYTES);

    checkpointed_once.checkpoint().await;

    assert_eq!(checkpointed_often.ranges(), [records(1, 4), records(5, 8)]);
    assert_eq!(checkpointed_once.ranges(), checkpointed_often.ranges());
}

#[tokio::test]
async fn artifacts_hold_the_same_bytes_whenever_checkpoints_happen() {
    let mut checkpointed_often = CutFixture::new().await;
    for _record in 0..10 {
        checkpointed_often.apply(1, SMALL_PAYLOAD_BYTES);
        checkpointed_often.checkpoint().await;
    }
    let mut checkpointed_once = CutFixture::new().await;
    checkpointed_once.apply(10, SMALL_PAYLOAD_BYTES);

    checkpointed_once.checkpoint().await;

    assert_eq!(
        checkpointed_once.artifact_contents().await,
        checkpointed_often.artifact_contents().await
    );
}

#[tokio::test]
async fn restored_builder_holds_the_records_that_were_saved() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    let manifest = fixture.checkpoint().await;
    // The next instance is handed records 7 and 8 only: the log may hold nothing older.
    let mut restarted = fixture
        .restart_from(Some(Committed {
            manifest,
            checkpoint: 6,
        }))
        .await;
    restarted.apply(2, 0);

    restarted.checkpoint().await;

    assert_eq!(restarted.ranges(), [records(1, 4), records(5, 8)]);
    assert_eq!(restarted.sequences_in_the_artifacts().await, sequences_through(8));
}

#[tokio::test]
async fn restart_cuts_the_artifacts_an_uninterrupted_run_cuts() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, SMALL_PAYLOAD_BYTES);
    let manifest = fixture.checkpoint().await;
    let mut restarted = fixture
        .restart_from(Some(Committed {
            manifest,
            checkpoint: 6,
        }))
        .await;
    restarted.apply(4, SMALL_PAYLOAD_BYTES);
    restarted.checkpoint().await;
    let mut uninterrupted = CutFixture::new().await;
    uninterrupted.apply(10, SMALL_PAYLOAD_BYTES);

    uninterrupted.checkpoint().await;

    assert_eq!(
        restarted.artifact_contents().await,
        uninterrupted.artifact_contents().await
    );
}

#[tokio::test]
async fn crash_before_saved_records_are_committed_replays_them_from_the_log() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    fixture.checkpoint().await;
    let mut restarted = fixture.restart_from(None).await;
    restarted.apply(8, 0);

    restarted.checkpoint().await;

    assert_eq!(restarted.ranges(), [records(1, 4), records(5, 8)]);
    assert_eq!(restarted.sequences_in_the_artifacts().await, sequences_through(8));
}

#[tokio::test]
async fn pending_records_are_not_saved_again_while_they_are_the_same() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    let first_manifest = fixture.checkpoint().await;

    let second_manifest = fixture.checkpoint().await;

    assert_eq!(second_manifest, first_manifest);
}

#[tokio::test]
async fn pending_records_are_saved_again_once_there_are_more() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(5, 0);
    let first_manifest = fixture.checkpoint().await;
    fixture.apply(1, 0);

    let second_manifest = fixture.checkpoint().await;

    assert_eq!(saved_pending_records(&second_manifest), Some(records(5, 6)));
    assert_ne!(second_manifest.artifact_keys, first_manifest.artifact_keys);
}

#[tokio::test]
async fn restored_pending_records_are_not_saved_again_while_they_are_the_same() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    let manifest = fixture.checkpoint().await;
    let mut restarted = fixture
        .restart_from(Some(Committed {
            manifest: manifest.clone(),
            checkpoint: 6,
        }))
        .await;

    let manifest_after_restart = restarted.checkpoint().await;

    assert_eq!(manifest_after_restart, manifest);
}

#[tokio::test]
async fn view_lists_sealed_artifacts_and_not_the_saved_pending_records() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);

    let manifest = fixture.checkpoint().await;

    assert_eq!(fixture.view.entries().len() + 1, manifest.artifact_keys.len());
}

#[tokio::test]
async fn checkpoint_with_saved_records_is_refused_by_a_builder_that_never_saves() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(6, 0);
    let manifest = fixture.checkpoint().await;

    let restored = ArtifactCatalog::<Sequencer>::restore_from_checkpoint(Some(manifest), &fixture.artifacts).await;

    assert!(matches!(restored, Err(MaterializerError::InvalidManifest { .. })));
}

#[tokio::test]
async fn save_that_fails_leaves_the_builder_and_the_catalog_as_they_were() {
    let mut fixture = CutFixture::new().await;
    fixture.apply(3, 0);
    fixture.artifact_store.set_unavailable(true);
    let failed = fixture.catalog.write_checkpoint(&fixture.artifacts).await;
    fixture.artifact_store.set_unavailable(false);
    fixture.apply(1, 0);

    fixture.checkpoint().await;

    assert!(failed.is_err_and(|error| error.is_transient()));
    assert_eq!(fixture.sequences_in_the_artifacts().await, sequences_through(4));
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
    assert_eq!(fixture.ranges(), [records(1, 7)]);
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
    /// Starts a runner of [`Sequencer`] artifacts that checkpoints every ten seconds.
    async fn start(harness: &Harness) -> Self {
        Self::start_with::<Sequencer>(harness).await
    }

    /// Starts a runner of the artifacts of the builder `B` that checkpoints every ten seconds.
    async fn start_with<Builder: ArtifactBuilder>(harness: &Harness) -> Self {
        let settings = RunnerSettings {
            checkpoint_after: Duration::from_secs(10),
            ..RunnerSettings::for_materializer(MATERIALIZER)
        };
        let runner =
            MaterializerRunner::<ArtifactCatalog<Builder>>::start(harness.dependencies(), STREAM_NAME, settings)
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

#[tokio::test(start_paused = true)]
async fn saved_pending_records_carry_a_runner_across_a_crash() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut first_run = RunningCatalog::start_with::<FixedCuts>(&harness).await;
    append_numbered(&writer, 6).await;
    first_run.committed(6).await;
    first_run.crash().await;

    let mut second_run = RunningCatalog::start_with::<FixedCuts>(&harness).await;
    append_numbered(&writer, 2).await;
    second_run.committed(8).await;

    let mut ranges = Vec::new();
    for entry in second_run.view.entries() {
        ranges.push((entry.covered_range.first().get(), entry.covered_range.last().get()));
    }
    assert_eq!(ranges, [(1, 4), (5, 8)]);
}

#[tokio::test(start_paused = true)]
async fn saved_pending_records_of_an_earlier_checkpoint_are_deleted_after_the_next_commit() {
    let harness = Harness::start().await;
    let writer = harness.open_writer().await;
    let mut running = RunningCatalog::start_with::<FixedCuts>(&harness).await;
    append_numbered(&writer, 2).await;
    running.committed(2).await;

    append_numbered(&writer, 1).await;
    running.committed(3).await;
    // The deletion follows the commit within the same step of the runner.
    sleep(Duration::from_secs(1)).await;

    // The records saved at the second checkpoint and the manifest that names them.
    assert_eq!(harness.stored_object_count().await, 2);
}
