//! Tests of the page store adapter.
//!
//! # What is covered
//!
//! - Reads at the latest and at earlier sequences, from memory, from delta layers and from image layers,
//!   including exactly at the boundaries between them.
//! - The layering policy: when an image is written, which layers are dropped, where the horizon ends up.
//! - Restarting from a checkpoint: identical pages at every readable sequence.
//! - Reads that wait for a sequence, and give up.
//! - Damage and failures: a damaged layer, a record that cannot be applied, an artifact store outage in
//!   the middle of a checkpoint, details of another format.
//!
//! # How the fixtures work
//!
//! [`Journal`] is the engine under test: a page is the list of values appended to it, so a page shows the
//! whole history that reached it and any missing, repeated or misordered record is visible. A record's
//! payload is one value followed by the pages it goes to. [`Fixture`] owns a page store over an in-memory
//! artifact store and plays the runner by hand: it applies records, writes checkpoints and restarts. It
//! also keeps an [`Oracle`], the same history as plain vectors, to compare reads against.

use bitacora_artifact::{ArtifactStore, MemoryArtifactStore};
use bitacora_types::StreamId;
use tokio::time::sleep;

use super::*;
use crate::namespace::namespace_prefix;

/// The value [`Journal`] refuses to append.
const POISON_VALUE: u8 = 0xFF;

/// The stream the fixture pretends to materialize.
const STREAM: StreamId = StreamId::new(1);

/// The name the fixture's materializer goes by.
const MATERIALIZER: &str = "journal";

/// The engine under test: a page is every value appended to it, in order.
#[derive(Default)]
struct Journal;

impl PageMaterializer for Journal {
    type PageId = u8;

    fn pages_touched(&self, log_record: &LogRecord) -> Vec<u8> {
        log_record.payload[1..].to_vec()
    }

    fn apply_record(
        &self,
        _page_id: &u8,
        page_before: Option<&[u8]>,
        log_record: &LogRecord,
    ) -> Result<Vec<u8>, MaterializerError> {
        let value = log_record.payload[0];
        if value == POISON_VALUE {
            return Err(MaterializerError::InvalidRecord {
                sequence: log_record.sequence,
                reason: "the value is poison".to_owned(),
            });
        }
        let mut page = page_before.unwrap_or_default().to_vec();
        page.push(value);
        Ok(page)
    }

    fn encode_page_id(page_id: &u8) -> Vec<u8> {
        vec![*page_id]
    }

    fn decode_page_id(encoded_page_id: &[u8]) -> Option<u8> {
        match encoded_page_id {
            [page_id] => Some(*page_id),
            _not_one_byte => None,
        }
    }

    /// An image after every second delta layer, so that a short test history has several.
    fn layering_policy(&self) -> LayeringPolicy {
        LayeringPolicy {
            image_after_delta_layers: 2,
            images_kept: 2,
        }
    }
}

/// What every page must hold at every sequence, kept as plain vectors.
#[derive(Clone, Default)]
struct Oracle {
    /// Every value appended to each page, with the sequence of the record that appended it.
    values_by_page: BTreeMap<u8, Vec<(u64, u8)>>,
}

impl Oracle {
    /// Remembers that the record at `sequence` appended `value` to `page_ids`.
    fn record(&mut self, sequence: u64, value: u8, page_ids: &[u8]) {
        for page_id in page_ids {
            self.values_by_page
                .entry(*page_id)
                .or_default()
                .push((sequence, value));
        }
    }

    /// The page `page_id` as of `as_of_sequence`: `None` if no record had touched it by then.
    fn page(&self, page_id: u8, as_of_sequence: u64) -> Option<Bytes> {
        let mut page = Vec::new();
        for (sequence, value) in self.values_by_page.get(&page_id)? {
            if *sequence <= as_of_sequence {
                page.push(*value);
            }
        }
        if page.is_empty() {
            return None;
        }
        Some(Bytes::from(page))
    }
}

/// Which byte of a stored layer a test damages.
enum DamagedByte {
    /// A byte in the middle of the header.
    InTheHeader,
    /// The last byte of the layer, which belongs to the blob of its last page.
    Last,
}

/// A page store with someone playing the runner.
struct Fixture {
    /// The artifact store, also read and damaged directly by the tests.
    artifact_store: MemoryArtifactStore,
    /// The view the current instance writes through.
    artifacts: ArtifactNamespace,
    /// The fencing token of the current instance.
    fencing_token: u64,
    /// The store under test.
    pages: PageStore<Journal>,
    /// A reader of the store under test.
    reader: PageReader<Journal>,
    /// The sequence number the next record gets.
    next_sequence: u64,
    /// What the pages must hold.
    oracle: Oracle,
}

impl Fixture {
    /// A store that has never been checkpointed.
    async fn new() -> Self {
        Self::restored(MemoryArtifactStore::new(), 1, None, 1, Oracle::default()).await
    }

    /// A store restored from `last_checkpoint` by the instance that holds `fencing_token`.
    async fn restored(
        artifact_store: MemoryArtifactStore,
        fencing_token: u64,
        last_checkpoint: Option<CheckpointManifest>,
        next_sequence: u64,
        oracle: Oracle,
    ) -> Self {
        let artifacts = ArtifactNamespace::for_runner(
            Arc::new(artifact_store.clone()),
            STREAM,
            MATERIALIZER,
            fencing_token,
        );
        let pages = PageStore::restore_from_checkpoint(last_checkpoint, &artifacts)
            .await
            .expect("the checkpoint is restorable");
        let reader = pages.reader();
        Self {
            artifact_store,
            artifacts,
            fencing_token,
            pages,
            reader,
            next_sequence,
            oracle,
        }
    }

    /// Applies one record that appends `value` to `page_ids`. Returns its sequence number.
    fn apply(&mut self, value: u8, page_ids: &[u8]) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        let mut payload = vec![value];
        payload.extend_from_slice(page_ids);
        self.pages
            .apply_records(&[LogRecord {
                sequence: SequenceNumber::new(sequence),
                payload: Bytes::from(payload),
            }])
            .expect("applying only appends to a chain");
        if value != POISON_VALUE {
            self.oracle.record(sequence, value, page_ids);
        }
        sequence
    }

    /// Applies `record_count` records, each appending a fresh value to one of the pages 1, 2 and 3.
    fn apply_many(&mut self, record_count: u64) {
        for _record in 0..record_count {
            let value = u8::try_from(self.next_sequence % 200).expect("below 200");
            let page_id = u8::try_from(self.next_sequence % 3).expect("below 3") + 1;
            self.apply(value, &[page_id]);
        }
    }

    /// Writes a checkpoint, as the runner does when one is due.
    async fn checkpoint(&mut self) -> CheckpointManifest {
        self.pages
            .write_checkpoint(&self.artifacts)
            .await
            .expect("the checkpoint is written")
    }

    /// The sequence of the last record applied.
    const fn applied(&self) -> u64 {
        self.next_sequence - 1
    }

    /// Reads a page as of a sequence, without waiting.
    async fn page(&self, page_id: u8, as_of_sequence: u64) -> Result<Option<Bytes>, PageReadError> {
        self.reader
            .read_page(
                &page_id,
                SequenceNumber::new(as_of_sequence),
                Duration::ZERO,
            )
            .await
    }

    /// Starts over as the next instance would: from `last_checkpoint`, with the next fencing token,
    /// and with nothing of the old instance's memory.
    async fn restart_from(self, last_checkpoint: CheckpointManifest) -> Self {
        Self::restored(
            self.artifact_store,
            self.fencing_token + 1,
            Some(last_checkpoint),
            self.next_sequence,
            self.oracle,
        )
        .await
    }

    /// Flips one bit of a stored layer.
    async fn damage_layer(&self, artifact_key: &str, damaged_byte: DamagedByte) {
        let object_key = format!("{}{artifact_key}", namespace_prefix(STREAM, MATERIALIZER));
        let mut layer = self
            .artifact_store
            .get_object(&object_key)
            .await
            .expect("the layer exists")
            .to_vec();
        let damaged_offset = match damaged_byte {
            DamagedByte::InTheHeader => LAYER_HEADER_BYTES / 2,
            DamagedByte::Last => layer.len() - 1,
        };
        layer[damaged_offset] ^= 0x01;
        self.artifact_store
            .put_object(&object_key, Bytes::from(layer))
            .await
            .expect("the damaged layer is stored");
    }

    /// Checks every page at every sequence from `first_sequence` to the last one applied against the
    /// oracle, and returns the first difference found, if any.
    async fn first_difference_from(&self, first_sequence: u64) -> Option<String> {
        for as_of_sequence in first_sequence..=self.applied() {
            for page_id in 1..=3 {
                let read = self.page(page_id, as_of_sequence).await;
                let expected = Ok(self.oracle.page(page_id, as_of_sequence));
                if read != expected {
                    return Some(format!(
                        "page {page_id} as of {as_of_sequence} is {read:?}, not {expected:?}"
                    ));
                }
            }
        }
        None
    }
}

/// The artifact keys of a manifest whose label is `label`.
fn keys_labelled(manifest: &CheckpointManifest, label: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for artifact_key in &manifest.artifact_keys {
        if artifact_key.ends_with(label) {
            keys.push(artifact_key.clone());
        }
    }
    keys
}

/// A page holding exactly `values`, as a read returns it: a read of a page that exists is `Some`.
#[allow(clippy::unnecessary_wraps)]
fn page_of(values: &'static [u8]) -> Option<Bytes> {
    Some(Bytes::from_static(values))
}

// ---- reads from memory ----

#[tokio::test]
async fn page_no_record_has_touched_is_absent() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);

    assert_eq!(fixture.page(2, 1).await, Ok(None));
}

#[tokio::test]
async fn page_holds_what_its_records_appended_in_order() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);
    fixture.apply(30, &[1]);

    assert_eq!(fixture.page(1, 3).await, Ok(page_of(&[10, 20, 30])));
}

#[tokio::test]
async fn record_that_touches_two_pages_changes_both() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1, 2]);

    assert_eq!(fixture.page(1, 1).await, Ok(page_of(&[10])));
    assert_eq!(fixture.page(2, 1).await, Ok(page_of(&[10])));
}

#[tokio::test]
async fn page_named_twice_by_one_record_is_changed_once() {
    let mut fixture = Fixture::new().await;
    let mut once = Oracle::default();
    once.record(1, 10, &[1]);
    fixture.apply(10, &[1, 1]);

    assert_eq!(fixture.page(1, 1).await, Ok(once.page(1, 1)));
}

#[tokio::test]
async fn record_that_touches_no_page_still_advances_the_store() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[]);

    assert_eq!(fixture.reader.applied_sequence(), SequenceNumber::new(1));
}

#[tokio::test]
async fn read_as_of_an_earlier_sequence_sees_only_the_records_up_to_it() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);
    fixture.apply(30, &[1]);

    assert_eq!(fixture.page(1, 2).await, Ok(page_of(&[10, 20])));
}

#[tokio::test]
async fn read_before_the_first_record_of_a_page_finds_it_absent() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[2]);

    assert_eq!(fixture.page(2, 1).await, Ok(None));
}

#[tokio::test]
async fn read_as_of_the_empty_position_sees_no_page() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);

    assert_eq!(fixture.page(1, 0).await, Ok(None));
}

// ---- reads from layers ----

#[tokio::test]
async fn flushed_records_are_read_from_their_delta_layer() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);

    fixture.checkpoint().await;

    assert_eq!(fixture.page(1, 2).await, Ok(page_of(&[10, 20])));
}

#[tokio::test]
async fn read_at_the_last_sequence_of_a_delta_layer_sees_the_whole_layer() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);
    fixture.checkpoint().await;
    fixture.apply(30, &[1]);

    assert_eq!(fixture.page(1, 2).await, Ok(page_of(&[10, 20])));
}

#[tokio::test]
async fn read_at_the_first_sequence_after_a_delta_layer_adds_the_record_in_memory() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);
    fixture.checkpoint().await;
    fixture.apply(30, &[1]);

    assert_eq!(fixture.page(1, 3).await, Ok(page_of(&[10, 20, 30])));
}

#[tokio::test]
async fn read_inside_a_delta_layer_stops_at_its_sequence() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[1]);
    fixture.apply(30, &[1]);
    fixture.checkpoint().await;

    assert_eq!(fixture.page(1, 2).await, Ok(page_of(&[10, 20])));
}

#[tokio::test]
async fn every_page_at_every_sequence_is_the_same_whatever_layers_exist() {
    let mut fixture = Fixture::new().await;
    // Delta, delta + image, memory: every kind of boundary lies in sequences 1 to 14.
    fixture.apply_many(4);
    fixture.checkpoint().await;
    fixture.apply_many(5);
    fixture.checkpoint().await;
    fixture.apply_many(3);
    fixture.checkpoint().await;
    fixture.apply_many(2);

    assert_eq!(fixture.first_difference_from(0).await, None);
}

// ---- layering policy ----

#[tokio::test]
async fn first_checkpoint_writes_one_delta_layer() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);

    let manifest = fixture.checkpoint().await;

    assert_eq!(keys_labelled(&manifest, "-delta").len(), 1);
    assert_eq!(keys_labelled(&manifest, "-image").len(), 0);
}

#[tokio::test]
async fn image_layer_is_written_once_enough_delta_layers_exist() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);
    fixture.checkpoint().await;
    fixture.apply_many(3);

    let manifest = fixture.checkpoint().await;

    assert_eq!(keys_labelled(&manifest, "-image").len(), 1);
}

#[tokio::test]
async fn checkpoint_with_nothing_new_names_the_same_layers() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);
    let first_manifest = fixture.checkpoint().await;

    let second_manifest = fixture.checkpoint().await;

    assert_eq!(second_manifest.artifact_keys, first_manifest.artifact_keys);
}

/// Applies records and writes checkpoints until three image layers were written: sequences 1 to 18,
/// images at 6, 12 and 18. Returns the manifests of all six checkpoints.
async fn history_with_three_images(fixture: &mut Fixture) -> Vec<CheckpointManifest> {
    let mut manifests = Vec::new();
    for _checkpoint in 0..6 {
        fixture.apply_many(3);
        manifests.push(fixture.checkpoint().await);
    }
    manifests
}

#[tokio::test]
async fn only_the_newest_images_are_kept() {
    let mut fixture = Fixture::new().await;

    let manifests = history_with_three_images(&mut fixture).await;

    let last_manifest = manifests.last().expect("six checkpoints were written");
    assert_eq!(keys_labelled(last_manifest, "-image").len(), 2);
}

#[tokio::test]
async fn layers_older_than_the_oldest_kept_image_are_dropped() {
    let mut fixture = Fixture::new().await;

    let manifests = history_with_three_images(&mut fixture).await;

    let first_delta_key = &manifests[0].artifact_keys[0];
    let last_manifest = manifests.last().expect("six checkpoints were written");
    assert!(!last_manifest.artifact_keys.contains(first_delta_key));
}

#[tokio::test]
async fn horizon_is_the_empty_position_while_nothing_was_dropped() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);
    fixture.checkpoint().await;

    assert_eq!(fixture.reader.horizon(), SequenceNumber::ZERO);
}

#[tokio::test]
async fn horizon_moves_to_the_oldest_kept_image() {
    let mut fixture = Fixture::new().await;

    history_with_three_images(&mut fixture).await;

    assert_eq!(fixture.reader.horizon(), SequenceNumber::new(12));
}

#[tokio::test]
async fn read_below_the_horizon_is_refused() {
    let mut fixture = Fixture::new().await;
    history_with_three_images(&mut fixture).await;

    let read = fixture.page(1, 11).await;

    assert_eq!(
        read,
        Err(PageReadError::BelowHorizon {
            as_of_sequence: SequenceNumber::new(11),
            horizon: SequenceNumber::new(12),
        })
    );
}

#[tokio::test]
async fn every_sequence_from_the_horizon_on_is_still_readable_after_layers_were_dropped() {
    let mut fixture = Fixture::new().await;
    history_with_three_images(&mut fixture).await;
    fixture.apply_many(2);

    assert_eq!(fixture.first_difference_from(12).await, None);
}

// ---- restarting ----

#[tokio::test]
async fn restored_store_is_at_the_sequence_of_its_checkpoint() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(5);
    let manifest = fixture.checkpoint().await;

    let restarted = fixture.restart_from(manifest).await;

    assert_eq!(restarted.reader.applied_sequence(), SequenceNumber::new(5));
}

#[tokio::test]
async fn restored_store_reads_identical_pages_at_every_sequence() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(4);
    fixture.checkpoint().await;
    fixture.apply_many(5);
    fixture.checkpoint().await;
    fixture.apply_many(3);
    let manifest = fixture.checkpoint().await;

    let restarted = fixture.restart_from(manifest).await;

    assert_eq!(restarted.first_difference_from(0).await, None);
}

#[tokio::test]
async fn restored_store_keeps_its_horizon() {
    let mut fixture = Fixture::new().await;
    let manifests = history_with_three_images(&mut fixture).await;
    let last_manifest = manifests
        .last()
        .expect("six checkpoints were written")
        .clone();

    let restarted = fixture.restart_from(last_manifest).await;

    assert_eq!(restarted.reader.horizon(), SequenceNumber::new(12));
}

#[tokio::test]
async fn restored_store_applies_the_records_after_its_checkpoint_on_top() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(5);
    let manifest = fixture.checkpoint().await;
    let mut restarted = fixture.restart_from(manifest).await;

    restarted.apply_many(4);
    restarted.checkpoint().await;
    restarted.apply_many(2);

    assert_eq!(restarted.first_difference_from(0).await, None);
}

#[tokio::test]
async fn records_applied_after_the_checkpoint_are_gone_after_a_restart_until_replayed() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    let manifest = fixture.checkpoint().await;
    fixture.apply(20, &[1]);

    let restarted = fixture.restart_from(manifest).await;

    assert_eq!(restarted.page(1, 1).await, Ok(page_of(&[10])));
    assert!(matches!(
        restarted.page(1, 2).await,
        Err(PageReadError::NotReached { .. })
    ));
}

#[tokio::test]
async fn details_of_another_format_are_refused_at_restore() {
    let artifacts = ArtifactNamespace::for_runner(
        Arc::new(MemoryArtifactStore::new()),
        STREAM,
        MATERIALIZER,
        1,
    );
    let manifest = CheckpointManifest {
        artifact_keys: Vec::new(),
        details: Bytes::from_static(b"not what a page store writes"),
    };

    let restored = PageStore::<Journal>::restore_from_checkpoint(Some(manifest), &artifacts).await;

    assert!(matches!(
        restored.err(),
        Some(MaterializerError::InvalidManifest { .. })
    ));
}

// ---- reads that wait ----

#[tokio::test(start_paused = true)]
async fn read_of_a_sequence_not_applied_yet_waits_for_it() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    let reader = fixture.reader.clone();
    let waiting_read = tokio::spawn(async move {
        reader
            .read_page(&1, SequenceNumber::new(2), Duration::from_secs(10))
            .await
    });
    sleep(Duration::from_secs(3)).await;

    fixture.apply(20, &[1]);

    let read = waiting_read.await.expect("the read does not panic");
    assert_eq!(read, Ok(page_of(&[10, 20])));
}

#[tokio::test(start_paused = true)]
async fn read_of_a_sequence_that_never_comes_gives_up_after_its_wait() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);

    let read = fixture
        .reader
        .read_page(&1, SequenceNumber::new(2), Duration::from_secs(10))
        .await;

    assert_eq!(
        read,
        Err(PageReadError::NotReached {
            as_of_sequence: SequenceNumber::new(2),
            applied_sequence: SequenceNumber::new(1),
        })
    );
}

// ---- damage and failures ----

#[tokio::test]
async fn damaged_page_in_a_layer_is_detected_when_it_is_read() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    let manifest = fixture.checkpoint().await;
    let delta_key = manifest.artifact_keys[0].clone();

    fixture.damage_layer(&delta_key, DamagedByte::Last).await;

    assert_eq!(
        fixture.page(1, 1).await,
        Err(PageReadError::Materializer(
            MaterializerError::DamagedArtifact {
                artifact_key: delta_key,
                reason: "a page fails its checksum".to_owned(),
            }
        ))
    );
}

#[tokio::test]
async fn damaged_layer_header_is_detected_by_a_store_that_has_not_read_it_yet() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    let manifest = fixture.checkpoint().await;
    let delta_key = manifest.artifact_keys[0].clone();
    fixture
        .damage_layer(&delta_key, DamagedByte::InTheHeader)
        .await;

    let restarted = fixture.restart_from(manifest).await;

    assert_eq!(
        restarted.page(1, 1).await,
        Err(PageReadError::Materializer(
            MaterializerError::DamagedArtifact {
                artifact_key: delta_key,
                reason: "its header fails its checksum".to_owned(),
            }
        ))
    );
}

#[tokio::test]
async fn damaged_layer_fails_the_checkpoint_that_would_build_an_image_from_it() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    let manifest = fixture.checkpoint().await;
    fixture
        .damage_layer(&manifest.artifact_keys[0], DamagedByte::Last)
        .await;
    fixture.apply(20, &[1]);

    let written = fixture.pages.write_checkpoint(&fixture.artifacts).await;

    assert!(matches!(
        written,
        Err(MaterializerError::DamagedArtifact { .. })
    ));
}

#[tokio::test]
async fn record_that_cannot_be_applied_fails_the_reads_of_its_page() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(POISON_VALUE, &[1]);

    let read = fixture.page(1, 2).await;

    assert_eq!(
        read,
        Err(PageReadError::Materializer(
            MaterializerError::InvalidRecord {
                sequence: SequenceNumber::new(2),
                reason: "the value is poison".to_owned(),
            }
        ))
    );
}

#[tokio::test]
async fn record_that_cannot_be_applied_leaves_other_pages_and_earlier_sequences_readable() {
    let mut fixture = Fixture::new().await;
    fixture.apply(10, &[1]);
    fixture.apply(20, &[2]);
    fixture.apply(POISON_VALUE, &[1]);

    assert_eq!(fixture.page(2, 3).await, Ok(page_of(&[20])));
    assert_eq!(fixture.page(1, 2).await, Ok(page_of(&[10])));
}

#[tokio::test]
async fn checkpoint_that_fails_for_an_outage_reports_it_as_transient() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);
    fixture.artifact_store.set_unavailable(true);

    let written = fixture.pages.write_checkpoint(&fixture.artifacts).await;

    assert!(written.is_err_and(|error| error.is_transient()));
}

#[tokio::test]
async fn checkpoint_that_failed_loses_nothing() {
    let mut fixture = Fixture::new().await;
    fixture.apply_many(3);
    fixture.artifact_store.set_unavailable(true);
    drop(fixture.pages.write_checkpoint(&fixture.artifacts).await);
    fixture.artifact_store.set_unavailable(false);
    fixture.apply_many(2);

    let manifest = fixture.checkpoint().await;
    let restarted = fixture.restart_from(manifest).await;

    assert_eq!(restarted.first_difference_from(0).await, None);
}
