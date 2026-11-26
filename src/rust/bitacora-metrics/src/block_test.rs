//! Tests of the block format.
//!
//! # What is covered
//!
//! - Encoding and decoding whole blocks: empty, one sample, several series, and what the summary says.
//! - The layout a reader in another language depends on: where the footer is and how it starts.
//! - Damage: a flipped bit anywhere in a block, a block cut short, another format version.
//! - Selecting series from the index, by labels and by time.
//! - Reading with range reads from an artifact store: which samples come back, how many reads it takes,
//!   and that damage is detected on this path as well.
//!
//! # How the fixtures work
//!
//! `three_series` is a block's worth of samples for `job` = `api`, `db` and `web`. `stored_block` puts an
//! encoded block into a `MemoryArtifactStore` through a real `ArtifactNamespace` and returns what a
//! reader needs to find it again. The samples come from `test_support.rs`.
//!
//! The reads from the artifact store are in `block/fetch.rs`; they are tested here with the format they read.

use bitacora_artifact::{ArtifactError, MemoryArtifactStore};
use bitacora_materializer::{ArtifactNamespace, MaterializerError};

use super::*;
use crate::sample::SeriesSamples;
use crate::selector::Matcher;
use crate::test_support::{TestArtifacts, job, sample, samples_at};

/// Three series: `api` with samples at 10, 20, 30; `db` at 100, 200; `web` at 20, 40.
fn three_series() -> BTreeMap<Labels, Vec<Sample>> {
    let mut samples_by_series = BTreeMap::new();
    samples_by_series.insert(job("api"), samples_at(&[10, 20, 30]));
    samples_by_series.insert(job("db"), samples_at(&[100, 200]));
    samples_by_series.insert(job("web"), samples_at(&[20, 40]));
    samples_by_series
}

/// The index of an encoded block, parsed from its bytes.
fn index_of(block: &Bytes) -> BlockIndex {
    let footer = BlockFooter::decode(block).expect("valid footer");
    let index_range = footer.index_range().expect("valid index range");
    let index_bytes = block.slice(byte_range_as_usize(&index_range));
    BlockIndex::decode(footer, &index_bytes).expect("valid index")
}

/// The selector `job="<job>"`.
fn only_job(job_name: &str) -> Selector {
    Selector::new(vec![Matcher::equal("job", job_name)])
}

/// A block in an artifact store, and what a reader needs to find it.
struct StoredBlock {
    /// The store, for damaging the block and counting reads.
    store: MemoryArtifactStore,
    /// The namespace the block is read through.
    artifacts: ArtifactNamespace,
    /// The block's key in the namespace.
    artifact_key: String,
    /// The block's key in the store.
    object_key: String,
    /// The block's size.
    size_bytes: u64,
}

impl StoredBlock {
    /// The block's index, read with range reads.
    async fn index(&self) -> Result<BlockIndex, MaterializerError> {
        load_block_index(&self.artifacts, &self.artifact_key, self.size_bytes).await
    }

    /// How many range reads the store has served for the block.
    fn range_reads(&self) -> u64 {
        self.store.served_range_reads(&self.object_key)
    }
}

/// Encodes `samples_by_series` as a block and stores it.
async fn stored_block(samples_by_series: &BTreeMap<Labels, Vec<Sample>>) -> StoredBlock {
    let test_artifacts = TestArtifacts::new();
    let encoded = encode_block(samples_by_series);
    let size_bytes = encoded.bytes.len() as u64;
    let artifact_key = test_artifacts
        .namespace
        .put_artifact("block", encoded.bytes)
        .await
        .expect("stored");
    let object_key = TestArtifacts::object_key(&artifact_key);
    StoredBlock {
        store: test_artifacts.store,
        artifacts: test_artifacts.namespace,
        artifact_key,
        object_key,
        size_bytes,
    }
}

/// A series whose chunk is larger than the gap two chunks may have and still be fetched in one read.
fn large_series() -> Vec<Sample> {
    let mut samples = Vec::new();
    let mut value = 1.0_f64;
    for sample_number in 0..12_000_i64 {
        // Irregular times and values, so that every sample costs several bytes.
        value = value * 1.000_173 + 0.37;
        samples.push(sample(sample_number * sample_number, value));
    }
    samples
}

#[test]
fn whole_block_decodes_to_the_series_it_was_made_from() {
    let samples_by_series = three_series();

    let encoded = encode_block(&samples_by_series);

    assert_eq!(decode_whole_block(&encoded.bytes), Ok(samples_by_series));
}

#[test]
fn block_of_no_series_is_a_valid_block() {
    let encoded = encode_block(&BTreeMap::new());

    assert_eq!(decode_whole_block(&encoded.bytes), Ok(BTreeMap::new()));
}

#[test]
fn summary_of_a_block_of_no_series_is_all_zero() {
    let encoded = encode_block(&BTreeMap::new());

    let nothing = BlockSummary {
        series_count: 0,
        sample_count: 0,
        min_time_millis: 0,
        max_time_millis: 0,
    };
    assert_eq!(encoded.summary, nothing);
}

#[test]
fn block_of_no_series_overlaps_no_time_range() {
    let encoded = encode_block(&BTreeMap::new());

    assert!(!encoded.summary.overlaps(TimeRange::ALL));
}

#[test]
fn block_of_one_sample_decodes_to_that_sample() {
    let mut samples_by_series = BTreeMap::new();
    samples_by_series.insert(job("api"), vec![sample(-5, f64::MIN_POSITIVE)]);

    let encoded = encode_block(&samples_by_series);

    assert_eq!(decode_whole_block(&encoded.bytes), Ok(samples_by_series));
}

#[test]
fn summary_of_a_block_of_one_sample_starts_and_ends_at_its_time() {
    let mut samples_by_series = BTreeMap::new();
    samples_by_series.insert(job("api"), vec![sample(-5, 1.0)]);

    let encoded = encode_block(&samples_by_series);

    let one_sample = BlockSummary {
        series_count: 1,
        sample_count: 1,
        min_time_millis: -5,
        max_time_millis: -5,
    };
    assert_eq!(encoded.summary, one_sample);
}

#[test]
fn summary_counts_series_and_samples_and_spans_all_series() {
    let encoded = encode_block(&three_series());

    let expected = BlockSummary {
        series_count: 3,
        sample_count: 7,
        min_time_millis: 10,
        max_time_millis: 200,
    };
    assert_eq!(encoded.summary, expected);
}

#[test]
fn summary_survives_its_catalog_bytes() {
    let summary = encode_block(&three_series()).summary;

    assert_eq!(BlockSummary::decode(&summary.encode()), Some(summary));
}

#[test]
fn summary_in_a_catalog_entry_is_28_bytes() {
    let summary = encode_block(&three_series()).summary;

    assert_eq!(summary.encode().len(), 28);
}

#[test]
fn summary_with_negative_times_survives_its_catalog_bytes() {
    let summary = BlockSummary {
        series_count: u32::MAX,
        sample_count: u64::MAX,
        min_time_millis: i64::MIN,
        max_time_millis: -1,
    };

    assert_eq!(BlockSummary::decode(&summary.encode()), Some(summary));
}

#[test]
fn bytes_of_another_length_are_not_a_summary() {
    let summary = encode_block(&three_series()).summary.encode();

    assert_eq!(BlockSummary::decode(&summary[..27]), None);
}

#[test]
fn no_bytes_are_not_a_summary() {
    assert_eq!(BlockSummary::decode(&[]), None);
}

#[test]
fn footer_repeats_the_summary() {
    let encoded = encode_block(&three_series());

    let footer = BlockFooter::decode(&encoded.bytes).expect("valid footer");

    assert_eq!(footer.summary, encoded.summary);
}

#[test]
fn series_without_samples_is_left_out_of_the_block() {
    let mut samples_by_series = three_series();
    samples_by_series.insert(job("idle"), Vec::new());

    let encoded = encode_block(&samples_by_series);

    assert_eq!(decode_whole_block(&encoded.bytes), Ok(three_series()));
}

#[test]
fn equal_series_encode_to_equal_bytes() {
    let first = encode_block(&three_series());
    let second = encode_block(&three_series());

    assert_eq!(first.bytes, second.bytes);
}

#[test]
fn block_ends_with_a_footer_that_starts_with_the_magic() {
    let encoded = encode_block(&three_series());

    let footer_start = encoded.bytes.len() - BLOCK_FOOTER_BYTES;

    assert_eq!(&encoded.bytes[footer_start..footer_start + 8], b"BITAMBLK");
}

#[test]
fn footer_states_format_version_one_after_the_magic() {
    let encoded = encode_block(&three_series());

    let footer_start = encoded.bytes.len() - BLOCK_FOOTER_BYTES;

    assert_eq!(&encoded.bytes[footer_start + 8..footer_start + 12], [1, 0, 0, 0]);
}

#[test]
fn chunks_start_at_the_first_byte_of_the_block() {
    let encoded = encode_block(&three_series());

    let index = index_of(&encoded.bytes);

    assert_eq!(index.series[0].chunk_offset, 0);
}

#[test]
fn footer_states_the_size_of_the_block() {
    let encoded = encode_block(&three_series());

    let footer = BlockFooter::decode(&encoded.bytes).expect("valid footer");

    assert_eq!(footer.block_bytes(), Some(encoded.bytes.len() as u64));
}

#[test]
fn series_entry_says_when_the_series_starts_and_ends() {
    let encoded = encode_block(&three_series());

    let index = index_of(&encoded.bytes);

    assert_eq!(index.series[1].min_time_millis, 100);
    assert_eq!(index.series[1].max_time_millis, 200);
}

#[test]
fn flipped_bit_anywhere_in_a_block_is_detected() {
    let encoded = encode_block(&three_series());

    for damaged_offset in 0..encoded.bytes.len() {
        let mut damaged = encoded.bytes.to_vec();
        damaged[damaged_offset] ^= 0x10;

        let decoded = decode_whole_block(&Bytes::from(damaged));

        assert!(decoded.is_err(), "damage at byte {damaged_offset} went unnoticed");
    }
}

#[test]
fn block_cut_short_anywhere_is_detected() {
    let encoded = encode_block(&three_series());

    for kept_bytes in 0..encoded.bytes.len() {
        let decoded = decode_whole_block(&encoded.bytes.slice(..kept_bytes));

        assert!(decoded.is_err(), "a block cut to {kept_bytes} bytes was accepted");
    }
}

#[test]
fn block_with_bytes_in_front_is_detected() {
    let encoded = encode_block(&three_series());
    let mut longer = vec![0_u8; 3];
    longer.extend_from_slice(&encoded.bytes);

    let decoded = decode_whole_block(&Bytes::from(longer));

    assert_eq!(
        decoded,
        Err(DamagedBlock::because("it is not the size its footer states"))
    );
}

#[test]
fn bytes_that_are_not_a_block_are_refused() {
    let not_a_block = Bytes::from(vec![0x5a_u8; 200]);

    let decoded = decode_whole_block(&not_a_block);

    assert_eq!(
        decoded,
        Err(DamagedBlock::because("it does not end with a block footer"))
    );
}

#[test]
fn other_format_version_is_named_in_the_refusal() {
    let encoded = encode_block(&three_series());
    let mut other_version = encoded.bytes.to_vec();
    let version_offset = other_version.len() - BLOCK_FOOTER_BYTES + 8;
    other_version[version_offset] = 2;

    let decoded = BlockFooter::decode(&other_version);

    assert_eq!(
        decoded,
        Err(DamagedBlock::because("block format version 2 is not supported"))
    );
}

#[test]
fn index_of_another_size_than_the_footer_states_is_refused() {
    let encoded = encode_block(&three_series());
    let footer = BlockFooter::decode(&encoded.bytes).expect("valid footer");
    let index_range = footer.index_range().expect("valid index range");
    let mut short_range = byte_range_as_usize(&index_range);
    short_range.end -= 1;

    let decoded = BlockIndex::decode(footer, &encoded.bytes.slice(short_range));

    assert_eq!(
        decoded,
        Err(DamagedBlock::because("its index is not the size its footer states"))
    );
}

#[test]
fn selector_without_matchers_selects_every_series() {
    let index = index_of(&encode_block(&three_series()).bytes);

    assert_eq!(index.select(&Selector::all(), TimeRange::ALL), [0, 1, 2]);
}

#[test]
fn equal_matcher_selects_the_series_with_that_label() {
    let index = index_of(&encode_block(&three_series()).bytes);

    assert_eq!(index.select(&only_job("db"), TimeRange::ALL), [1]);
}

#[test]
fn equal_matcher_for_a_value_no_series_has_selects_nothing() {
    let index = index_of(&encode_block(&three_series()).bytes);

    assert_eq!(index.select(&only_job("cache"), TimeRange::ALL), [0_u32; 0]);
}

#[test]
fn not_equal_matcher_selects_every_other_series() {
    let index = index_of(&encode_block(&three_series()).bytes);
    let not_db = Selector::new(vec![Matcher::not_equal("job", "db")]);

    assert_eq!(index.select(&not_db, TimeRange::ALL), [0, 2]);
}

#[test]
fn two_equal_matchers_select_the_series_that_have_both_labels() {
    let mut samples_by_series = BTreeMap::new();
    for (job_name, zone) in [("api", "a"), ("api", "b"), ("web", "a")] {
        let labels = Labels::from_pairs(&[("job", job_name), ("zone", zone)]).expect("valid labels");
        samples_by_series.insert(labels, samples_at(&[1]));
    }
    let index = index_of(&encode_block(&samples_by_series).bytes);
    let api_in_zone_b = Selector::new(vec![Matcher::equal("job", "api"), Matcher::equal("zone", "b")]);

    assert_eq!(index.select(&api_in_zone_b, TimeRange::ALL), [1]);
}

#[test]
fn equal_matcher_for_the_empty_value_selects_series_without_the_label() {
    let mut samples_by_series = three_series();
    let with_zone = Labels::from_pairs(&[("job", "api"), ("zone", "a")]).expect("valid labels");
    samples_by_series.insert(with_zone, samples_at(&[1]));
    let index = index_of(&encode_block(&samples_by_series).bytes);
    let without_zone = Selector::new(vec![Matcher::equal("zone", "")]);

    // `job="api",zone="a"` sorts right after `job="api"`, so it is series 1.
    assert_eq!(index.select(&without_zone, TimeRange::ALL), [0, 2, 3]);
}

#[test]
fn series_with_no_sample_in_the_time_range_is_not_selected() {
    let index = index_of(&encode_block(&three_series()).bytes);
    let early = TimeRange::new(0, 50).expect("valid range");

    assert_eq!(index.select(&Selector::all(), early), [0, 2]);
}

#[test]
fn series_that_ends_exactly_where_the_time_range_starts_is_selected() {
    let index = index_of(&encode_block(&three_series()).bytes);
    let from_the_last_sample_of_api = TimeRange::new(30, 35).expect("valid range");

    assert!(index.select(&Selector::all(), from_the_last_sample_of_api).contains(&0));
}

#[test]
fn series_that_spans_the_time_range_is_selected_even_if_no_sample_lies_in_it() {
    let index = index_of(&encode_block(&three_series()).bytes);
    // `db` has samples at 100 and 200. Only its chunk can tell that none lies between 120 and 180.
    let between_two_samples = TimeRange::new(120, 180).expect("valid range");

    assert_eq!(index.select(&Selector::all(), between_two_samples), [1]);
}

#[test]
fn numbers_in_both_ascending_lists_are_kept() {
    assert_eq!(intersect_ascending(&[1, 3, 5, 8], &[0, 3, 4, 8, 9]), [3, 8]);
}

#[test]
fn lists_with_nothing_in_common_intersect_to_nothing() {
    assert_eq!(intersect_ascending(&[1, 2], &[3, 4]), [0_u32; 0]);
}

#[tokio::test]
async fn index_read_from_the_store_is_the_index_of_the_block() {
    let samples_by_series = three_series();
    let stored = stored_block(&samples_by_series).await;

    let index = stored.index().await;

    assert_eq!(index, Ok(index_of(&encode_block(&samples_by_series).bytes)));
}

#[tokio::test]
async fn index_is_read_in_two_range_reads() {
    let stored = stored_block(&three_series()).await;

    stored.index().await.expect("valid index");

    assert_eq!(stored.range_reads(), 2);
}

#[tokio::test]
async fn selected_series_are_read_with_their_samples() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[1], TimeRange::ALL).await;

    let db = SeriesSamples {
        labels: job("db"),
        samples: samples_at(&[100, 200]),
    };
    assert_eq!(series, Ok(vec![db]));
}

#[tokio::test]
async fn only_samples_in_the_time_range_are_read_ends_included() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");
    let range = TimeRange::new(20, 30).expect("valid range");

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[0], range).await;

    let api = SeriesSamples {
        labels: job("api"),
        samples: samples_at(&[20, 30]),
    };
    assert_eq!(series, Ok(vec![api]));
}

#[tokio::test]
async fn series_whose_samples_all_lie_between_the_ends_of_another_range_is_left_out() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");
    // `db` has samples at 100 and 200: the range lies inside its span and holds none of them.
    let between_samples = TimeRange::new(120, 180).expect("valid range");

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[1], between_samples).await;

    assert_eq!(series, Ok(Vec::new()));
}

#[tokio::test]
async fn neighbouring_chunks_are_fetched_in_one_read() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");
    let reads_before = stored.range_reads();

    read_series(
        &stored.artifacts,
        &stored.artifact_key,
        &index,
        &[0, 1, 2],
        TimeRange::ALL,
    )
    .await
    .expect("read");

    assert_eq!(stored.range_reads() - reads_before, 1);
}

#[tokio::test]
async fn chunks_far_apart_are_fetched_separately() {
    let mut samples_by_series = three_series();
    samples_by_series.insert(job("db"), large_series());
    let stored = stored_block(&samples_by_series).await;
    let index = stored.index().await.expect("valid index");
    let reads_before = stored.range_reads();

    read_series(&stored.artifacts, &stored.artifact_key, &index, &[0, 2], TimeRange::ALL)
        .await
        .expect("read");

    assert_eq!(stored.range_reads() - reads_before, 2);
}

#[tokio::test]
async fn chunk_between_two_selected_ones_is_not_returned() {
    let mut samples_by_series = three_series();
    samples_by_series.insert(job("db"), large_series());
    let stored = stored_block(&samples_by_series).await;
    let index = stored.index().await.expect("valid index");

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[0, 2], TimeRange::ALL)
        .await
        .expect("read");

    let mut read_labels = Vec::new();
    for read in &series {
        read_labels.push(read.labels.clone());
    }
    assert_eq!(read_labels, [job("api"), job("web")]);
}

#[tokio::test]
async fn large_series_survives_a_block() {
    let mut samples_by_series = BTreeMap::new();
    samples_by_series.insert(job("db"), large_series());
    let stored = stored_block(&samples_by_series).await;
    let index = stored.index().await.expect("valid index");

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[0], TimeRange::ALL)
        .await
        .expect("read");

    assert_eq!(series[0].samples, large_series());
}

#[tokio::test]
async fn damaged_chunk_is_detected_when_it_is_read() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");
    stored.store.corrupt_byte(&stored.object_key, 5, 0x01);

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[0], TimeRange::ALL).await;

    let damaged = MaterializerError::DamagedArtifact {
        artifact_key: stored.artifact_key.clone(),
        reason: "a chunk fails its checksum".to_owned(),
    };
    assert_eq!(series, Err(damaged));
}

#[tokio::test]
async fn damage_in_a_chunk_that_is_not_selected_does_not_fail_the_read() {
    let stored = stored_block(&three_series()).await;
    let index = stored.index().await.expect("valid index");
    stored.store.corrupt_byte(&stored.object_key, 5, 0x01);

    let series = read_series(&stored.artifacts, &stored.artifact_key, &index, &[2], TimeRange::ALL).await;

    assert!(series.is_ok());
}

#[tokio::test]
async fn damaged_series_index_is_detected_when_the_index_is_read() {
    let stored = stored_block(&three_series()).await;
    let footer = stored.index().await.expect("valid index").footer;
    let index_start = usize::try_from(footer.series_index_offset).expect("small block");
    stored.store.corrupt_byte(&stored.object_key, index_start + 3, 0x01);

    let index = stored.index().await;

    let damaged = MaterializerError::DamagedArtifact {
        artifact_key: stored.artifact_key.clone(),
        reason: "its series index fails its checksum".to_owned(),
    };
    assert_eq!(index, Err(damaged));
}

#[tokio::test]
async fn damaged_postings_are_detected_when_the_index_is_read() {
    let stored = stored_block(&three_series()).await;
    let footer = stored.index().await.expect("valid index").footer;
    let postings_start = usize::try_from(footer.series_index_offset + footer.series_index_bytes).expect("small block");
    stored.store.corrupt_byte(&stored.object_key, postings_start + 1, 0x01);

    let index = stored.index().await;

    let damaged = MaterializerError::DamagedArtifact {
        artifact_key: stored.artifact_key.clone(),
        reason: "its postings fail their checksum".to_owned(),
    };
    assert_eq!(index, Err(damaged));
}

#[tokio::test]
async fn damaged_footer_is_detected_when_the_index_is_read() {
    let stored = stored_block(&three_series()).await;
    let last_byte = usize::try_from(stored.size_bytes).expect("small block") - 1;
    stored.store.corrupt_byte(&stored.object_key, last_byte, 0x01);

    let index = stored.index().await;

    let damaged = MaterializerError::DamagedArtifact {
        artifact_key: stored.artifact_key.clone(),
        reason: "its footer fails its checksum".to_owned(),
    };
    assert_eq!(index, Err(damaged));
}

#[tokio::test]
async fn block_smaller_than_the_catalog_says_is_detected() {
    let stored = stored_block(&three_series()).await;

    let index = load_block_index(&stored.artifacts, &stored.artifact_key, stored.size_bytes + 1).await;

    assert!(matches!(
        index,
        Err(MaterializerError::ArtifactStore(ArtifactError::RangeOutOfBounds { .. }))
    ));
}

#[tokio::test]
async fn block_larger_than_the_catalog_says_is_detected() {
    let stored = stored_block(&three_series()).await;

    let index = load_block_index(&stored.artifacts, &stored.artifact_key, stored.size_bytes - 1).await;

    assert!(matches!(index, Err(MaterializerError::DamagedArtifact { .. })));
}

#[tokio::test]
async fn size_too_small_for_a_footer_is_refused_without_a_read() {
    let stored = stored_block(&three_series()).await;

    let index = load_block_index(&stored.artifacts, &stored.artifact_key, 10).await;

    let damaged = MaterializerError::DamagedArtifact {
        artifact_key: stored.artifact_key.clone(),
        reason: "it is too short to hold a footer".to_owned(),
    };
    assert_eq!(index, Err(damaged));
}

#[tokio::test]
async fn missing_block_is_reported_as_not_found() {
    let stored = stored_block(&three_series()).await;

    let index = load_block_index(&stored.artifacts, "00000000000000000001/0000000099-block", 200).await;

    assert!(matches!(
        index,
        Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
    ));
}
