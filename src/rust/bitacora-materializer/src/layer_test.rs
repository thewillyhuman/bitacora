//! Tests of the layer file format.
//!
//! # What is covered
//!
//! - Round trips: header, index and every blob of a layer read back as written, for both kinds of layer
//!   and for a layer without pages.
//! - Reading one page: the three fetches a reader makes find exactly that page's blob.
//! - Damage: every flipped bit of a layer is detected by the part that holds it; cut or oversized parts
//!   are refused.
//!
//! # How the fixtures work
//!
//! `sample_layer` is a delta layer with three pages whose blobs are record runs of different lengths, one
//! of them empty. `read_page` reads one page the way the page store does: header, index, blob.

use super::*;

/// The sequence range the sample layer covers.
const FIRST_SEQUENCE: SequenceNumber = SequenceNumber::new(100);

/// The end of the sequence range the sample layer covers.
const LAST_SEQUENCE: SequenceNumber = SequenceNumber::new(199);

/// A record with a short text payload.
fn record(sequence: u64, payload: &'static str) -> LogRecord {
    LogRecord {
        sequence: SequenceNumber::new(sequence),
        payload: Bytes::from_static(payload.as_bytes()),
    }
}

/// The pages of the sample layer: identifier to blob.
fn sample_blobs() -> BTreeMap<Bytes, Bytes> {
    let mut blobs_by_page = BTreeMap::new();
    blobs_by_page.insert(
        Bytes::from_static(b"page-a"),
        encode_records(&[record(100, "first"), record(150, "second")]),
    );
    blobs_by_page.insert(
        Bytes::from_static(b"page-b"),
        encode_records(&[record(120, "only")]),
    );
    blobs_by_page.insert(Bytes::from_static(b"page-c"), encode_records(&[]));
    blobs_by_page
}

/// A delta layer of three pages.
fn sample_layer() -> Bytes {
    encode_layer(
        LayerKind::Delta,
        FIRST_SEQUENCE,
        LAST_SEQUENCE,
        &sample_blobs(),
    )
}

/// The bytes of `layer` in `byte_range`, as a range request would return them.
fn bytes_of(layer: &Bytes, byte_range: &Range<u64>) -> Result<Bytes, LayerDamage> {
    let start = usize::try_from(byte_range.start).expect("test layers are small");
    let end = usize::try_from(byte_range.end).expect("test layers are small");
    if end > layer.len() {
        return Err(damage("a range lies outside the layer"));
    }
    Ok(layer.slice(start..end))
}

/// Reads one page of a layer the way the page store does. `None` if the layer does not hold the page.
fn read_page(layer: &Bytes, encoded_page_id: &'static [u8]) -> Result<Option<Bytes>, LayerDamage> {
    let header = decode_header(layer)?;
    let index = decode_index(&header, &bytes_of(layer, &header.index_range())?)?;
    let Some(entry) = index.get(encoded_page_id) else {
        return Ok(None);
    };
    let blob = bytes_of(layer, &entry.blob_range())?;
    verify_blob(entry, &blob)?;
    Ok(Some(blob))
}

#[test]
fn header_says_what_the_layer_covers() {
    let header = decode_header(&sample_layer()).expect("the header is intact");

    assert_eq!(header.kind, LayerKind::Delta);
    assert_eq!(header.first_sequence, FIRST_SEQUENCE);
    assert_eq!(header.last_sequence, LAST_SEQUENCE);
    assert_eq!(header.page_count, 3);
}

#[test]
fn every_page_reads_back_the_blob_it_was_built_from() {
    let layer = sample_layer();

    for (encoded_page_id, blob) in sample_blobs() {
        let header = decode_header(&layer).expect("the header is intact");
        let index_bytes = bytes_of(&layer, &header.index_range()).expect("the index is there");
        let index = decode_index(&header, &index_bytes).expect("the index is intact");
        let entry = index.get(&encoded_page_id).expect("the page is indexed");

        let read_blob = bytes_of(&layer, &entry.blob_range()).expect("the blob is there");

        assert_eq!(read_blob, blob);
    }
}

#[test]
fn records_of_a_page_read_back_in_order() {
    let blob = read_page(&sample_layer(), b"page-a")
        .expect("the layer is intact")
        .expect("the page is in the layer");

    let records = decode_records(&blob);

    assert_eq!(
        records,
        Ok(vec![record(100, "first"), record(150, "second")])
    );
}

#[test]
fn page_without_records_reads_back_empty() {
    let blob = read_page(&sample_layer(), b"page-c")
        .expect("the layer is intact")
        .expect("the page is in the layer");

    assert_eq!(decode_records(&blob), Ok(Vec::new()));
}

#[test]
fn page_the_layer_does_not_hold_is_not_found() {
    assert_eq!(read_page(&sample_layer(), b"page-z"), Ok(None));
}

#[test]
fn image_layer_reads_back_its_pages() {
    let mut pages = BTreeMap::new();
    pages.insert(
        Bytes::from_static(b"page-a"),
        Bytes::from_static(b"whole page"),
    );
    let image_at = SequenceNumber::new(500);
    let layer = encode_layer(LayerKind::Image, image_at, image_at, &pages);

    let header = decode_header(&layer).expect("the header is intact");
    let page = read_page(&layer, b"page-a");

    assert_eq!(header.kind, LayerKind::Image);
    assert_eq!(page, Ok(Some(Bytes::from_static(b"whole page"))));
}

#[test]
fn layer_without_pages_is_a_header_alone() {
    let layer = encode_layer(
        LayerKind::Delta,
        FIRST_SEQUENCE,
        LAST_SEQUENCE,
        &BTreeMap::new(),
    );

    assert_eq!(layer.len(), LAYER_HEADER_BYTES);
    assert_eq!(read_page(&layer, b"page-a"), Ok(None));
}

#[test]
fn every_flipped_bit_is_detected_by_a_reader_of_the_page_it_hits() {
    let layer = sample_layer();

    for damaged_offset in 0..layer.len() {
        let mut damaged = layer.to_vec();
        damaged[damaged_offset] ^= 0x04;
        let damaged = Bytes::from(damaged);

        let mut pages_read_intact = 0;
        for encoded_page_id in [&b"page-a"[..], b"page-b", b"page-c"] {
            if read_page(&damaged, encoded_page_id) == read_page(&layer, encoded_page_id) {
                pages_read_intact += 1;
            }
        }

        // Damage to the header or the index fails every read; damage to a blob fails that page's.
        assert!(
            pages_read_intact < 3,
            "damage at byte {damaged_offset} went unnoticed"
        );
    }
}

#[test]
fn bytes_shorter_than_a_header_are_refused() {
    let layer = sample_layer();

    let decoded = decode_header(&layer[..LAYER_HEADER_BYTES - 1]);

    assert_eq!(decoded, Err(damage("it is shorter than a layer header")));
}

#[test]
fn bytes_that_are_not_a_layer_are_refused() {
    let mut not_a_layer = vec![b'x'; LAYER_HEADER_BYTES];
    let checksum = crc32c::crc32c(&not_a_layer[..HEADER_CHECKSUM_OFFSET]);
    not_a_layer[HEADER_CHECKSUM_OFFSET..].copy_from_slice(&checksum.to_le_bytes());

    let decoded = decode_header(&not_a_layer);

    assert_eq!(decoded, Err(damage("it does not start like a layer")));
}

#[test]
fn index_of_another_size_than_the_header_says_is_refused() {
    let layer = sample_layer();
    let header = decode_header(&layer).expect("the header is intact");
    let index = bytes_of(&layer, &header.index_range()).expect("the index is there");

    let decoded = decode_index(&header, &index.slice(..index.len() - 1));

    assert_eq!(
        decoded,
        Err(damage("its index is not the size its header says"))
    );
}

#[test]
fn records_cut_in_the_middle_are_refused() {
    let blob = encode_records(&[record(100, "first"), record(150, "second")]);

    for kept_bytes in 1..blob.len() {
        let is_between_two_records = kept_bytes == RECORD_HEADER_BYTES + "first".len();
        if is_between_two_records {
            continue;
        }

        let decoded = decode_records(&blob.slice(..kept_bytes));

        assert_eq!(
            decoded,
            Err(damage("a page's records are cut short")),
            "a blob cut to {kept_bytes} bytes"
        );
    }
}
