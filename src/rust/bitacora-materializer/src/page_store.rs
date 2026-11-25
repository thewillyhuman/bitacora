//! The adapter for engines that update pages in place and log redo records.
//!
//! # Why this file exists
//!
//! Most storage engines think in pages: a record of the log says "change this page like so". Such an
//! engine should not have to implement checkpoints, artifact formats and point-in-time reads itself. It
//! implements [`PageMaterializer`], which only says which pages a record touches and how a record changes
//! one page, and [`PageStore`] turns that into a full [`Materializer`]: it remembers the history of every
//! page and answers "what did this page look like at sequence N" (design, section 11, "Versioned pages").
//!
//! # What it contains
//!
//! - [`PageMaterializer`]: what an engine implements.
//! - [`PageStore`]: the adapter; a [`Materializer`] the runner can run.
//! - [`PageReader`]: a cloneable handle that reads pages while the runner applies records.
//! - [`LayeringPolicy`]: how often full images are written and how many are kept.
//! - [`PageReadError`]: why a page could not be read.
//!
//! # How it works
//!
//! The history of the pages is a stack of immutable layers, oldest first (see `layer.rs` for the bytes):
//!
//! ```text
//! delta 1..40 | delta 41..90 | image at 90 | delta 91..130 | (records 131..150 still in memory)
//! ```
//!
//! - Applying a record appends it to the in-memory chain of every page it touches. Nothing is computed.
//! - Writing a checkpoint flushes the chains into one new delta layer. Once
//!   [`LayeringPolicy::image_after_delta_layers`] delta layers have piled up since the last image, it also
//!   writes an image layer: every page in full, as of the checkpoint. Then it drops the layers that only
//!   reads below the oldest of the [`LayeringPolicy::images_kept`] newest images would need. The lowest
//!   sequence that can still be read is the horizon.
//! - Reading a page as of sequence N takes the page from the newest image at or below N, then applies,
//!   in order, the page's records from the delta layers after that image and from memory, up to N. A read
//!   for a sequence that is not applied yet waits for it, up to a limit.
//!
//! An image layer holds every page, not only the pages that changed. That keeps garbage collection to
//! one rule, "everything older than the oldest kept image goes", at the price of rewriting cold pages. It
//! suits a reference implementation; an engine with a large, mostly cold page space would split images
//! by page range, as Neon does.
//!
//! # Invariants
//!
//! - Layers are immutable and are listed oldest first; a later layer never covers an earlier sequence.
//! - A read at any sequence from the horizon to the last record applied sees exactly the records up to
//!   it, whatever was flushed, imaged or dropped in between.
//! - The lock on the layer list is never held across an await: readers take a snapshot and then fetch.
//! - When a record is applied, only [`PageMaterializer::pages_touched`] sees it, and may refuse it; the
//!   runner then stops. Its effect on a page is computed when the page is read or imaged, because
//!   applying must not do I/O. [`PageMaterializer::apply_record`] should therefore fail only for records
//!   the engine can never have written.
//!
//! # See also
//!
//! - `layer.rs`: the layer file format.
//! - `contract.rs`: the contract this adapter implements.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use async_trait::async_trait;
use bitacora_artifact::ArtifactError;
use bitacora_types::{LogRecord, SequenceNumber};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::timeout;

use crate::contract::{CheckpointManifest, Materializer, MaterializerError};
use crate::layer::{
    LAYER_HEADER_BYTES, LayerDamage, LayerEntry, LayerKind, decode_header, decode_index, decode_records, encode_layer,
    encode_records, verify_blob,
};
use crate::namespace::ArtifactNamespace;

/// The format of the details a page store puts in its checkpoint manifest.
const DETAILS_FORMAT_VERSION: u32 = 1;

/// How a page store shapes its layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LayeringPolicy {
    /// An image layer is written at a checkpoint once this many delta layers exist that are newer than
    /// the newest image. Fewer means faster reads and more bytes written.
    pub image_after_delta_layers: usize,
    /// How many image layers are kept. Reads below the oldest kept image fail with
    /// [`PageReadError::BelowHorizon`]. At least one is always kept.
    pub images_kept: usize,
}

impl Default for LayeringPolicy {
    /// An image after every four delta layers; the two newest images are kept.
    fn default() -> Self {
        Self {
            image_after_delta_layers: 4,
            images_kept: 2,
        }
    }
}

/// What an engine with pages and redo records implements, instead of the whole [`Materializer`] contract.
///
/// The implementation holds no state: it is created with `Default` and only computes. Anything that
/// changes how pages are laid out must be a constant of the type, because it has to be the same in every
/// process that ever reads the artifacts.
///
/// # Examples
///
/// Pages that hold the payloads appended to them, one page per first payload byte:
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use bitacora_artifact::MemoryArtifactStore;
/// use bitacora_materializer::{
///     ArtifactNamespace, Materializer, MaterializerError, PageMaterializer, PageStore,
/// };
/// use bitacora_types::{LogRecord, SequenceNumber, StreamId};
/// use bytes::Bytes;
///
/// #[derive(Default)]
/// struct AppendOnlyPages;
///
/// impl PageMaterializer for AppendOnlyPages {
///     type PageId = u8;
///
///     fn pages_touched(&self, log_record: &LogRecord) -> Result<Vec<u8>, MaterializerError> {
///         match log_record.payload.first() {
///             Some(page_id) => Ok(vec![*page_id]),
///             None => Err(MaterializerError::InvalidRecord {
///                 sequence: log_record.sequence,
///                 reason: "the record names no page".to_owned(),
///             }),
///         }
///     }
///
///     fn apply_record(
///         &self,
///         _page_id: &u8,
///         page_before: Option<&[u8]>,
///         log_record: &LogRecord,
///     ) -> Result<Vec<u8>, MaterializerError> {
///         let mut page = page_before.unwrap_or_default().to_vec();
///         page.extend_from_slice(&log_record.payload[1..]);
///         Ok(page)
///     }
///
///     fn encode_page_id(page_id: &u8) -> Vec<u8> {
///         vec![*page_id]
///     }
///
///     fn decode_page_id(encoded_page_id: &[u8]) -> Option<u8> {
///         encoded_page_id.first().copied()
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Arc::new(MemoryArtifactStore::new());
/// let artifacts = ArtifactNamespace::for_runner(store, StreamId::new(1), "pages", 1);
/// let mut pages = PageStore::<AppendOnlyPages>::restore_from_checkpoint(None, &artifacts).await?;
/// let reader = pages.reader();
///
/// pages.apply_records(&[
///     LogRecord { sequence: SequenceNumber::new(1), payload: Bytes::from_static(b"\x07ab") },
///     LogRecord { sequence: SequenceNumber::new(2), payload: Bytes::from_static(b"\x07cd") },
/// ])?;
///
/// let no_wait = Duration::ZERO;
/// let latest = reader.read_page(&7, SequenceNumber::new(2), no_wait).await?;
/// let earlier = reader.read_page(&7, SequenceNumber::new(1), no_wait).await?;
/// assert_eq!(latest, Some(Bytes::from_static(b"abcd")));
/// assert_eq!(earlier, Some(Bytes::from_static(b"ab")));
/// # Ok(())
/// # }
/// ```
pub trait PageMaterializer: Default + Send + Sync + 'static {
    /// What identifies a page.
    type PageId: Ord + Clone + Send + Sync + 'static;

    /// Which pages does this record change?
    ///
    /// # Returns
    ///
    /// The pages, in any order. Naming a page twice is the same as naming it once. A record may touch no
    /// page at all.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::InvalidRecord`] if the record cannot even be read, for example
    /// because a newer version of the engine wrote it. This is the one check made when a record is
    /// applied: the runner stops instead of going on past a record it does not understand. The design's
    /// signature returns the pages alone; the error is what lets an engine refuse a record in time.
    fn pages_touched(&self, log_record: &LogRecord) -> Result<Vec<Self::PageId>, MaterializerError>;

    /// Produces the next version of one page.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page being changed; one of those [`PageMaterializer::pages_touched`] named.
    /// * `page_before` - The page before the record, or `None` if no record has touched it yet.
    /// * `log_record` - The record to apply.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError::InvalidRecord`] if the record cannot be applied. The record is
    /// committed by then, so this should only happen for records the engine can never have written; the
    /// page becomes unreadable from that sequence on.
    fn apply_record(
        &self,
        page_id: &Self::PageId,
        page_before: Option<&[u8]>,
        log_record: &LogRecord,
    ) -> Result<Vec<u8>, MaterializerError>;

    /// The bytes a page identifier is stored as in layer files. Two identifiers encode to the same bytes
    /// only if they are equal.
    fn encode_page_id(page_id: &Self::PageId) -> Vec<u8>;

    /// The reverse of [`PageMaterializer::encode_page_id`].
    ///
    /// # Returns
    ///
    /// `None` if the bytes do not encode a page identifier; the layer they come from is then reported as
    /// damaged.
    fn decode_page_id(encoded_page_id: &[u8]) -> Option<Self::PageId>;

    /// How the page store shapes its layers. The default is [`LayeringPolicy::default`].
    fn layering_policy(&self) -> LayeringPolicy {
        LayeringPolicy::default()
    }
}

/// Why a page could not be read.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PageReadError {
    /// The requested sequence was not applied within the time the caller allowed.
    #[error("sequence {as_of_sequence} was not applied in time; the page store is at {applied_sequence}")]
    NotReached {
        /// The sequence the read asked for.
        as_of_sequence: SequenceNumber,
        /// The last record the page store had applied when the read gave up.
        applied_sequence: SequenceNumber,
    },
    /// The requested sequence is older than anything the page store still keeps.
    #[error("pages can no longer be read as of {as_of_sequence}; the oldest readable sequence is {horizon}")]
    BelowHorizon {
        /// The sequence the read asked for.
        as_of_sequence: SequenceNumber,
        /// The lowest sequence pages can still be read at.
        horizon: SequenceNumber,
    },
    /// A layer could not be read or is damaged, or a record could not be applied to the page.
    #[error(transparent)]
    Materializer(#[from] MaterializerError),
}

/// One layer in use: what it covers and where it is.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LayerDescriptor {
    /// What the layer holds.
    kind: LayerKind,
    /// The first sequence number the layer covers.
    first_sequence: SequenceNumber,
    /// The last sequence number the layer covers.
    last_sequence: SequenceNumber,
    /// The artifact the layer is stored as.
    artifact_key: String,
}

/// Everything a page store knows about its pages, guarded by one lock.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StoreState {
    /// The layers in use, oldest first.
    layers: Vec<LayerDescriptor>,
    /// The records applied since the last flush, per encoded page identifier, in sequence order.
    unflushed_by_page: BTreeMap<Bytes, Vec<LogRecord>>,
    /// The last record applied. `ZERO` before the first one.
    applied_sequence: SequenceNumber,
    /// The lowest sequence pages can be read at.
    horizon: SequenceNumber,
}

/// The index of a layer, kept in memory after its first use.
#[derive(Debug)]
struct LoadedIndex {
    /// Where each page's blob lies, by encoded page identifier.
    entries: BTreeMap<Bytes, LayerEntry>,
}

/// What it takes to read one page as of one sequence: decided under the lock, carried out without it.
struct ReadPlan {
    /// The newest image at or below the sequence, if any.
    base_image: Option<LayerDescriptor>,
    /// The sequence the base image is at, or `ZERO` without one. Only later records are applied.
    base_sequence: SequenceNumber,
    /// The delta layers that may hold records of the page after the base, oldest first.
    delta_layers: Vec<LayerDescriptor>,
    /// The page's records still in memory, up to the sequence.
    unflushed_records: Vec<LogRecord>,
}

/// What the store and its readers share.
struct Shared<M: PageMaterializer> {
    /// The engine's logic.
    logic: M,
    /// Where the layers are.
    artifacts: ArtifactNamespace,
    /// The layers and the in-memory chains. Never held across an await.
    state: RwLock<StoreState>,
    /// The last record applied, for reads that wait.
    applied: watch::Sender<SequenceNumber>,
    /// The headers and indexes of layers already read, by artifact key, so that reading a page costs one
    /// fetch per layer. Entries of dropped layers are removed at each checkpoint.
    loaded_indexes: Mutex<BTreeMap<String, Arc<LoadedIndex>>>,
}

/// The error for a layer that does not hold what it should.
fn damaged(artifact_key: &str, layer_damage: LayerDamage) -> MaterializerError {
    MaterializerError::DamagedArtifact {
        artifact_key: artifact_key.to_owned(),
        reason: layer_damage.reason.to_owned(),
    }
}

impl<M: PageMaterializer> Shared<M> {
    /// Locks the state for reading. A panic elsewhere cannot leave it half updated: every update is a
    /// handful of assignments.
    fn read_state(&self) -> RwLockReadGuard<'_, StoreState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the state for writing.
    fn write_state(&self) -> RwLockWriteGuard<'_, StoreState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Decides what a read of `encoded_page_id` as of `as_of_sequence` needs.
    fn plan_read(&self, encoded_page_id: &Bytes, as_of_sequence: SequenceNumber) -> Result<ReadPlan, PageReadError> {
        let state = self.read_state();
        if as_of_sequence < state.horizon {
            return Err(PageReadError::BelowHorizon {
                as_of_sequence,
                horizon: state.horizon,
            });
        }
        let mut base_image = None;
        for layer in &state.layers {
            if layer.kind == LayerKind::Image && layer.last_sequence <= as_of_sequence {
                base_image = Some(layer.clone());
            }
        }
        let base_sequence = base_image
            .as_ref()
            .map_or(SequenceNumber::ZERO, |image| image.last_sequence);
        let mut delta_layers = Vec::new();
        for layer in &state.layers {
            let holds_records_in_reach = layer.kind == LayerKind::Delta
                && layer.last_sequence > base_sequence
                && layer.first_sequence <= as_of_sequence;
            if holds_records_in_reach {
                delta_layers.push(layer.clone());
            }
        }
        let mut unflushed_records = Vec::new();
        if let Some(chain) = state.unflushed_by_page.get(encoded_page_id) {
            for record in chain {
                if record.sequence <= as_of_sequence {
                    unflushed_records.push(record.clone());
                }
            }
        }
        Ok(ReadPlan {
            base_image,
            base_sequence,
            delta_layers,
            unflushed_records,
        })
    }

    /// Carries out a read plan: fetches the page's base and records and folds them together.
    async fn read_planned_page(
        &self,
        page_id: &M::PageId,
        encoded_page_id: &Bytes,
        as_of_sequence: SequenceNumber,
        plan: &ReadPlan,
    ) -> Result<Option<Bytes>, MaterializerError> {
        let mut page = None;
        if let Some(image) = &plan.base_image {
            page = self.read_blob(&image.artifact_key, encoded_page_id).await?;
        }
        for delta in &plan.delta_layers {
            let Some(blob) = self.read_blob(&delta.artifact_key, encoded_page_id).await? else {
                continue;
            };
            let records = decode_records(&blob).map_err(|layer_damage| damaged(&delta.artifact_key, layer_damage))?;
            for record in &records {
                let is_in_reach = record.sequence > plan.base_sequence && record.sequence <= as_of_sequence;
                if is_in_reach {
                    page = Some(self.apply_to_page(page_id, page.as_ref(), record)?);
                }
            }
        }
        for record in &plan.unflushed_records {
            page = Some(self.apply_to_page(page_id, page.as_ref(), record)?);
        }
        Ok(page)
    }

    /// The page after `record`, given the page before it.
    fn apply_to_page(
        &self,
        page_id: &M::PageId,
        page_before: Option<&Bytes>,
        record: &LogRecord,
    ) -> Result<Bytes, MaterializerError> {
        let page_before = page_before.map(|page| &page[..]);
        let page_after = self.logic.apply_record(page_id, page_before, record)?;
        Ok(Bytes::from(page_after))
    }

    /// The header and index of a layer, fetched once and then served from memory.
    async fn load_index(&self, artifact_key: &str) -> Result<Arc<LoadedIndex>, MaterializerError> {
        let already_loaded = self
            .loaded_indexes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(artifact_key)
            .cloned();
        if let Some(loaded) = already_loaded {
            return Ok(loaded);
        }
        let header_bytes = self
            .artifacts
            .get_artifact_range(artifact_key, 0..LAYER_HEADER_BYTES as u64)
            .await?;
        let header = decode_header(&header_bytes).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let index_bytes = self
            .artifacts
            .get_artifact_range(artifact_key, header.index_range())
            .await?;
        let entries =
            decode_index(&header, &index_bytes).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let loaded = Arc::new(LoadedIndex { entries });
        self.loaded_indexes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(artifact_key.to_owned(), Arc::clone(&loaded));
        Ok(loaded)
    }

    /// The verified blob of one page of a layer, or `None` if the layer does not hold the page.
    async fn read_blob(&self, artifact_key: &str, encoded_page_id: &Bytes) -> Result<Option<Bytes>, MaterializerError> {
        let loaded = self.load_index(artifact_key).await?;
        let Some(entry) = loaded.entries.get(encoded_page_id) else {
            return Ok(None);
        };
        let blob = self
            .artifacts
            .get_artifact_range(artifact_key, entry.blob_range())
            .await?;
        verify_blob(entry, &blob).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        Ok(Some(blob))
    }

    /// Every verified blob of a layer, by encoded page identifier, fetched as one object.
    async fn read_whole_layer(&self, artifact_key: &str) -> Result<BTreeMap<Bytes, Bytes>, MaterializerError> {
        let layer = self.artifacts.get_artifact(artifact_key).await?;
        let as_damage = |layer_damage| damaged(artifact_key, layer_damage);
        let header = decode_header(&layer).map_err(as_damage)?;
        let index_range = header.index_range();
        let index = slice_of(&layer, index_range.start, index_range.end).ok_or_else(|| {
            as_damage(LayerDamage {
                reason: "it is cut short",
            })
        })?;
        let entries = decode_index(&header, &index).map_err(as_damage)?;
        let mut blobs_by_page = BTreeMap::new();
        for (encoded_page_id, entry) in entries {
            let blob_range = entry.blob_range();
            let blob = slice_of(&layer, blob_range.start, blob_range.end).ok_or_else(|| {
                as_damage(LayerDamage {
                    reason: "it is cut short",
                })
            })?;
            verify_blob(&entry, &blob).map_err(as_damage)?;
            blobs_by_page.insert(encoded_page_id, blob);
        }
        Ok(blobs_by_page)
    }

    /// Every page in full as of the end of `layers`: the newest image with all later delta layers
    /// applied. This reads whole layers and holds every page in memory; it is the cost of an image.
    async fn materialize_every_page(
        &self,
        layers: &[LayerDescriptor],
    ) -> Result<BTreeMap<Bytes, Bytes>, MaterializerError> {
        let mut newest_image_position = None;
        for (position, layer) in layers.iter().enumerate() {
            if layer.kind == LayerKind::Image {
                newest_image_position = Some(position);
            }
        }
        let mut pages = BTreeMap::new();
        let mut later_layers = layers;
        if let Some(position) = newest_image_position {
            pages = self.read_whole_layer(&layers[position].artifact_key).await?;
            later_layers = &layers[position + 1..];
        }
        for delta in later_layers {
            let records_by_page = self.read_whole_layer(&delta.artifact_key).await?;
            for (encoded_page_id, blob) in records_by_page {
                let Some(page_id) = M::decode_page_id(&encoded_page_id) else {
                    return Err(MaterializerError::DamagedArtifact {
                        artifact_key: delta.artifact_key.clone(),
                        reason: "a page identifier cannot be decoded".to_owned(),
                    });
                };
                let records =
                    decode_records(&blob).map_err(|layer_damage| damaged(&delta.artifact_key, layer_damage))?;
                let mut page = pages.get(&encoded_page_id).cloned();
                for record in &records {
                    page = Some(self.apply_to_page(&page_id, page.as_ref(), record)?);
                }
                if let Some(page) = page {
                    pages.insert(encoded_page_id, page);
                }
            }
        }
        Ok(pages)
    }
}

/// The part of `bytes` from `start` to `end`, or `None` if it lies outside.
fn slice_of(bytes: &Bytes, start: u64, end: u64) -> Option<Bytes> {
    let start = usize::try_from(start).ok()?;
    let end = usize::try_from(end).ok()?;
    if start > end || end > bytes.len() {
        return None;
    }
    Some(bytes.slice(start..end))
}

/// How many delta layers are newer than the newest image layer.
fn delta_layers_since_the_last_image(layers: &[LayerDescriptor]) -> usize {
    let mut delta_layers = 0;
    for layer in layers {
        match layer.kind {
            LayerKind::Image => delta_layers = 0,
            LayerKind::Delta => delta_layers += 1,
        }
    }
    delta_layers
}

/// Drops the layers that only reads below the oldest of the `images_kept` newest images would need.
///
/// # Returns
///
/// The new horizon if anything was dropped: the sequence of the oldest image kept.
fn drop_layers_no_read_needs(layers: &mut Vec<LayerDescriptor>, images_kept: usize) -> Option<SequenceNumber> {
    let mut image_positions = Vec::new();
    for (position, layer) in layers.iter().enumerate() {
        if layer.kind == LayerKind::Image {
            image_positions.push(position);
        }
    }
    let images_kept = images_kept.max(1);
    let images_dropped = image_positions.len().checked_sub(images_kept)?;
    let oldest_kept_position = image_positions[images_dropped];
    if oldest_kept_position == 0 {
        return None;
    }
    let horizon = layers[oldest_kept_position].last_sequence;
    layers.drain(..oldest_kept_position);
    Some(horizon)
}

/// Serializes what a page store needs to restore: its position, its horizon and its layers.
fn encode_details(state: &StoreState) -> Bytes {
    let mut details = BytesMut::new();
    details.put_u32_le(DETAILS_FORMAT_VERSION);
    details.put_u64_le(state.applied_sequence.get());
    details.put_u64_le(state.horizon.get());
    details.put_u32_le(u32::try_from(state.layers.len()).expect("far fewer than 2^32 layers"));
    for layer in &state.layers {
        details.put_u8(match layer.kind {
            LayerKind::Image => 1,
            LayerKind::Delta => 2,
        });
        details.put_u64_le(layer.first_sequence.get());
        details.put_u64_le(layer.last_sequence.get());
        details.put_u32_le(u32::try_from(layer.artifact_key.len()).expect("an artifact key is short"));
        details.put_slice(layer.artifact_key.as_bytes());
    }
    details.freeze()
}

/// The reverse of [`encode_details`]. The manifest that carries the details is checksummed as a whole,
/// so this only has to refuse what this version of the code did not write.
fn decode_details(details: &Bytes) -> Result<StoreState, MaterializerError> {
    let invalid = |reason: &str| MaterializerError::InvalidManifest {
        reason: format!("the page store details are not valid: {reason}"),
    };
    let cut_short = |_cut_short| invalid("they are cut short");
    let mut unread = details.clone();
    if unread.try_get_u32_le().map_err(cut_short)? != DETAILS_FORMAT_VERSION {
        return Err(invalid("they were written by another format"));
    }
    let applied_sequence = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
    let horizon = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
    let layer_count = unread.try_get_u32_le().map_err(cut_short)?;
    let mut layers = Vec::new();
    for _layer in 0..layer_count {
        let kind = match unread.try_get_u8().map_err(cut_short)? {
            1 => LayerKind::Image,
            2 => LayerKind::Delta,
            _unknown => return Err(invalid("a layer is of an unknown kind")),
        };
        let first_sequence = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
        let last_sequence = SequenceNumber::new(unread.try_get_u64_le().map_err(cut_short)?);
        let key_bytes = unread.try_get_u32_le().map_err(cut_short)? as usize;
        if unread.remaining() < key_bytes {
            return Err(invalid("they are cut short"));
        }
        let Ok(artifact_key) = String::from_utf8(unread.split_to(key_bytes).to_vec()) else {
            return Err(invalid("an artifact key is not text"));
        };
        layers.push(LayerDescriptor {
            kind,
            first_sequence,
            last_sequence,
            artifact_key,
        });
    }
    if unread.has_remaining() {
        return Err(invalid("they hold more than their lengths say"));
    }
    Ok(StoreState {
        layers,
        unflushed_by_page: BTreeMap::new(),
        applied_sequence,
        horizon,
    })
}

/// The adapter that turns a [`PageMaterializer`] into a [`Materializer`] with versioned pages.
///
/// The runner owns the store. Take a [`PageReader`] from it with [`PageStore::reader`] before handing it
/// to the runner, and read pages through that. See [`PageMaterializer`] for an example.
pub struct PageStore<M: PageMaterializer> {
    /// Shared with the readers.
    shared: Arc<Shared<M>>,
}

impl<M: PageMaterializer> PageStore<M> {
    /// A handle that reads pages of this store, from any task, while records are applied.
    #[must_use]
    pub fn reader(&self) -> PageReader<M> {
        PageReader {
            shared: Arc::clone(&self.shared),
        }
    }
}

#[async_trait]
impl<M: PageMaterializer> Materializer for PageStore<M> {
    /// Appends each record to the in-memory chain of every page it touches. No page is computed here.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let Some(last_record) = committed_records.last() else {
            return Ok(());
        };
        // Every record is read before any is taken in, so a record that is refused leaves nothing of
        // its batch behind.
        let mut pages_by_record = Vec::with_capacity(committed_records.len());
        for record in committed_records {
            let mut pages_of_this_record = BTreeSet::new();
            for page_id in self.shared.logic.pages_touched(record)? {
                pages_of_this_record.insert(Bytes::from(M::encode_page_id(&page_id)));
            }
            pages_by_record.push(pages_of_this_record);
        }
        {
            let mut state = self.shared.write_state();
            for (record, pages_of_this_record) in committed_records.iter().zip(pages_by_record) {
                for encoded_page_id in pages_of_this_record {
                    state
                        .unflushed_by_page
                        .entry(encoded_page_id)
                        .or_default()
                        .push(record.clone());
                }
            }
            state.applied_sequence = last_record.sequence;
        }
        self.shared.applied.send_replace(last_record.sequence);
        Ok(())
    }

    /// Flushes the in-memory chains into a delta layer, writes an image layer if the policy asks for
    /// one, drops the layers no read needs any more, and names what is left.
    ///
    /// Nothing changes in memory until every artifact is stored: if a store fails, the chains are still
    /// there and the next attempt writes them again.
    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError> {
        let before = self.shared.read_state().clone();
        let mut layers = before.layers;
        let policy = self.shared.logic.layering_policy();

        if !before.unflushed_by_page.is_empty() {
            let mut records_by_page = BTreeMap::new();
            let mut first_sequence = before.applied_sequence;
            for (encoded_page_id, chain) in &before.unflushed_by_page {
                if let Some(first_record) = chain.first() {
                    first_sequence = first_sequence.min(first_record.sequence);
                }
                records_by_page.insert(encoded_page_id.clone(), encode_records(chain));
            }
            let delta = encode_layer(
                LayerKind::Delta,
                first_sequence,
                before.applied_sequence,
                &records_by_page,
            );
            layers.push(LayerDescriptor {
                kind: LayerKind::Delta,
                first_sequence,
                last_sequence: before.applied_sequence,
                artifact_key: artifacts.put_artifact("delta", delta).await?,
            });
        }

        if delta_layers_since_the_last_image(&layers) >= policy.image_after_delta_layers.max(1) {
            let pages = self.shared.materialize_every_page(&layers).await?;
            let image = encode_layer(
                LayerKind::Image,
                before.applied_sequence,
                before.applied_sequence,
                &pages,
            );
            layers.push(LayerDescriptor {
                kind: LayerKind::Image,
                first_sequence: before.applied_sequence,
                last_sequence: before.applied_sequence,
                artifact_key: artifacts.put_artifact("image", image).await?,
            });
        }
        let horizon = drop_layers_no_read_needs(&mut layers, policy.images_kept).unwrap_or(before.horizon);

        let after = StoreState {
            layers,
            unflushed_by_page: BTreeMap::new(),
            applied_sequence: before.applied_sequence,
            horizon,
        };
        let manifest = CheckpointManifest {
            artifact_keys: after.layers.iter().map(|layer| layer.artifact_key.clone()).collect(),
            details: encode_details(&after),
        };
        self.shared
            .loaded_indexes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|artifact_key, _loaded| manifest.artifact_keys.contains(artifact_key));
        // Records cannot have been applied meanwhile: applying needs the store exclusively, as this does.
        *self.shared.write_state() = after;
        Ok(manifest)
    }

    /// Rebuilds the layer list from the manifest. No layer is read until a page is.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let state = match last_checkpoint {
            Some(manifest) => decode_details(&manifest.details)?,
            None => StoreState {
                layers: Vec::new(),
                unflushed_by_page: BTreeMap::new(),
                applied_sequence: SequenceNumber::ZERO,
                horizon: SequenceNumber::ZERO,
            },
        };
        let (applied, _first_receiver) = watch::channel(state.applied_sequence);
        Ok(Self {
            shared: Arc::new(Shared {
                logic: M::default(),
                artifacts: artifacts.clone(),
                state: RwLock::new(state),
                applied,
                loaded_indexes: Mutex::new(BTreeMap::new()),
            }),
        })
    }
}

/// Reads the pages of a [`PageStore`], as of any sequence it still keeps.
///
/// Clones read the same store. A reader stays usable after the runner that owned the store has stopped;
/// the store then simply no longer advances.
pub struct PageReader<M: PageMaterializer> {
    /// Shared with the store.
    shared: Arc<Shared<M>>,
}

impl<M: PageMaterializer> Clone for PageReader<M> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<M: PageMaterializer> PageReader<M> {
    /// Reads one page as it was right after the record at `as_of_sequence` was applied.
    ///
    /// Waits, up to `wait`, until the store has applied that record. Then it takes the page from the
    /// newest image layer at or below the sequence and applies the page's later records up to the
    /// sequence, from delta layers and from memory. It fetches one blob per layer involved.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page to read.
    /// * `as_of_sequence` - The sequence to read at. `ZERO` reads the state before any record.
    /// * `wait` - How long to wait for the store to reach `as_of_sequence`. `Duration::ZERO` never waits.
    ///
    /// # Returns
    ///
    /// `Some(page)` with the page's bytes, or `None` if no record up to `as_of_sequence` touched it.
    ///
    /// # Errors
    ///
    /// - [`PageReadError::NotReached`] if the store did not apply `as_of_sequence` within `wait`.
    /// - [`PageReadError::BelowHorizon`] if the layers needed for `as_of_sequence` were dropped.
    /// - [`PageReadError::Materializer`] if a layer cannot be read or is damaged, or a record of the
    ///   page cannot be applied.
    pub async fn read_page(
        &self,
        page_id: &M::PageId,
        as_of_sequence: SequenceNumber,
        wait: Duration,
    ) -> Result<Option<Bytes>, PageReadError> {
        self.wait_until_applied(as_of_sequence, wait).await?;
        let encoded_page_id = Bytes::from(M::encode_page_id(page_id));
        let plan = self.shared.plan_read(&encoded_page_id, as_of_sequence)?;
        let read = self
            .shared
            .read_planned_page(page_id, &encoded_page_id, as_of_sequence, &plan)
            .await;
        match read {
            Ok(page) => Ok(page),
            Err(MaterializerError::ArtifactStore(ArtifactError::NotFound { .. }))
                if as_of_sequence < self.horizon() =>
            {
                // A checkpoint dropped a layer of the plan while it was being carried out.
                Err(PageReadError::BelowHorizon {
                    as_of_sequence,
                    horizon: self.horizon(),
                })
            }
            Err(materializer_error) => Err(PageReadError::Materializer(materializer_error)),
        }
    }

    /// The last record the store has applied. `ZERO` before the first one.
    #[must_use]
    pub fn applied_sequence(&self) -> SequenceNumber {
        *self.shared.applied.borrow()
    }

    /// The lowest sequence pages can still be read at.
    #[must_use]
    pub fn horizon(&self) -> SequenceNumber {
        self.shared.read_state().horizon
    }

    /// Waits until the store has applied `as_of_sequence`, for at most `wait`.
    async fn wait_until_applied(&self, as_of_sequence: SequenceNumber, wait: Duration) -> Result<(), PageReadError> {
        let mut applied = self.shared.applied.subscribe();
        let reached = timeout(
            wait,
            applied.wait_for(|applied_sequence| *applied_sequence >= as_of_sequence),
        )
        .await;
        match reached {
            Ok(Ok(_applied_sequence)) => Ok(()),
            // The store was dropped, or the wait is over: either way the sequence did not come.
            Ok(Err(_)) | Err(_) => Err(PageReadError::NotReached {
                as_of_sequence,
                applied_sequence: self.applied_sequence(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "page_store_test.rs"]
mod tests;
