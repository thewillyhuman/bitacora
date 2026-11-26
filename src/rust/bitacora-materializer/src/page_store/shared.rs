//! What a page store and its readers share, and every read of a layer.
//!
//! # Why this file exists
//!
//! The runner owns the page store and applies records to it; any number of readers ask for pages at the same time,
//! from other tasks. Both sides work on the same state and on the same layers in the artifact store. This file is the
//! one place where that state is locked and where layers are fetched, so that the two rules that keep readers and the
//! runner out of each other's way are kept in one place: the lock is never held across an await, and a layer's index
//! is fetched once.
//!
//! # What it contains
//!
//! - [`Shared`]: the engine, the artifacts, the locked [`StoreState`] and the indexes of layers already read.
//! - Taking records in and replacing the state after a checkpoint, for the store.
//! - Planning and carrying out a page read, for the readers.
//! - Building every page in full, for an image layer.
//!
//! # How it works
//!
//! A read is two steps. [`Shared::plan_read`] takes the lock, copies what the read needs out of the state and lets
//! go. [`Shared::read_planned_page`] then fetches, without the lock: the page's blob from the base image, the page's
//! records from each delta layer of the plan, and folds them together with the records the plan copied from memory.
//!
//! Reading one page out of a layer takes the layer's header and index first. They are fetched once per layer and
//! kept, so that a later read of the same layer costs one fetch: the blob. A checkpoint that drops layers also drops
//! their indexes.
//!
//! An image is the one operation that reads whole layers: [`Shared::materialize_every_page`] fetches the newest image
//! and every later delta layer as single objects and applies the records page by page.
//!
//! # Invariants
//!
//! - Neither lock is held across an await: every method takes it, copies or changes what it needs, and lets go.
//! - A panic elsewhere cannot leave the state half updated, because every update is a handful of assignments. A
//!   poisoned lock is therefore taken anyway.
//! - Every byte that comes out of a layer was checked against the layer's own checksums before it is used.
//! - [`Shared::take_in`] and the read path run for every batch of records and for every page read. They do no I/O
//!   beyond the fetches a read is made of, and copy only what they hand out. Their cost was not measured.
//!
//! # See also
//!
//! - `state.rs`: the state and the rules decided from it.
//! - `page_store.rs`: the store and the reader that use this.
//! - `layer.rs`: the bytes of a layer and their checks.
//! - `page_store_test.rs`: where all of this is tested, through the store and its reader.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bitacora_types::{LogRecord, SequenceNumber};
use bytes::Bytes;
use tokio::sync::watch;

use super::state::{LayerDescriptor, ReadPlan, StoreState, newest_image_position};
use super::{PageMaterializer, PageReadError};
use crate::contract::MaterializerError;
use crate::layer::{
    LAYER_HEADER_BYTES, LayerDamage, LayerEntry, decode_header, decode_index, decode_records, verify_blob,
};
use crate::namespace::ArtifactNamespace;

/// The index of a layer, kept in memory after its first use.
#[derive(Debug)]
struct LoadedIndex {
    /// Where each page's blob lies, by encoded page identifier.
    entries: BTreeMap<Bytes, LayerEntry>,
}

/// What the store and its readers share.
pub(super) struct Shared<Engine: PageMaterializer> {
    /// The engine's logic.
    engine: Engine,
    /// Where the layers are.
    artifacts: ArtifactNamespace,
    /// The layers and the in-memory chains. Never held across an await.
    state: RwLock<StoreState>,
    /// The last record applied, for reads that wait.
    applied: watch::Sender<SequenceNumber>,
    /// The headers and indexes of layers already read, by artifact key, so that reading a page costs one fetch per
    /// layer. Entries of dropped layers are removed at each checkpoint.
    loaded_indexes: Mutex<BTreeMap<String, Arc<LoadedIndex>>>,
}

impl<Engine: PageMaterializer> Shared<Engine> {
    /// Creates what a store that was just restored shares with its readers.
    ///
    /// # Arguments
    ///
    /// * `artifacts` - Where the layers are read from.
    /// * `state` - The state the store starts from.
    ///
    /// # Returns
    ///
    /// The shared part, with a fresh engine and no layer index loaded yet.
    pub(super) fn new(artifacts: ArtifactNamespace, state: StoreState) -> Self {
        let (applied, _first_receiver) = watch::channel(state.applied_sequence);
        Self {
            engine: Engine::default(),
            artifacts,
            state: RwLock::new(state),
            applied,
            loaded_indexes: Mutex::new(BTreeMap::new()),
        }
    }

    /// The engine's logic.
    ///
    /// # Returns
    ///
    /// The engine this store was created with. It holds no state.
    pub(super) const fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Copies the state as it is now.
    ///
    /// # Returns
    ///
    /// The layers, the records in memory, the position and the horizon, all of the same moment.
    pub(super) fn state_snapshot(&self) -> StoreState {
        self.read_state().clone()
    }

    /// The lowest sequence pages can still be read at.
    ///
    /// # Returns
    ///
    /// The horizon as it is now. A checkpoint may raise it at any moment.
    pub(super) fn horizon(&self) -> SequenceNumber {
        self.read_state().horizon
    }

    /// The last record the store has applied.
    ///
    /// # Returns
    ///
    /// The sequence of that record, or `ZERO` before the first one.
    pub(super) fn applied_sequence(&self) -> SequenceNumber {
        *self.applied.borrow()
    }

    /// Gives a way to wait for the store to apply records.
    ///
    /// # Returns
    ///
    /// A receiver that sees every later value of the last record applied.
    pub(super) fn watch_applied_sequence(&self) -> watch::Receiver<SequenceNumber> {
        self.applied.subscribe()
    }

    /// Appends each record to the in-memory chain of every page it touches, and announces the new position.
    ///
    /// The whole batch becomes visible to readers at once: the chains and the position change under one lock, and
    /// waiting readers are woken after it is released.
    ///
    /// # Arguments
    ///
    /// * `committed_records` - Consecutive records, the first right after the last one applied.
    /// * `pages_by_record` - For each record, in the same order, the encoded identifiers of the pages it touches.
    pub(super) fn take_in(&self, committed_records: &[LogRecord], pages_by_record: Vec<BTreeSet<Bytes>>) {
        let Some(last_record) = committed_records.last() else {
            return;
        };
        {
            let mut state = self.write_state();
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
        self.applied.send_replace(last_record.sequence);
    }

    /// Replaces the state after a checkpoint and forgets the indexes of the layers the checkpoint dropped.
    ///
    /// # Arguments
    ///
    /// * `state_after` - The state as the checkpoint left it: the new layer list, nothing in memory.
    /// * `kept_artifact_keys` - The artifact keys of the layers still in use.
    pub(super) fn replace_state(&self, state_after: StoreState, kept_artifact_keys: &[String]) {
        self.lock_loaded_indexes()
            .retain(|artifact_key, _loaded| kept_artifact_keys.contains(artifact_key));
        *self.write_state() = state_after;
    }

    /// Decides what a read of `encoded_page_id` as of `as_of_sequence` needs.
    ///
    /// # Arguments
    ///
    /// * `encoded_page_id` - The page, as its identifier is stored in layers.
    /// * `as_of_sequence` - The sequence to read at.
    ///
    /// # Returns
    ///
    /// The plan, copied out of the state. The state may change as soon as this returns.
    ///
    /// # Errors
    ///
    /// Returns [`PageReadError::BelowHorizon`] if the layers the read would need were dropped.
    pub(super) fn plan_read(
        &self,
        encoded_page_id: &Bytes,
        as_of_sequence: SequenceNumber,
    ) -> Result<ReadPlan, PageReadError> {
        self.read_state().plan_read(encoded_page_id, as_of_sequence)
    }

    /// Carries out a read plan: fetches the page's base and records and folds them together.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page to read.
    /// * `encoded_page_id` - The same page, as its identifier is stored in layers.
    /// * `as_of_sequence` - The sequence the plan was made for.
    /// * `plan` - What [`Shared::plan_read`] decided.
    ///
    /// # Returns
    ///
    /// The page as of `as_of_sequence`, or `None` if no record up to there touched it.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if a layer of the plan cannot be fetched or is damaged, or if the engine cannot
    /// apply one of the page's records.
    pub(super) async fn read_planned_page(
        &self,
        page_id: &Engine::PageId,
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
            let records = records_of_blob(&delta.artifact_key, &blob)?;
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

    /// Builds every page in full as of the end of `layers`: the newest image with all later delta layers applied.
    ///
    /// This reads whole layers and holds every page in memory; it is the cost of an image.
    ///
    /// # Arguments
    ///
    /// * `layers` - The layers in use, oldest first.
    ///
    /// # Returns
    ///
    /// Every page that exists at the end of `layers`, keyed by its encoded identifier.
    ///
    /// # Errors
    ///
    /// Returns [`MaterializerError`] if a layer cannot be fetched or is damaged, or if the engine cannot apply a
    /// record.
    pub(super) async fn materialize_every_page(
        &self,
        layers: &[LayerDescriptor],
    ) -> Result<BTreeMap<Bytes, Bytes>, MaterializerError> {
        let mut pages_by_id = BTreeMap::new();
        let mut later_layers = layers;
        if let Some(image_position) = newest_image_position(layers) {
            pages_by_id = self.read_whole_layer(&layers[image_position].artifact_key).await?;
            later_layers = &layers[image_position + 1..];
        }
        for delta in later_layers {
            let records_by_page = self.read_whole_layer(&delta.artifact_key).await?;
            for (encoded_page_id, blob) in records_by_page {
                self.apply_blob_to_pages(&delta.artifact_key, encoded_page_id, &blob, &mut pages_by_id)?;
            }
        }
        Ok(pages_by_id)
    }

    /// Applies the records one delta layer holds for one page to that page in `pages_by_id`.
    fn apply_blob_to_pages(
        &self,
        artifact_key: &str,
        encoded_page_id: Bytes,
        blob: &Bytes,
        pages_by_id: &mut BTreeMap<Bytes, Bytes>,
    ) -> Result<(), MaterializerError> {
        let Some(page_id) = Engine::decode_page_id(&encoded_page_id) else {
            return Err(MaterializerError::DamagedArtifact {
                artifact_key: artifact_key.to_owned(),
                reason: "a page identifier cannot be decoded".to_owned(),
            });
        };
        let records = records_of_blob(artifact_key, blob)?;
        let mut page = pages_by_id.get(&encoded_page_id).cloned();
        for record in &records {
            page = Some(self.apply_to_page(&page_id, page.as_ref(), record)?);
        }
        if let Some(page) = page {
            pages_by_id.insert(encoded_page_id, page);
        }
        Ok(())
    }

    /// The page after `record`, given the page before it.
    fn apply_to_page(
        &self,
        page_id: &Engine::PageId,
        page_before: Option<&Bytes>,
        record: &LogRecord,
    ) -> Result<Bytes, MaterializerError> {
        let page_before = page_before.map(|page| &page[..]);
        let page_after = self.engine.apply_record(page_id, page_before, record)?;
        Ok(Bytes::from(page_after))
    }

    /// The header and index of a layer, fetched once and then served from memory.
    async fn load_index(&self, artifact_key: &str) -> Result<Arc<LoadedIndex>, MaterializerError> {
        let already_loaded = self.lock_loaded_indexes().get(artifact_key).cloned();
        if let Some(loaded) = already_loaded {
            return Ok(loaded);
        }
        let header_range = 0..LAYER_HEADER_BYTES as u64;
        let header_bytes = self.artifacts.get_artifact_range(artifact_key, header_range).await?;
        let header = decode_header(&header_bytes).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let index_bytes = self
            .artifacts
            .get_artifact_range(artifact_key, header.index_range())
            .await?;
        let entries =
            decode_index(&header, &index_bytes).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let loaded = Arc::new(LoadedIndex { entries });
        self.lock_loaded_indexes()
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
        let header = decode_header(&layer).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let index = part_of_layer(artifact_key, &layer, header.index_range())?;
        let entries = decode_index(&header, &index).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
        let mut blobs_by_page = BTreeMap::new();
        for (encoded_page_id, entry) in entries {
            let blob = part_of_layer(artifact_key, &layer, entry.blob_range())?;
            verify_blob(&entry, &blob).map_err(|layer_damage| damaged(artifact_key, layer_damage))?;
            blobs_by_page.insert(encoded_page_id, blob);
        }
        Ok(blobs_by_page)
    }

    /// Locks the state for reading.
    fn read_state(&self) -> RwLockReadGuard<'_, StoreState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the state for writing.
    fn write_state(&self) -> RwLockWriteGuard<'_, StoreState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the indexes of the layers already read.
    fn lock_loaded_indexes(&self) -> MutexGuard<'_, BTreeMap<String, Arc<LoadedIndex>>> {
        self.loaded_indexes.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The error for a layer that does not hold what it should.
fn damaged(artifact_key: &str, layer_damage: LayerDamage) -> MaterializerError {
    MaterializerError::DamagedArtifact {
        artifact_key: artifact_key.to_owned(),
        reason: layer_damage.reason.to_owned(),
    }
}

/// The records that one page's blob in a delta layer holds, or the error that names the layer.
fn records_of_blob(artifact_key: &str, blob: &Bytes) -> Result<Vec<LogRecord>, MaterializerError> {
    match decode_records(blob) {
        Ok(records) => Ok(records),
        Err(layer_damage) => Err(damaged(artifact_key, layer_damage)),
    }
}

/// The bytes of `byte_range` of a layer that was fetched whole, or the error for a layer that ends before them.
fn part_of_layer(artifact_key: &str, layer: &Bytes, byte_range: Range<u64>) -> Result<Bytes, MaterializerError> {
    match slice_of(layer, byte_range) {
        Some(part) => Ok(part),
        None => Err(damaged(
            artifact_key,
            LayerDamage {
                reason: "it is cut short",
            },
        )),
    }
}

/// The part of `bytes` that `byte_range` names, or `None` if it lies outside.
fn slice_of(bytes: &Bytes, byte_range: Range<u64>) -> Option<Bytes> {
    let start = usize::try_from(byte_range.start).ok()?;
    let end = usize::try_from(byte_range.end).ok()?;
    if start > end || end > bytes.len() {
        return None;
    }
    Some(bytes.slice(start..end))
}
