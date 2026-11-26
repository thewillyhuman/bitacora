//! What a page store knows about its pages, and everything that is decided from that alone.
//!
//! # Why this file exists
//!
//! Between two checkpoints a page store holds little: the list of layers in use, the records applied since the last
//! flush, how far it has applied and how far back it can still read. Most of what the store does is decided from that
//! alone, without reading a single layer: what a read needs, whether the next checkpoint writes an image, which layers
//! no read needs any more, and what the next instance must be told to carry on. Those are rules over a small value.
//! This file keeps them apart from the I/O around them, so that each can be read, and tested, by itself.
//!
//! # What it contains
//!
//! - [`LayerDescriptor`]: one layer in use.
//! - [`StoreState`]: the layers, the unflushed records, the position and the horizon, with the questions it answers:
//!   [`StoreState::plan_read`] and [`StoreState::delta_layer_of_unflushed_records`].
//! - [`ReadPlan`]: what one read needs.
//! - [`NewLayer`]: a layer that was built and is not stored yet, with [`image_layer_of`].
//! - [`is_image_due`], [`newest_image_position`] and [`drop_layers_no_read_needs`]: the layering policy as rules over
//!   the list.
//! - [`encode_details`] and [`decode_details`]: the state as the details of a checkpoint manifest.
//!
//! # How it works
//!
//! The layers are listed oldest first. A read as of sequence N starts from the newest image at or below N, the base,
//! and needs every delta layer that can hold a record after the base and at or below N, plus the page's records still
//! in memory up to N.
//!
//! The details of a manifest are the state without the records in memory, which a checkpoint has just flushed. All
//! integers are little-endian:
//!
//! ```text
//! details  format version u32 | applied sequence u64 | horizon u64 | layer count u32 | layer*
//! layer    kind u8 | first sequence u64 | last sequence u64 | key length u32 | key
//! ```
//!
//! The manifest that carries the details is checksummed as a whole, so decoding only has to refuse what this version
//! of the code did not write.
//!
//! # Invariants
//!
//! - Nothing here does I/O, waits or reads a clock.
//! - Layers are listed oldest first, and a later layer never covers an earlier sequence.
//! - At least one image is kept, whatever the policy says, once one exists.
//! - [`StoreState::plan_read`] runs under the store's lock on every page read. It walks the layer list twice and
//!   copies only the descriptors and records the read needs; it must stay free of anything slower. Its cost was not
//!   measured.
//! - Decoding never panics, whatever the bytes.
//!
//! # See also
//!
//! - `page_store.rs`: the adapter that owns the state and the policy these rules serve.
//! - `shared.rs`: where the state is locked and where plans are carried out.
//! - `layer.rs`: the bytes of a layer.

use std::collections::BTreeMap;

use bitacora_types::{LogRecord, SequenceNumber};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{LayeringPolicy, PageReadError};
use crate::contract::MaterializerError;
use crate::layer::{LayerKind, encode_layer, encode_records};

/// The format of the details a page store puts in its checkpoint manifest.
const DETAILS_FORMAT_VERSION: u32 = 1;

/// What is wrong with details that end before their lengths say.
const CUT_SHORT: &str = "they are cut short";

/// One layer in use: what it covers and where it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LayerDescriptor {
    /// What the layer holds.
    pub(super) kind: LayerKind,
    /// The first sequence number the layer covers.
    pub(super) first_sequence: SequenceNumber,
    /// The last sequence number the layer covers.
    pub(super) last_sequence: SequenceNumber,
    /// The artifact the layer is stored as.
    pub(super) artifact_key: String,
}

/// A layer that was built and is not stored yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct NewLayer {
    /// What the layer holds.
    pub(super) kind: LayerKind,
    /// The first sequence number the layer covers.
    pub(super) first_sequence: SequenceNumber,
    /// The last sequence number the layer covers.
    pub(super) last_sequence: SequenceNumber,
    /// The whole layer, as it is stored.
    pub(super) contents: Bytes,
}

impl NewLayer {
    /// Describes the layer once it is stored.
    ///
    /// # Arguments
    ///
    /// * `artifact_key` - The key the artifact store gave the layer.
    ///
    /// # Returns
    ///
    /// The entry of the layer in the list of layers in use.
    pub(super) fn stored_as(self, artifact_key: String) -> LayerDescriptor {
        LayerDescriptor {
            kind: self.kind,
            first_sequence: self.first_sequence,
            last_sequence: self.last_sequence,
            artifact_key,
        }
    }
}

/// What it takes to read one page as of one sequence: decided under the lock, carried out without it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ReadPlan {
    /// The newest image at or below the sequence, if any.
    pub(super) base_image: Option<LayerDescriptor>,
    /// The sequence the base image is at, or `ZERO` without one. Only later records are applied.
    pub(super) base_sequence: SequenceNumber,
    /// The delta layers that may hold records of the page after the base, oldest first.
    pub(super) delta_layers: Vec<LayerDescriptor>,
    /// The page's records still in memory, up to the sequence.
    pub(super) unflushed_records: Vec<LogRecord>,
}

/// Everything a page store knows about its pages, guarded by one lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct StoreState {
    /// The layers in use, oldest first.
    pub(super) layers: Vec<LayerDescriptor>,
    /// The records applied since the last flush, per encoded page identifier, in sequence order.
    pub(super) unflushed_by_page: BTreeMap<Bytes, Vec<LogRecord>>,
    /// The last record applied. `ZERO` before the first one.
    pub(super) applied_sequence: SequenceNumber,
    /// The lowest sequence pages can be read at.
    pub(super) horizon: SequenceNumber,
}

impl StoreState {
    /// The state of a store that has never applied a record.
    ///
    /// # Returns
    ///
    /// A state without layers and without records, at the empty position, from which everything is readable.
    pub(super) const fn empty() -> Self {
        Self {
            layers: Vec::new(),
            unflushed_by_page: BTreeMap::new(),
            applied_sequence: SequenceNumber::ZERO,
            horizon: SequenceNumber::ZERO,
        }
    }

    /// Decides what a read of one page as of one sequence needs.
    ///
    /// # Arguments
    ///
    /// * `encoded_page_id` - The page, as its identifier is stored in layers.
    /// * `as_of_sequence` - The sequence to read at. The caller has made sure it is applied.
    ///
    /// # Returns
    ///
    /// The base image, the delta layers after it and the records in memory that together give the page.
    ///
    /// # Errors
    ///
    /// Returns [`PageReadError::BelowHorizon`] if the layers a read at `as_of_sequence` would need were dropped.
    pub(super) fn plan_read(
        &self,
        encoded_page_id: &Bytes,
        as_of_sequence: SequenceNumber,
    ) -> Result<ReadPlan, PageReadError> {
        if as_of_sequence < self.horizon {
            return Err(PageReadError::BelowHorizon {
                as_of_sequence,
                horizon: self.horizon,
            });
        }
        let base_image = self.newest_image_at_or_below(as_of_sequence);
        let base_sequence = match &base_image {
            Some(image) => image.last_sequence,
            None => SequenceNumber::ZERO,
        };
        Ok(ReadPlan {
            delta_layers: self.delta_layers_in_reach(base_sequence, as_of_sequence),
            unflushed_records: self.unflushed_records_up_to(encoded_page_id, as_of_sequence),
            base_image,
            base_sequence,
        })
    }

    /// The newest image layer that is at or below `as_of_sequence`, if there is one.
    fn newest_image_at_or_below(&self, as_of_sequence: SequenceNumber) -> Option<LayerDescriptor> {
        let mut newest_image = None;
        for layer in &self.layers {
            if layer.kind == LayerKind::Image && layer.last_sequence <= as_of_sequence {
                newest_image = Some(layer.clone());
            }
        }
        newest_image
    }

    /// The delta layers that can hold a record after `base_sequence` and at or below `as_of_sequence`, oldest first.
    fn delta_layers_in_reach(
        &self,
        base_sequence: SequenceNumber,
        as_of_sequence: SequenceNumber,
    ) -> Vec<LayerDescriptor> {
        let mut delta_layers = Vec::new();
        for layer in &self.layers {
            let holds_records_in_reach = layer.kind == LayerKind::Delta
                && layer.last_sequence > base_sequence
                && layer.first_sequence <= as_of_sequence;
            if holds_records_in_reach {
                delta_layers.push(layer.clone());
            }
        }
        delta_layers
    }

    /// The records of one page that are still in memory and at or below `as_of_sequence`, in order.
    fn unflushed_records_up_to(&self, encoded_page_id: &Bytes, as_of_sequence: SequenceNumber) -> Vec<LogRecord> {
        let mut unflushed_records = Vec::new();
        let Some(chain) = self.unflushed_by_page.get(encoded_page_id) else {
            return unflushed_records;
        };
        for record in chain {
            if record.sequence <= as_of_sequence {
                unflushed_records.push(record.clone());
            }
        }
        unflushed_records
    }

    /// Builds the delta layer that holds every record applied since the last flush.
    ///
    /// # Returns
    ///
    /// The layer, which covers from the oldest record in memory to the last record applied, or `None` if no record
    /// is in memory. Nothing changes in the state: the caller drops the records once the layer is stored.
    pub(super) fn delta_layer_of_unflushed_records(&self) -> Option<NewLayer> {
        if self.unflushed_by_page.is_empty() {
            return None;
        }
        let mut records_by_page = BTreeMap::new();
        let mut first_sequence = self.applied_sequence;
        for (encoded_page_id, chain) in &self.unflushed_by_page {
            if let Some(first_record) = chain.first() {
                first_sequence = first_sequence.min(first_record.sequence);
            }
            records_by_page.insert(encoded_page_id.clone(), encode_records(chain));
        }
        Some(NewLayer {
            kind: LayerKind::Delta,
            first_sequence,
            last_sequence: self.applied_sequence,
            contents: encode_layer(
                LayerKind::Delta,
                first_sequence,
                self.applied_sequence,
                &records_by_page,
            ),
        })
    }
}

/// Builds the image layer that holds every page in full as of one sequence.
///
/// # Arguments
///
/// * `pages_by_id` - Every page, keyed by its encoded identifier.
/// * `as_of_sequence` - The sequence the pages are at.
///
/// # Returns
///
/// The layer, not stored yet. An image covers one sequence: its first and last sequence are both `as_of_sequence`.
pub(super) fn image_layer_of(pages_by_id: &BTreeMap<Bytes, Bytes>, as_of_sequence: SequenceNumber) -> NewLayer {
    NewLayer {
        kind: LayerKind::Image,
        first_sequence: as_of_sequence,
        last_sequence: as_of_sequence,
        contents: encode_layer(LayerKind::Image, as_of_sequence, as_of_sequence, pages_by_id),
    }
}

/// Whether enough delta layers have piled up since the newest image for the policy to ask for a new one.
///
/// # Arguments
///
/// * `layers` - The layers in use, oldest first, with the delta layer of this checkpoint already in.
/// * `policy` - The engine's layering policy. A threshold of zero counts as one.
///
/// # Returns
///
/// `true` if the checkpoint being written must also write an image layer.
pub(super) fn is_image_due(layers: &[LayerDescriptor], policy: LayeringPolicy) -> bool {
    delta_layers_since_the_last_image(layers) >= policy.image_after_delta_layers.max(1)
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

/// Where the newest image layer is in a list of layers.
///
/// # Arguments
///
/// * `layers` - Layers, oldest first.
///
/// # Returns
///
/// The position of the last image layer in `layers`, or `None` if there is no image.
pub(super) fn newest_image_position(layers: &[LayerDescriptor]) -> Option<usize> {
    let mut newest_image_position = None;
    for (position, layer) in layers.iter().enumerate() {
        if layer.kind == LayerKind::Image {
            newest_image_position = Some(position);
        }
    }
    newest_image_position
}

/// Drops the layers that only reads below the oldest of the `images_kept` newest images would need.
///
/// # Arguments
///
/// * `layers` - The layers in use, oldest first. The dropped ones are removed from the front.
/// * `images_kept` - How many image layers stay. Zero counts as one.
///
/// # Returns
///
/// The new horizon if anything was dropped: the sequence of the oldest image kept. `None` if every layer stays.
pub(super) fn drop_layers_no_read_needs(
    layers: &mut Vec<LayerDescriptor>,
    images_kept: usize,
) -> Option<SequenceNumber> {
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
///
/// # Arguments
///
/// * `state` - The state right after a checkpoint. Its records in memory are not stored: a checkpoint flushes them.
///
/// # Returns
///
/// The details to put in the checkpoint manifest. [`decode_details`] turns them back.
///
/// # Panics
///
/// Panics if the store has 2^32 layers or more, or an artifact key of 4 GiB or more.
pub(super) fn encode_details(state: &StoreState) -> Bytes {
    let mut details = BytesMut::new();
    details.put_u32_le(DETAILS_FORMAT_VERSION);
    details.put_u64_le(state.applied_sequence.get());
    details.put_u64_le(state.horizon.get());
    details.put_u32_le(u32::try_from(state.layers.len()).expect("far fewer than 2^32 layers"));
    for layer in &state.layers {
        details.put_u8(layer.kind.code());
        details.put_u64_le(layer.first_sequence.get());
        details.put_u64_le(layer.last_sequence.get());
        details.put_u32_le(u32::try_from(layer.artifact_key.len()).expect("an artifact key is short"));
        details.put_slice(layer.artifact_key.as_bytes());
    }
    details.freeze()
}

/// The reverse of [`encode_details`].
///
/// # Arguments
///
/// * `details` - The details of a manifest a page store wrote.
///
/// # Returns
///
/// The state the store had right after that checkpoint, with no record in memory.
///
/// # Errors
///
/// Returns [`MaterializerError::InvalidManifest`] if the details come from another format, end early, name a layer of
/// an unknown kind or a key that is not text, or hold more than their lengths say.
pub(super) fn decode_details(details: &Bytes) -> Result<StoreState, MaterializerError> {
    match read_details(details.clone()) {
        Ok(state) => Ok(state),
        Err(reason) => Err(MaterializerError::InvalidManifest {
            reason: format!("the page store details are not valid: {reason}"),
        }),
    }
}

/// Reads a whole state out of `unread`, or says what is wrong with the bytes.
fn read_details(mut unread: Bytes) -> Result<StoreState, &'static str> {
    if take_u32(&mut unread)? != DETAILS_FORMAT_VERSION {
        return Err("they were written by another format");
    }
    let applied_sequence = SequenceNumber::new(take_u64(&mut unread)?);
    let horizon = SequenceNumber::new(take_u64(&mut unread)?);
    let layer_count = take_u32(&mut unread)?;
    let mut layers = Vec::new();
    for _layer in 0..layer_count {
        layers.push(take_layer(&mut unread)?);
    }
    if unread.has_remaining() {
        return Err("they hold more than their lengths say");
    }
    Ok(StoreState {
        layers,
        unflushed_by_page: BTreeMap::new(),
        applied_sequence,
        horizon,
    })
}

/// Reads one layer of stored details and moves `unread` past it.
fn take_layer(unread: &mut Bytes) -> Result<LayerDescriptor, &'static str> {
    let Ok(kind_code) = unread.try_get_u8() else {
        return Err(CUT_SHORT);
    };
    let Some(kind) = LayerKind::from_code(kind_code) else {
        return Err("a layer is of an unknown kind");
    };
    let first_sequence = SequenceNumber::new(take_u64(unread)?);
    let last_sequence = SequenceNumber::new(take_u64(unread)?);
    let key_bytes = take_u32(unread)? as usize;
    if unread.remaining() < key_bytes {
        return Err(CUT_SHORT);
    }
    let Ok(artifact_key) = String::from_utf8(unread.split_to(key_bytes).to_vec()) else {
        return Err("an artifact key is not text");
    };
    Ok(LayerDescriptor {
        kind,
        first_sequence,
        last_sequence,
        artifact_key,
    })
}

/// Reads the next four bytes as an integer, or says that the details end first.
fn take_u32(unread: &mut Bytes) -> Result<u32, &'static str> {
    match unread.try_get_u32_le() {
        Ok(value) => Ok(value),
        Err(_cut_short) => Err(CUT_SHORT),
    }
}

/// Reads the next eight bytes as an integer, or says that the details end first.
fn take_u64(unread: &mut Bytes) -> Result<u64, &'static str> {
    match unread.try_get_u64_le() {
        Ok(value) => Ok(value),
        Err(_cut_short) => Err(CUT_SHORT),
    }
}

#[cfg(test)]
#[path = "state_test.rs"]
mod tests;
