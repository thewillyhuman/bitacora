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
//! The parts nobody outside sees are in two files of their own: `page_store/state.rs` holds what the store knows
//! and the rules decided from that alone, `page_store/shared.rs` what the store and its readers share and every read
//! of a layer.
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
//! - `page_store/state.rs`: the layer list, the read plan, the layering rules and the stored details.
//! - `page_store/shared.rs`: the lock, the layer fetches and the building of images.
//! - `layer.rs`: the layer file format.
//! - `contract.rs`: the contract this adapter implements.

mod shared;
mod state;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitacora_artifact::ArtifactError;
use bitacora_types::{LogRecord, SequenceNumber};
use bytes::Bytes;
use thiserror::Error;
use tokio::time::timeout;

use self::shared::Shared;
use self::state::{
    LayerDescriptor, NewLayer, StoreState, decode_details, drop_layers_no_read_needs, encode_details, image_layer_of,
    is_image_due,
};
use crate::contract::{CheckpointManifest, Materializer, MaterializerError};
use crate::layer::LayerKind;
use crate::namespace::ArtifactNamespace;

/// An image layer is written after this many delta layers, unless the engine says otherwise.
const DEFAULT_IMAGE_AFTER_DELTA_LAYERS: usize = 4;

/// This many image layers are kept, unless the engine says otherwise. Pages stay readable from the oldest image
/// kept on, so with two a read can still go back to the image before the newest one.
const DEFAULT_IMAGES_KEPT: usize = 2;

/// The label of the artifact a delta layer is stored as.
const DELTA_LAYER_LABEL: &str = "delta";

/// The label of the artifact an image layer is stored as.
const IMAGE_LAYER_LABEL: &str = "image";

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
            image_after_delta_layers: DEFAULT_IMAGE_AFTER_DELTA_LAYERS,
            images_kept: DEFAULT_IMAGES_KEPT,
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

/// The adapter that turns a [`PageMaterializer`] into a [`Materializer`] with versioned pages.
///
/// The runner owns the store. Take a [`PageReader`] from it with [`PageStore::reader`] before handing it
/// to the runner, and read pages through that. See [`PageMaterializer`] for an example.
pub struct PageStore<Engine: PageMaterializer> {
    /// Shared with the readers.
    shared: Arc<Shared<Engine>>,
}

impl<Engine: PageMaterializer> PageStore<Engine> {
    /// Gives a handle that reads pages of this store, from any task, while records are applied.
    ///
    /// # Returns
    ///
    /// A reader of this store. It can be cloned, and it stays usable after the runner that owns the store has
    /// stopped.
    ///
    /// # Examples
    ///
    /// The example of [`PageMaterializer`] runs this against a store; here only the call is shown.
    ///
    /// ```
    /// use bitacora_materializer::{PageMaterializer, PageReader, PageStore};
    ///
    /// fn reader_for_the_query_path<Engine: PageMaterializer>(pages: &PageStore<Engine>) -> PageReader<Engine> {
    ///     pages.reader()
    /// }
    /// ```
    #[must_use]
    pub fn reader(&self) -> PageReader<Engine> {
        PageReader {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Asks the engine which pages each record touches, as the identifiers are stored in layers.
    ///
    /// Every record is asked about before any is taken in, so a record that is refused leaves nothing of its batch
    /// behind.
    fn pages_touched_by_each(
        &self,
        committed_records: &[LogRecord],
    ) -> Result<Vec<BTreeSet<Bytes>>, MaterializerError> {
        let mut pages_by_record = Vec::with_capacity(committed_records.len());
        for record in committed_records {
            let mut pages_of_this_record = BTreeSet::new();
            for page_id in self.shared.engine().pages_touched(record)? {
                pages_of_this_record.insert(Bytes::from(Engine::encode_page_id(&page_id)));
            }
            pages_by_record.push(pages_of_this_record);
        }
        Ok(pages_by_record)
    }
}

/// Stores a layer that was just built, and describes where it now is.
async fn store_layer(artifacts: &ArtifactNamespace, new_layer: NewLayer) -> Result<LayerDescriptor, MaterializerError> {
    let label = match new_layer.kind {
        LayerKind::Delta => DELTA_LAYER_LABEL,
        LayerKind::Image => IMAGE_LAYER_LABEL,
    };
    let artifact_key = artifacts.put_artifact(label, new_layer.contents.clone()).await?;
    Ok(new_layer.stored_as(artifact_key))
}

/// The manifest of a checkpoint that left the store in `state_after`: every layer in use, and the details to
/// restore from.
fn manifest_of(state_after: &StoreState) -> CheckpointManifest {
    let mut artifact_keys = Vec::with_capacity(state_after.layers.len());
    for layer in &state_after.layers {
        artifact_keys.push(layer.artifact_key.clone());
    }
    CheckpointManifest {
        artifact_keys,
        details: encode_details(state_after),
    }
}

#[async_trait]
impl<Engine: PageMaterializer> Materializer for PageStore<Engine> {
    /// Appends each record to the in-memory chain of every page it touches. No page is computed here.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let pages_by_record = self.pages_touched_by_each(committed_records)?;
        self.shared.take_in(committed_records, pages_by_record);
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
        let state_before = self.shared.state_snapshot();
        let policy = self.shared.engine().layering_policy();
        let mut layers = state_before.layers.clone();

        if let Some(delta_layer) = state_before.delta_layer_of_unflushed_records() {
            layers.push(store_layer(artifacts, delta_layer).await?);
        }
        if is_image_due(&layers, policy) {
            let pages_by_id = self.shared.materialize_every_page(&layers).await?;
            let image_layer = image_layer_of(&pages_by_id, state_before.applied_sequence);
            layers.push(store_layer(artifacts, image_layer).await?);
        }
        let horizon = drop_layers_no_read_needs(&mut layers, policy.images_kept).unwrap_or(state_before.horizon);

        let state_after = StoreState {
            layers,
            unflushed_by_page: BTreeMap::new(),
            applied_sequence: state_before.applied_sequence,
            horizon,
        };
        let manifest = manifest_of(&state_after);
        // Records cannot have been applied meanwhile: applying needs the store exclusively, as this does.
        self.shared.replace_state(state_after, &manifest.artifact_keys);
        Ok(manifest)
    }

    /// Rebuilds the layer list from the manifest. No layer is read until a page is.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let state = match last_checkpoint {
            Some(manifest) => decode_details(&manifest.details)?,
            None => StoreState::empty(),
        };
        Ok(Self {
            shared: Arc::new(Shared::new(artifacts.clone(), state)),
        })
    }
}

/// Reads the pages of a [`PageStore`], as of any sequence it still keeps.
///
/// Clones read the same store. A reader stays usable after the runner that owned the store has stopped;
/// the store then simply no longer advances. See [`PageMaterializer`] for an example.
pub struct PageReader<Engine: PageMaterializer> {
    /// Shared with the store.
    shared: Arc<Shared<Engine>>,
}

impl<Engine: PageMaterializer> Clone for PageReader<Engine> {
    /// Gives another handle on the same store.
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<Engine: PageMaterializer> PageReader<Engine> {
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
    ///
    /// # Examples
    ///
    /// The example of [`PageMaterializer`] runs reads against a store; here only the call is shown.
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use bitacora_materializer::{PageMaterializer, PageReadError, PageReader};
    /// use bitacora_types::SequenceNumber;
    /// use bytes::Bytes;
    ///
    /// /// The page as of record 42, waiting up to a second for the store to get there.
    /// async fn page_at_record_42<Engine: PageMaterializer>(
    ///     reader: &PageReader<Engine>,
    ///     page_id: &Engine::PageId,
    /// ) -> Result<Option<Bytes>, PageReadError> {
    ///     reader.read_page(page_id, SequenceNumber::new(42), Duration::from_secs(1)).await
    /// }
    /// ```
    pub async fn read_page(
        &self,
        page_id: &Engine::PageId,
        as_of_sequence: SequenceNumber,
        wait: Duration,
    ) -> Result<Option<Bytes>, PageReadError> {
        self.wait_until_applied(as_of_sequence, wait).await?;
        let encoded_page_id = Bytes::from(Engine::encode_page_id(page_id));
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

    /// The last record the store has applied.
    ///
    /// # Returns
    ///
    /// The sequence of that record, or `ZERO` before the first one. A read at or below it does not wait.
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{PageMaterializer, PageReader};
    ///
    /// fn records_applied<Engine: PageMaterializer>(reader: &PageReader<Engine>) -> u64 {
    ///     reader.applied_sequence().get()
    /// }
    /// ```
    #[must_use]
    pub fn applied_sequence(&self) -> SequenceNumber {
        self.shared.applied_sequence()
    }

    /// The lowest sequence pages can still be read at.
    ///
    /// # Returns
    ///
    /// `ZERO` until a checkpoint drops layers; from then on the sequence of the oldest image layer kept. A read
    /// below it fails with [`PageReadError::BelowHorizon`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bitacora_materializer::{PageMaterializer, PageReader};
    /// use bitacora_types::SequenceNumber;
    ///
    /// fn is_still_readable<Engine: PageMaterializer>(reader: &PageReader<Engine>, sequence: SequenceNumber) -> bool {
    ///     sequence >= reader.horizon()
    /// }
    /// ```
    #[must_use]
    pub fn horizon(&self) -> SequenceNumber {
        self.shared.horizon()
    }

    /// Waits until the store has applied `as_of_sequence`, for at most `wait`.
    async fn wait_until_applied(&self, as_of_sequence: SequenceNumber, wait: Duration) -> Result<(), PageReadError> {
        let mut applied = self.shared.watch_applied_sequence();
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
