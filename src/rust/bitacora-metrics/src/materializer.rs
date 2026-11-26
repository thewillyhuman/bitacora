//! The materializer of the metrics store, as the framework's runner runs it.
//!
//! # Why this file exists
//!
//! The framework's `ArtifactCatalog` turns a builder into a full materializer: it keeps the catalog of
//! blocks, seals at every checkpoint and merges small blocks. It owns the builder and shows nothing of it.
//! That is enough to produce blocks, and not enough to answer a query, because the newest samples are not
//! in any block yet: they are inside the builder, which nobody can reach.
//!
//! This file wraps the catalog in a materializer of its own that keeps a second copy of the unsealed
//! samples where queries can read them. The copy costs memory and a second decoding of every record. It
//! is here because the framework's public interface left no other way; see finding 1 in
//! `docs/decisions/0015-metrics-materializer.md`.
//!
//! # What it contains
//!
//! - [`MetricsMaterializer`]: the catalog of blocks plus a head that queries can read.
//! - [`METRICS_MATERIALIZER`]: the name the materializer is registered under on a stream.
//!
//! # How it works
//!
//! - **Applying records.** Each record is decoded and added to the shared head, and handed to the catalog,
//!   whose builder decodes it again and adds it to its own samples.
//! - **Checkpointing.** The catalog seals what its builder holds into a block and publishes it. Only then
//!   does this file drop the same records from the shared head, so there is no moment at which a sample
//!   is neither in the head nor in a published block.
//! - **Restoring.** The catalog is restored from the manifest. The head starts empty: the runner applies
//!   the records above the checkpoint again, and they fill it.
//!
//! # Invariants
//!
//! - The shared head holds exactly the records applied since the last checkpoint this instance wrote.
//! - A record the catalog refuses is not added to the shared head.
//!
//! # See also
//!
//! - `builder.rs`: the builder inside the catalog.
//! - `head.rs`: the shared head.
//! - `store.rs`: where the materializer is started and its head and catalog are handed to queries.

use std::sync::Arc;

use async_trait::async_trait;
use bitacora_materializer::{
    ArtifactCatalog, ArtifactNamespace, CatalogView, CheckpointManifest, Materializer, MaterializerError,
};
use bitacora_types::LogRecord;

use crate::builder::{BlockBuilder, BlockPolicy, StandardBlocks};
use crate::head::SharedHead;
use crate::record::decode_record;

/// The name the materializer of a metrics store is registered under on its stream.
pub const METRICS_MATERIALIZER: &str = "bitacora-metrics";

/// The materializer of a metrics store: a catalog of blocks, and the unsealed samples where queries can
/// read them.
///
/// The framework's runner owns it. It is not used directly; see [`MetricsStore`](crate::MetricsStore).
pub struct MetricsMaterializer<P: BlockPolicy = StandardBlocks> {
    /// The framework's adapter: the builder and the catalog of blocks.
    catalog: ArtifactCatalog<BlockBuilder<P>>,
    /// The samples applied since the last checkpoint, readable by queries.
    head: Arc<SharedHead>,
}

impl<P: BlockPolicy> MetricsMaterializer<P> {
    /// The head, for queries.
    pub(crate) fn head(&self) -> Arc<SharedHead> {
        Arc::clone(&self.head)
    }

    /// A view of the catalog of blocks, for queries.
    pub(crate) fn catalog(&self) -> CatalogView {
        self.catalog.view()
    }
}

#[async_trait]
impl<P: BlockPolicy> Materializer for MetricsMaterializer<P> {
    /// Hands the records to the catalog and adds their samples to the shared head.
    fn apply_records(&mut self, committed_records: &[LogRecord]) -> Result<(), MaterializerError> {
        let mut decoded_records = Vec::with_capacity(committed_records.len());
        for record in committed_records {
            match decode_record(&record.payload) {
                Ok(batch) => decoded_records.push((record.sequence, batch)),
                Err(record_error) => {
                    return Err(MaterializerError::InvalidRecord {
                        sequence: record.sequence,
                        reason: record_error.to_string(),
                    });
                }
            }
        }
        self.catalog.apply_records(committed_records)?;
        self.head.add_records(&decoded_records);
        Ok(())
    }

    /// Whatever the catalog wants: its builder asks once a block is full.
    fn wants_checkpoint(&self) -> bool {
        self.catalog.wants_checkpoint()
    }

    /// Has the catalog seal and publish a block, then drops the sealed records from the shared head.
    ///
    /// If the catalog fails, the head keeps everything, including what a block may already hold. A query
    /// then finds those samples in both places, and they are the same samples.
    async fn write_checkpoint(
        &mut self,
        artifacts: &ArtifactNamespace,
    ) -> Result<CheckpointManifest, MaterializerError> {
        let manifest = self.catalog.write_checkpoint(artifacts).await?;
        // The catalog has sealed everything applied so far and shows the block: the head's copy can go.
        self.head.clear();
        Ok(manifest)
    }

    /// Restores the catalog and starts with an empty head.
    async fn restore_from_checkpoint(
        last_checkpoint: Option<CheckpointManifest>,
        artifacts: &ArtifactNamespace,
    ) -> Result<Self, MaterializerError> {
        let catalog = ArtifactCatalog::restore_from_checkpoint(last_checkpoint, artifacts).await?;
        Ok(Self {
            catalog,
            head: Arc::new(SharedHead::default()),
        })
    }
}
