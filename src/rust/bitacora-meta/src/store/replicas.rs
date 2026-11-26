//! The replica registry.
//!
//! # Why this file exists
//!
//! Writers, readers and the controller find replicas through the metadata store: a segment's record names replica
//! identities, and the registry says where each one is reached, which failure domains it is in, how full it is and when
//! it was last heard from (design, section 7, "Capacity and admission"). Placement and failure detection are computed
//! from nothing else. This file holds the three operations on that registry.
//!
//! # What it contains
//!
//! - `MetadataStore::put_replica`: a replica registering itself, or renewing its registration.
//! - `MetadataStore::remove_replica`: an operator taking a replica out of the cluster.
//! - `MetadataStore::list_replicas`: the whole registry.
//!
//! # How it works
//!
//! A replica writes its own record when it starts and again on a timer. The write is unconditional: the record has one
//! author, the replica itself, and a renewal carries everything the record holds, so there is nothing a concurrent
//! writer could lose. This is the one place in the crate where a write does not depend on a read.
//!
//! Nothing is deleted when a replica stops. Its record stays, with a heartbeat that grows old, and it is the readers of
//! the registry who decide what an old heartbeat means: placement stops choosing the replica after a short window,
//! repair gives its copies up after a much longer one. Only an operator removes a record.
//!
//! # Invariants
//!
//! - There is one record per replica identity, written only by that replica.
//! - A replica's record is never removed by the cluster itself.
//!
//! # See also
//!
//! - `../records.rs`: [`ReplicaRecord`] and what "alive" means.
//! - `../placement.rs`: how the registry becomes a choice of replicas.
//! - `bita-replica`: the daemon that registers itself.

use bitacora_types::ReplicaId;

use super::{MetadataStore, delete_key, put_record};
use crate::error::MetaError;
use crate::keys::{REPLICA_KEY_PREFIX, replica_key};
use crate::records::ReplicaRecord;

impl MetadataStore {
    /// Registers a replica, or renews its registration.
    ///
    /// The record replaces whatever was registered under the same identity. A replica calls this when it starts and
    /// then on a timer, each time with its current free space and a fresh heartbeat.
    ///
    /// # Arguments
    ///
    /// * `replica` - Everything the replica says about itself.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Unavailable`] if the store cannot be reached. The registration may or may not have been
    /// written; the next renewal settles it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// use std::sync::Arc;
    ///
    /// use bitacora_meta::{MemoryKeyValueStore, MetadataStore, RECORD_SCHEMA_VERSION, ReplicaRecord};
    /// use bitacora_types::{ReplicaId, Timestamp};
    ///
    /// let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let registration = ReplicaRecord {
    ///     schema_version: RECORD_SCHEMA_VERSION,
    ///     replica_id: ReplicaId::new("replica-a").expect("a valid identifier"),
    ///     address: "10.0.0.11:7000".to_owned(),
    ///     failure_domains: vec!["zone-1".to_owned(), "host-11".to_owned()],
    ///     capacity_bytes: 960_000_000_000,
    ///     free_bytes: 412_000_000_000,
    ///     is_draining: false,
    ///     heartbeat_at: Timestamp::from_unix_millis(1_000),
    /// };
    ///
    /// metadata.put_replica(&registration).await?;
    ///
    /// assert_eq!(metadata.list_replicas().await?, [registration]);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put_replica(&self, replica: &ReplicaRecord) -> Result<(), MetaError> {
        let registration = put_record(&replica_key(&replica.replica_id), replica);
        self.key_value_store.transact(Vec::new(), vec![registration]).await?;
        Ok(())
    }

    /// Removes a replica from the registry.
    ///
    /// After that, the controller treats the replica's copies of sealed segments as lost and has them rebuilt
    /// elsewhere. Removing a replica that is not registered succeeds.
    ///
    /// # Arguments
    ///
    /// * `replica_id` - The identity of the replica to remove.
    ///
    /// # Errors
    ///
    /// Returns [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    /// # use bitacora_types::ReplicaId;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let never_registered = ReplicaId::new("replica-z").expect("a valid identifier");
    ///
    /// metadata.remove_replica(&never_registered).await?;
    ///
    /// assert!(metadata.list_replicas().await?.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_replica(&self, replica_id: &ReplicaId) -> Result<(), MetaError> {
        let removal = delete_key(&replica_key(replica_id));
        self.key_value_store.transact(Vec::new(), vec![removal]).await?;
        Ok(())
    }

    /// Lists every registered replica.
    ///
    /// The listing is one consistent view of the store. It includes replicas that have not renewed their registration
    /// for a long time; whether one is alive is for the caller to judge, with [`ReplicaRecord::is_alive`].
    ///
    /// # Returns
    ///
    /// The registrations, sorted by replica identifier. Empty if no replica ever registered.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if a stored registration cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let registry = metadata.list_replicas().await?;
    ///
    /// assert!(registry.is_empty());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_replicas(&self) -> Result<Vec<ReplicaRecord>, MetaError> {
        self.list_records(REPLICA_KEY_PREFIX).await
    }
}

#[cfg(test)]
#[path = "replicas_test.rs"]
mod tests;
