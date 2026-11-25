//! An in-memory [`ArtifactStore`], for tests and simulation.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;

use crate::store::{ArtifactError, ArtifactStore, ObjectInfo, slice_range, validate_key};

#[derive(Debug, Default)]
struct MemoryArtifacts {
    objects_by_key: BTreeMap<String, Bytes>,
    /// While set, every operation fails.
    is_unavailable: bool,
    /// While set, no operation ever answers.
    is_unresponsive: bool,
}

/// An in-memory artifact store. Clones share the same objects, like two
/// clients of one bucket.
#[derive(Clone, Debug, Default)]
pub struct MemoryArtifactStore {
    artifacts: Arc<Mutex<MemoryArtifacts>>,
}

impl MemoryArtifactStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates an outage: while unavailable, every operation fails and
    /// nothing changes.
    pub fn set_unavailable(&self, is_unavailable: bool) {
        self.lock().is_unavailable = is_unavailable;
    }

    /// Simulates a store that accepts requests and never answers them:
    /// while unresponsive, every operation hangs until its caller gives
    /// up, and nothing changes.
    pub fn set_unresponsive(&self, is_unresponsive: bool) {
        self.lock().is_unresponsive = is_unresponsive;
    }

    /// Simulates silent corruption: flips `flipped_bits` in the byte at
    /// `offset_bytes` of a stored object. Does nothing if there is no such
    /// object or byte.
    pub fn corrupt_byte(&self, object_key: &str, offset_bytes: usize, flipped_bits: u8) {
        let mut artifacts = self.lock();
        let Some(contents) = artifacts.objects_by_key.get_mut(object_key) else {
            return;
        };
        let mut damaged_contents = contents.to_vec();
        if let Some(byte) = damaged_contents.get_mut(offset_bytes) {
            *byte ^= flipped_bits;
        }
        *contents = Bytes::from(damaged_contents);
    }

    fn lock(&self) -> MutexGuard<'_, MemoryArtifacts> {
        // Every critical section is a single map operation.
        self.artifacts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the store for one operation, after applying the faults a test
    /// has switched on.
    async fn available(&self) -> Result<MutexGuard<'_, MemoryArtifacts>, ArtifactError> {
        if self.lock().is_unresponsive {
            std::future::pending::<()>().await;
        }
        let artifacts = self.lock();
        if artifacts.is_unavailable {
            return Err(ArtifactError::Unavailable {
                reason: "injected outage".to_owned(),
            });
        }
        Ok(artifacts)
    }
}

fn not_found(object_key: &str) -> ArtifactError {
    ArtifactError::NotFound {
        object_key: object_key.to_owned(),
    }
}

#[async_trait]
impl ArtifactStore for MemoryArtifactStore {
    async fn put_object(&self, object_key: &str, contents: Bytes) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        self.available()
            .await?
            .objects_by_key
            .insert(object_key.to_owned(), contents);
        Ok(())
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        self.available()
            .await?
            .objects_by_key
            .get(object_key)
            .cloned()
            .ok_or_else(|| not_found(object_key))
    }

    async fn get_range(
        &self,
        object_key: &str,
        byte_range: Range<u64>,
    ) -> Result<Bytes, ArtifactError> {
        let contents = self.get_object(object_key).await?;
        slice_range(object_key, &contents, &byte_range)
    }

    async fn delete_object(&self, object_key: &str) -> Result<(), ArtifactError> {
        validate_key(object_key)?;
        self.available().await?.objects_by_key.remove(object_key);
        Ok(())
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        let artifacts = self.available().await?;
        let mut listed = Vec::new();
        for (object_key, contents) in artifacts.objects_by_key.range(key_prefix.to_owned()..) {
            if !object_key.starts_with(key_prefix) {
                break;
            }
            listed.push(ObjectInfo {
                object_key: object_key.clone(),
                size_bytes: u64::try_from(contents.len()).expect("lengths fit in 64 bits"),
            });
        }
        Ok(listed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract;

    #[tokio::test]
    async fn memory_store_honours_the_artifact_store_contract() {
        contract::check(&MemoryArtifactStore::new()).await;
    }

    #[tokio::test]
    async fn unavailable_store_fails_and_keeps_its_objects() {
        let store = MemoryArtifactStore::new();
        store
            .put_object("a", Bytes::from_static(b"kept"))
            .await
            .expect("put");
        store.set_unavailable(true);
        let during_outage = (
            store.get_object("a").await,
            store.put_object("a", Bytes::from_static(b"lost")).await,
            store.list_prefix("").await,
        );
        store.set_unavailable(false);
        assert!(
            matches!(
                during_outage,
                (
                    Err(ArtifactError::Unavailable { .. }),
                    Err(ArtifactError::Unavailable { .. }),
                    Err(ArtifactError::Unavailable { .. })
                )
            ) && store.get_object("a").await == Ok(Bytes::from_static(b"kept"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unresponsive_store_never_answers() {
        let store = MemoryArtifactStore::new();
        store.set_unresponsive(true);
        let answer =
            tokio::time::timeout(std::time::Duration::from_hours(1), store.get_object("a")).await;
        assert!(answer.is_err(), "{answer:?}");
    }

    #[tokio::test]
    async fn corrupted_byte_reads_back_with_its_bits_flipped() {
        let store = MemoryArtifactStore::new();
        store
            .put_object("a", Bytes::from_static(b"abc"))
            .await
            .expect("put");
        store.corrupt_byte("a", 1, 0x01);
        assert_eq!(store.get_object("a").await, Ok(Bytes::from_static(b"acc")));
    }

    #[tokio::test]
    async fn corrupting_a_missing_object_or_byte_does_nothing() {
        let store = MemoryArtifactStore::new();
        store
            .put_object("a", Bytes::from_static(b"abc"))
            .await
            .expect("put");
        store.corrupt_byte("missing", 0, 0x01);
        store.corrupt_byte("a", 3, 0x01);
        assert_eq!(store.get_object("a").await, Ok(Bytes::from_static(b"abc")));
    }

    #[tokio::test]
    async fn clones_share_the_same_objects() {
        let store = MemoryArtifactStore::new();
        store
            .clone()
            .put_object("a", Bytes::from_static(b"shared"))
            .await
            .expect("put");
        assert_eq!(
            store.get_object("a").await,
            Ok(Bytes::from_static(b"shared"))
        );
    }
}
