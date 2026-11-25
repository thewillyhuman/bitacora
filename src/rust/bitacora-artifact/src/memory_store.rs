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
    is_unavailable: bool,
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

    fn lock(&self) -> MutexGuard<'_, MemoryArtifacts> {
        // Every critical section is a single map operation.
        self.artifacts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn available(&self) -> Result<MutexGuard<'_, MemoryArtifacts>, ArtifactError> {
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
        self.available()?
            .objects_by_key
            .insert(object_key.to_owned(), contents);
        Ok(())
    }

    async fn get_object(&self, object_key: &str) -> Result<Bytes, ArtifactError> {
        validate_key(object_key)?;
        self.available()?
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
        self.available()?.objects_by_key.remove(object_key);
        Ok(())
    }

    async fn list_prefix(&self, key_prefix: &str) -> Result<Vec<ObjectInfo>, ArtifactError> {
        Ok(self
            .available()?
            .objects_by_key
            .range(key_prefix.to_owned()..)
            .take_while(|(object_key, _contents)| object_key.starts_with(key_prefix))
            .map(|(object_key, contents)| ObjectInfo {
                object_key: object_key.clone(),
                size_bytes: u64::try_from(contents.len()).expect("lengths fit in 64 bits"),
            })
            .collect())
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
