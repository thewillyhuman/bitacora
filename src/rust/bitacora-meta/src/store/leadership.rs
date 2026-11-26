//! Which controller instance is active.
//!
//! # Why this file exists
//!
//! A deployment runs two or three controllers so that one is always there, but their passes over the cluster are work,
//! and doing every pass two or three times would waste it and fill the logs with conflicts. So one instance at a time
//! does the passes and the others stand by (design, section 8). The metadata store is where they agree on which one;
//! this file holds that agreement.
//!
//! # What it contains
//!
//! - `MetadataStore::try_lead`: becoming, or staying, the active controller.
//! - `MetadataStore::get_leader`: who it is, for operators and tools.
//!
//! # How it works
//!
//! One record, `controller/leader`, names the active instance and the moment its leadership lapses unless renewed. An
//! instance calls `try_lead` before every pass. The call succeeds if nobody leads, if the previous leadership has
//! lapsed, or if the caller is the leader already, in which case it renews. The write is conditional on the record
//! being as it was read, so of two instances that both see a lapsed leadership only one takes it.
//!
//! The record also says since when the holder has led without a lapse. A renewal keeps that moment; any other
//! successful call starts a new leadership, also for an instance that let its own lapse.
//!
//! Leadership is a liveness optimization and nothing more. Time is the caller's clock, clocks differ, and a paused
//! leader does not know it was replaced, so two instances can believe they lead at once. That is harmless: every write
//! a controller makes during a pass is itself conditional on what the pass read.
//!
//! # Invariants
//!
//! - A live leadership is never taken from its holder by another instance.
//! - The safety of the cluster does not depend on there being only one leader.
//!
//! # See also
//!
//! - `../records.rs`: [`LeaderRecord`].
//! - `bitacora-controller/src/controller.rs`: `Controller::run_round`, the caller.

use bitacora_types::Timestamp;

use super::{MetadataStore, decode_record, put_record, unchanged_since_read};
use crate::error::MetaError;
use crate::key_value::TransactionOutcome;
use crate::keys::CONTROLLER_LEADER_KEY;
use crate::records::LeaderRecord;

impl MetadataStore {
    /// Tries to become, or stay, the active controller until `expires_at`.
    ///
    /// Succeeds if nobody leads, if the previous leadership lapsed before `now`, or if `candidate` leads already. A
    /// renewal keeps the moment the leadership began; anything else starts a new one.
    ///
    /// Leadership is a liveness optimization: every controller write is itself conditional, so two controllers that
    /// both believe they lead cannot corrupt anything.
    ///
    /// # Arguments
    ///
    /// * `candidate` - The name of the calling controller instance.
    /// * `now` - The current time, from the caller's clock.
    /// * `expires_at` - When the leadership lapses unless it is renewed by another call.
    ///
    /// # Returns
    ///
    /// `true` if the caller leads until `expires_at`. `false` if another instance holds a live leadership, or took the
    /// lapsed one a moment before the caller could.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if the stored leader record cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    /// # use bitacora_types::Timestamp;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let now = Timestamp::from_unix_millis(1_000);
    /// let in_five_seconds = Timestamp::from_unix_millis(6_000);
    ///
    /// let first_leads = metadata.try_lead("controller-1", now, in_five_seconds).await?;
    /// let second_leads = metadata.try_lead("controller-2", now, in_five_seconds).await?;
    ///
    /// assert!(first_leads);
    /// assert!(!second_leads);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_lead(&self, candidate: &str, now: Timestamp, expires_at: Timestamp) -> Result<bool, MetaError> {
        let stored_leader = self.key_value_store.get(CONTROLLER_LEADER_KEY).await?;
        let mut leading_since = now;
        if let Some(stored) = &stored_leader {
            let leader: LeaderRecord = decode_record(CONTROLLER_LEADER_KEY, &stored.value)?;
            let is_live = leader.expires_at > now;
            let is_held_by_the_candidate = leader.holder == candidate;
            if is_live && !is_held_by_the_candidate {
                return Ok(false);
            }
            // A renewal continues the leadership; anything else starts a new one, also for an instance that let its own
            // lapse. A record written before the start was recorded has none to keep.
            let is_renewal = is_live && is_held_by_the_candidate;
            if is_renewal {
                leading_since = leader.leading_since.unwrap_or(now);
            }
        }
        let claim = LeaderRecord {
            holder: candidate.to_owned(),
            expires_at,
            leading_since: Some(leading_since),
        };
        let outcome = self
            .key_value_store
            .transact(
                vec![unchanged_since_read(CONTROLLER_LEADER_KEY, stored_leader.as_ref())],
                vec![put_record(CONTROLLER_LEADER_KEY, &claim)],
            )
            .await?;
        Ok(outcome == TransactionOutcome::Applied)
    }

    /// Reads which controller instance leads.
    ///
    /// The record stays after its leadership lapsed. To tell whether somebody leads right now, compare `expires_at`
    /// with the clock.
    ///
    /// # Returns
    ///
    /// The leader record, or `None` if no instance ever led.
    ///
    /// # Errors
    ///
    /// * [`MetaError::CorruptRecord`] if the stored leader record cannot be decoded.
    /// * [`MetaError::Unavailable`] if the store cannot be reached.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// # use std::sync::Arc;
    /// # use bitacora_meta::{MemoryKeyValueStore, MetadataStore};
    /// # use bitacora_types::Timestamp;
    /// # let metadata = MetadataStore::new(Arc::new(MemoryKeyValueStore::new()));
    /// let now = Timestamp::from_unix_millis(1_000);
    /// metadata
    ///     .try_lead("controller-1", now, Timestamp::from_unix_millis(6_000))
    ///     .await?;
    ///
    /// let leader = metadata.get_leader().await?.expect("somebody leads");
    ///
    /// assert_eq!(leader.holder, "controller-1");
    /// assert_eq!(leader.leading_since, Some(now));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_leader(&self) -> Result<Option<LeaderRecord>, MetaError> {
        let Some(stored) = self.key_value_store.get(CONTROLLER_LEADER_KEY).await? else {
            return Ok(None);
        };
        let leader = decode_record(CONTROLLER_LEADER_KEY, &stored.value)?;
        Ok(Some(leader))
    }
}

#[cfg(test)]
#[path = "leadership_test.rs"]
mod tests;
