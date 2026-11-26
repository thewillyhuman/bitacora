//! Leadership and the loop around the pass.
//!
//! # Why this file exists
//!
//! A cluster runs two or three controllers so that one is always there, and only one of them should work at a time:
//! two that ask the same replica to repair the same copy, or to upload the same segment, do the work twice. This
//! file is how a controller finds out whether it is the one, and how it keeps going: a round takes or renews the
//! leadership and runs a pass if it has it, and the loop runs rounds forever.
//!
//! # What it contains
//!
//! - [`Controller::run_round`]: one round. Returns whether this instance led and what its pass did.
//! - [`Controller::run`]: rounds forever, for a process that does not need to look at them.
//!
//! # How it works
//!
//! Leadership is a record in the metadata store with a holder and an expiry
//! (`MetadataStore::try_lead`). An instance that finds the record absent, expired or its own writes itself in with a
//! new expiry, conditionally on what it read; one that finds another holder's unexpired record stands by. A leader
//! that stops renewing, because it crashed or cannot reach the metadata store, is replaced once its record lapses.
//!
//! # Invariants
//!
//! - Leadership only avoids duplicate work. It is not a lock: a leader that is paused keeps believing it leads after
//!   its record lapsed, and for a moment two instances work. That is harmless, because every write of a pass is
//!   conditional on what the pass saw.
//! - The loop never ends on a failure. A round that fails is logged, and the next one starts after the pass
//!   interval like any other.
//!
//! # See also
//!
//! - `../controller.rs`: the pass a round runs.
//! - `bitacora-meta/src/store/leadership.rs`: the leadership record.
//! - `bita-controller`: a process that calls `run_round` in a loop of its own, to export what each round did.

use bitacora_meta::MetaError;
use tokio::time::sleep;

use super::{Controller, Round};

impl Controller {
    /// Runs rounds forever, resting for the pass interval between two.
    ///
    /// Stop it by dropping the future. A round that fails is logged and the next one tries again. A process that
    /// wants to see what each round did, for example to export metrics, calls [`Controller::run_round`] in a loop of
    /// its own.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// use std::time::Duration;
    ///
    /// use bitacora_controller::{Controller, ControllerSettings};
    /// use bitacora_meta::RetentionPolicy;
    /// use bitacora_testkit::Cluster;
    /// use bitacora_types::ReplicationSettings;
    ///
    /// let cluster = Cluster::start(3).await;
    /// let controller = Controller::new(
    ///     cluster.metadata().clone(),
    ///     cluster.connector(),
    ///     cluster.clock(),
    ///     ControllerSettings::default(),
    /// );
    /// let running = tokio::spawn(async move { controller.run().await });
    ///
    /// // A stream created while the controller runs gets its standby within a pass or two.
    /// cluster
    ///     .metadata()
    ///     .create_stream("orders", ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
    ///     .await?;
    /// tokio::time::sleep(Duration::from_secs(3)).await;
    /// running.abort();
    ///
    /// let stream = cluster.metadata().get_stream("orders").await?;
    /// assert!(stream.standby_replicas.is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(&self) {
        loop {
            if let Err(meta_error) = self.run_round().await {
                tracing::warn!(%meta_error, "controller round failed");
            }
            sleep(self.settings.pass_interval).await;
        }
    }

    /// Runs one round: takes or renews the leadership and, if this instance leads, runs one pass.
    ///
    /// # Returns
    ///
    /// [`Round::Led`] with the report of the pass if this instance holds the leadership, [`Round::StoodBy`] if
    /// another instance holds it. An instance that stood by read the leadership record and did nothing else.
    ///
    /// # Errors
    ///
    /// Fails if the leadership cannot be read or written, or if the pass fails; see [`Controller::run_pass`].
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), bitacora_meta::MetaError> {
    /// use bitacora_controller::{Controller, ControllerSettings, Round};
    /// use bitacora_testkit::Cluster;
    ///
    /// let cluster = Cluster::start(3).await;
    /// let controller_named = |identity: &str| {
    ///     let settings = ControllerSettings {
    ///         identity: identity.to_owned(),
    ///         ..ControllerSettings::default()
    ///     };
    ///     Controller::new(cluster.metadata().clone(), cluster.connector(), cluster.clock(), settings)
    /// };
    ///
    /// let first_round = controller_named("controller-a").run_round().await?;
    /// let round_of_the_other = controller_named("controller-b").run_round().await?;
    ///
    /// assert!(matches!(first_round, Round::Led(_)));
    /// assert_eq!(round_of_the_other, Round::StoodBy);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_round(&self) -> Result<Round, MetaError> {
        let now = self.clock.now();
        let leadership_expires_at = now.plus(self.settings.leadership_duration);
        let leads = self
            .metadata
            .try_lead(&self.settings.identity, now, leadership_expires_at)
            .await?;
        if !leads {
            return Ok(Round::StoodBy);
        }
        let report = self.run_pass().await?;
        Ok(Round::Led(report))
    }
}

#[cfg(test)]
#[path = "round_test.rs"]
mod tests;
