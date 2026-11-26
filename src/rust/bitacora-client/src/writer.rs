//! The stream writer: the write path of the log.
//!
//! # Why this file exists
//!
//! A database commits by appending to the log, so an append has to be as cheap as replication allows: one network round
//! trip to the replicas and one disk flush on each, with the metadata store never touched (design, section 5). The
//! writer is therefore a library inside the application, not a service of its own. This file is the handle the
//! application holds: it accepts records, tells the caller when they are committed, and ends the writer cleanly.
//!
//! # What it contains
//!
//! - [`StreamWriter`]: the single writer of a stream, with `append`, `append_batch`, `durable_sequence`, `epoch` and
//!   `close`.
//! - `Shared`: what the writer's tasks share, and the few things they all do with it.
//! - The submodules: `state` (the bookkeeping), `sender` (one task per replica), `driver` (lease and rolls) and
//!   `takeover` (becoming the writer).
//!
//! # How it works
//!
//! Three kinds of task cooperate through one `WriterState` behind a mutex:
//!
//! - the callers of [`StreamWriter::append`], which put a batch into the state and wait for it to commit;
//! - one *sender* per replica of the open segment, which sends whatever that replica is missing and reports what it
//!   acknowledged;
//! - one *driver*, which renews the lease and rolls to a new segment when a replica fails or the segment is full.
//!
//! They wake each other through a watch channel whose value is only a version number: whoever changes the state in a
//! way that someone may be waiting for bumps it. An appender bumps it after enqueueing, which wakes the senders; a
//! sender bumps it after an acknowledgement, which wakes appenders waiting for capacity and the driver.
//!
//! An append resolves through a one-shot channel that the state completes when the batch commits or the writer stops.
//! Once a record has a sequence number it is sent even if its caller stops waiting.
//!
//! # Invariants
//!
//! - The mutex is never held across an `await`. Every critical section is a method of `WriterState` that leaves it
//!   consistent, so a panic in one task does not corrupt the state for the others.
//! - `append` makes no request of its own and touches no channel except its own: one lock to enqueue, one bump of the
//!   version, one wait. This is the hot path of the whole library and is kept that short on purpose.
//! - A writer that learns it lost the stream stops for good: every pending and later append fails with
//!   [`AppendError::Fenced`].
//! - Dropping the writer stops its tasks. Its segment stays open, to be recovered by the next writer.
//!
//! # See also
//!
//! - `writer/state.rs`: the commit rule and everything else that is decided, as pure code.
//! - `writer/sender.rs`, `writer/driver.rs`: the tasks.
//! - `writer/takeover.rs`: the failover protocol that opening a writer runs.
//! - `docs/protocol/replication.md`: the rules a writer follows.

mod driver;
mod sender;
mod state;
pub(crate) mod takeover;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bitacora_meta::MetaError;
use bitacora_types::replica_api::SegmentDescriptor;
use bitacora_types::{Epoch, ReplicaId, SequenceNumber, SequenceRange, StreamId};
use bytes::Bytes;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};

use crate::context::{ClientContext, ConnectedReplica};
use crate::error::{AppendError, OpenError};
use crate::settings::{TakeoverPolicy, WriterSettings};
use driver::run_driver;
use state::{CommitResult, Status, WriterState};
use takeover::{OpenedStream, announce_seal_of_listed_segment, open_stream};

/// What the writer's tasks share.
struct Shared {
    /// The injected interfaces: metadata store, connector, clock.
    context: Arc<ClientContext>,
    /// The writer's settings.
    settings: WriterSettings,
    /// The name of the stream, for metadata operations and log lines.
    stream_name: String,
    /// The identifier of the stream, which segment addresses are built from.
    stream_id: StreamId,
    /// The epoch this writer claimed. Every request and every metadata write carries it.
    epoch: Epoch,
    /// The bookkeeping. Never locked across an `await`.
    state: Mutex<WriterState>,
    /// Bumped on every change of `state` that someone may be waiting for. The value itself means nothing.
    changes: watch::Sender<u64>,
}

impl Shared {
    /// Locks the state, whether or not another holder of the lock panicked.
    ///
    /// The state is only changed through methods that leave it consistent, so a panic in one task does not corrupt it
    /// for the others.
    fn lock(&self) -> MutexGuard<'_, WriterState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wakes every task that waits for the state to change.
    fn announce_change(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// Stops the writer and wakes everyone so they notice.
    fn stop(&self, error: &AppendError) {
        self.lock().stop(error);
        self.announce_change();
    }

    /// Stops the writer on behalf of a task working for `generation`, unless the writer has moved on to a newer segment
    /// since: what an old segment's replica says no longer concerns it.
    ///
    /// Returns whether this call stopped the writer. It does not if the writer has moved on, or was stopped before,
    /// which is how a sender's last answer after a close looks.
    fn stop_if_current(&self, generation: u64, error: &AppendError) -> bool {
        let mut state = self.lock();
        if state.generation() != generation || matches!(state.status(), Status::Stopped(_)) {
            return false;
        }
        state.stop(error);
        drop(state);
        self.announce_change();
        true
    }
}

/// The single writer of a stream.
///
/// Obtained from [`crate::BitacoraClient::open_writer`], which runs the takeover protocol first. Dropping the writer
/// without closing it leaves its segment open, to be recovered by the next writer; appends still in flight then resolve
/// as [`AppendError::Indeterminate`].
///
/// A writer can be shared between tasks behind an `Arc`: `append` takes `&self`, and concurrent appends each get their
/// own sequence numbers.
///
/// # Examples
///
/// ```
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions};
/// use bitacora_client::TakeoverPolicy;
/// use bitacora_types::SequenceNumber;
/// # let cluster = bitacora_testkit::Cluster::start(3).await;
/// # let client = BitacoraClient::new(
/// #     cluster.metadata().clone(),
/// #     cluster.connector(),
/// #     cluster.clock(),
/// #     ClientSettings::default(),
/// # );
/// # client.admin().create_stream("orders", StreamOptions::default()).await?;
///
/// let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
/// writer.append("order 1 placed".into()).await?;
/// writer.append("order 1 paid".into()).await?;
///
/// assert_eq!(writer.durable_sequence(), SequenceNumber::new(2));
/// writer.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct StreamWriter {
    /// What this handle shares with the writer's tasks.
    shared: Arc<Shared>,
    /// The driver task, which owns the senders. Aborted when the writer is dropped.
    driver: JoinHandle<()>,
}

impl StreamWriter {
    /// Takes over `stream_name` and starts the writer's tasks.
    ///
    /// Runs the takeover protocol (`takeover.rs`), then spawns the driver, which spawns one sender per replica of the
    /// segment the takeover opened.
    ///
    /// # Arguments
    ///
    /// * `context` - The injected interfaces of the client.
    /// * `settings` - The writer's settings.
    /// * `stream_name` - The stream to take over.
    /// * `policy` - Whether to wait for the previous writer's lease.
    ///
    /// # Returns
    ///
    /// The writer, with its segment open and its tasks running.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] as [`crate::BitacoraClient::open_writer`] documents it.
    pub(crate) async fn open(
        context: Arc<ClientContext>,
        settings: WriterSettings,
        stream_name: &str,
        policy: TakeoverPolicy,
    ) -> Result<Self, OpenError> {
        let OpenedStream {
            stream,
            epoch,
            segment,
            replicas,
        } = open_stream(&context, &settings, stream_name, policy).await?;

        let state = WriterState::new(
            segment.descriptor(stream.stream_id),
            replica_ids_of(&replicas),
            stream.replication.write_quorum(),
            Instant::now(),
        );
        let (changes, _first_subscriber) = watch::channel(0);
        let shared = Arc::new(Shared {
            context,
            settings,
            stream_name: stream_name.to_owned(),
            stream_id: stream.stream_id,
            epoch,
            state: Mutex::new(state),
            changes,
        });
        let driver = tokio::spawn(run_driver(Arc::clone(&shared), replicas));
        Ok(Self { shared, driver })
    }

    /// Appends one record and waits until it is committed: stored on a write quorum and visible to every reader.
    ///
    /// Waits first if too many records are still awaiting commit. Once the record has a sequence number it will be sent
    /// even if the returned future is dropped.
    ///
    /// # Arguments
    ///
    /// * `payload` - The record, opaque to the log. At most `bitacora_types::MAX_RECORD_PAYLOAD_BYTES` long.
    ///
    /// # Returns
    ///
    /// The record's sequence number.
    ///
    /// # Errors
    ///
    /// * [`AppendError::Fenced`] if another writer took the stream. The record may or may not be committed.
    /// * [`AppendError::Indeterminate`] if the writer was dropped before the outcome was known.
    /// * [`AppendError::Closed`] if the writer is closing. The record was not appended.
    /// * [`AppendError::Rejected`] if the record is too large. It was not appended.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    ///
    /// let commit_sequence = writer.append("order 1 placed".into()).await?;
    ///
    /// assert_eq!(commit_sequence, SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append(&self, payload: Bytes) -> Result<SequenceNumber, AppendError> {
        let range = self.append_batch(vec![payload]).await?;
        Ok(range.first())
    }

    /// Appends several records as one unit: they get consecutive sequence numbers and are committed together or not at
    /// all.
    ///
    /// The records travel as one checksummed batch. Waits like [`StreamWriter::append`].
    ///
    /// # Arguments
    ///
    /// * `payloads` - The records, at least one.
    ///
    /// # Returns
    ///
    /// The range of sequence numbers the records occupy, in the order given.
    ///
    /// # Errors
    ///
    /// As [`StreamWriter::append`]. [`AppendError::Rejected`] also if `payloads` is empty or the batch is too large.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// let range = writer
    ///     .append_batch(vec!["debit account 1".into(), "credit account 2".into()])
    ///     .await?;
    ///
    /// assert_eq!(range.to_string(), "1..=2");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn append_batch(&self, payloads: Vec<Bytes>) -> Result<SequenceRange, AppendError> {
        let (range, committed) = self.enqueue_under_flow_control(&payloads).await?;
        self.shared.announce_change();
        match committed.await {
            Ok(Ok(())) => Ok(range),
            Ok(Err(append_error)) => Err(append_error),
            Err(_writer_dropped) => Err(writer_gone()),
        }
    }

    /// Puts `payloads` into the writer's state as one batch, waiting first while too much is unacknowledged.
    ///
    /// A writer that is closing or stopped does not make the caller wait: the enqueue is attempted at once and fails
    /// with the writer's reason.
    async fn enqueue_under_flow_control(
        &self,
        payloads: &[Bytes],
    ) -> Result<(SequenceRange, oneshot::Receiver<CommitResult>), AppendError> {
        let mut changes = self.shared.changes.subscribe();
        loop {
            {
                let mut state = self.shared.lock();
                let can_enqueue_or_must_fail =
                    state.has_capacity(&self.shared.settings) || *state.status() != Status::Running;
                if can_enqueue_or_must_fail {
                    return state.enqueue(payloads);
                }
            }
            if changes.changed().await.is_err() {
                return Err(writer_gone());
            }
        }
    }

    /// The highest sequence number such that every record at or below it is committed.
    ///
    /// Reads the writer's own state; nothing is contacted.
    ///
    /// # Returns
    ///
    /// The durable sequence. For a writer that has appended nothing, the last sequence number of the stream before it
    /// took over.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::SequenceNumber;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// # let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    ///
    /// assert_eq!(writer.durable_sequence(), SequenceNumber::ZERO);
    /// writer.append("order 1 placed".into()).await?;
    /// assert_eq!(writer.durable_sequence(), SequenceNumber::FIRST);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn durable_sequence(&self) -> SequenceNumber {
        self.shared.lock().durable_sequence()
    }

    /// The epoch this writer holds.
    ///
    /// # Returns
    ///
    /// The epoch the writer claimed when it was opened. It identifies this writer's tenure of the stream and never
    /// changes for the life of the handle.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// use bitacora_types::Epoch;
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    ///
    /// assert_eq!(writer.epoch(), Epoch::new(1));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn epoch(&self) -> Epoch {
        self.shared.epoch
    }

    /// Stops accepting appends, waits for the accepted ones to commit, then seals the segment and releases the lease,
    /// so that the next writer can start without recovery.
    ///
    /// # Errors
    ///
    /// * [`AppendError::Fenced`] if another writer took the stream.
    /// * [`AppendError::Indeterminate`] if the accepted records could not be committed within the request timeout, or
    ///   the segment could not be sealed. The next writer then recovers the segment as after a crash.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread", start_paused = true)]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # use bitacora_client::{BitacoraClient, ClientSettings, StreamOptions, TakeoverPolicy};
    /// # let cluster = bitacora_testkit::Cluster::start(3).await;
    /// # let client = BitacoraClient::new(
    /// #     cluster.metadata().clone(),
    /// #     cluster.connector(),
    /// #     cluster.clock(),
    /// #     ClientSettings::default(),
    /// # );
    /// # client.admin().create_stream("orders", StreamOptions::default()).await?;
    /// let writer = client.open_writer("orders", TakeoverPolicy::Force).await?;
    /// writer.append("order 1 placed".into()).await?;
    ///
    /// writer.close().await?;
    ///
    /// let stream = client.admin().describe_stream("orders").await?.stream;
    /// assert!(stream.writer.is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(self) -> Result<(), AppendError> {
        self.shared.lock().begin_closing();
        self.shared.announce_change();

        let drained = match timeout(self.shared.settings.request_timeout, self.wait_until_drained()).await {
            Ok(drained) => drained,
            Err(_elapsed) => Err(AppendError::Indeterminate {
                reason: "accepted records did not commit before the writer closed".to_owned(),
            }),
        };
        let closed = match drained {
            Ok(()) => self.seal_and_release().await,
            Err(append_error) => Err(append_error),
        };
        let stop_reason = match &closed {
            Ok(()) => AppendError::Closed,
            Err(close_error) => close_error.clone(),
        };
        self.shared.stop(&stop_reason);
        closed
    }

    /// Waits until every accepted record is committed, or fails with the error the writer stopped with.
    async fn wait_until_drained(&self) -> Result<(), AppendError> {
        let mut changes = self.shared.changes.subscribe();
        loop {
            {
                let state = self.shared.lock();
                if let Status::Stopped(error) = state.status() {
                    return Err(error.clone());
                }
                if state.is_drained() {
                    return Ok(());
                }
            }
            if changes.changed().await.is_err() {
                return Err(writer_gone());
            }
        }
    }

    /// Seals the open segment at the durable sequence, gives up the lease, and tells the replicas the sealed end.
    async fn seal_and_release(&self) -> Result<(), AppendError> {
        let (segment, sealed_end): (SegmentDescriptor, SequenceNumber) = {
            let state = self.shared.lock();
            (state.segment(), state.durable_sequence())
        };
        let shared = &self.shared;
        let metadata = &shared.context.metadata;
        let sealed_at = shared.context.clock.now();
        metadata
            .seal_segment(
                &shared.stream_name,
                shared.epoch,
                segment.address.segment_id,
                sealed_end,
                sealed_at,
            )
            .await
            .map_err(|meta_error| close_error(&meta_error))?;
        metadata
            .release_lease(&shared.stream_name, shared.epoch)
            .await
            .map_err(|meta_error| close_error(&meta_error))?;
        let stream = metadata
            .get_stream(&shared.stream_name)
            .await
            .map_err(|meta_error| close_error(&meta_error))?;
        announce_seal_of_listed_segment(&shared.context, &stream, segment.address, sealed_end, &shared.settings).await;
        tracing::info!(stream = %shared.stream_name, %sealed_end, "writer closed");
        Ok(())
    }
}

impl Drop for StreamWriter {
    /// Stops the driver, which stops the senders it owns. Appends still waiting see the writer stop and report an
    /// unknown outcome.
    fn drop(&mut self) {
        self.driver.abort();
        self.shared.stop(&writer_gone());
    }
}

/// The identities of `replicas`, in the same order: what the writer's state indexes its replicas by.
fn replica_ids_of(replicas: &[ConnectedReplica]) -> Vec<ReplicaId> {
    let mut replica_ids = Vec::with_capacity(replicas.len());
    for replica in replicas {
        replica_ids.push(replica.replica_id.clone());
    }
    replica_ids
}

/// The error of an append whose writer went away before the outcome was known.
fn writer_gone() -> AppendError {
    AppendError::Indeterminate {
        reason: "the writer was dropped before the outcome was known".to_owned(),
    }
}

/// What a failed metadata write means for a writer that is closing: it lost the stream, or the outcome is unknown.
fn close_error(meta_error: &MetaError) -> AppendError {
    match meta_error {
        MetaError::Fenced { .. } => AppendError::Fenced,
        other => AppendError::Indeterminate {
            reason: format!("the segment could not be sealed: {other}"),
        },
    }
}

#[cfg(test)]
#[path = "writer_test.rs"]
mod tests;
