//! The writer's sender tasks: one per replica of the open segment.
//!
//! # Why this file exists
//!
//! A record is committed when a write quorum of replicas has flushed it, and the replicas answer at different speeds.
//! If one loop sent to all of them in turn, the slowest would set the pace. So every replica of the open segment gets
//! its own task, which sends that replica whatever it is missing and reports what it acknowledged; the commit rule in
//! `state.rs` then needs only the fastest quorum (design, section 5).
//!
//! # What it contains
//!
//! - `run_sender`: the task.
//! - `ReplicaSender`: the task's own variables, and the steps of its loop as methods (`has_news`, `send_next_request`,
//!   `handle_answer`).
//!
//! # How it works
//!
//! The task loops over three steps:
//!
//! 1. Look at the state. If the writer stopped or moved to a newer segment, the task ends. If there is nothing new for
//!    this replica and no heartbeat is due, wait for a change or for the heartbeat.
//! 2. Ask the state for the next request, which pipelines every batch the replica has not acknowledged up to the
//!    request size limit, and send it with a timeout. A request without batches is a heartbeat: it carries the durable
//!    sequence, which is how replicas learn what they may serve to readers.
//! 3. Report the answer to the state. An acknowledgement may commit records and wakes whoever waits for that.
//!
//! What the answer means:
//!
//! | answer                         | the sender                                                              |
//! |--------------------------------|-------------------------------------------------------------------------|
//! | acknowledged                   | records the replica's local end                                         |
//! | out of order                   | resumes from the local end the replica reports                          |
//! | fenced, or sealed              | stops the writer: a newer writer took the stream                        |
//! | sealed, while the writer rolls | waits one retry interval: the seal is the roll's own                    |
//! | unavailable, or no answer      | waits one retry interval and sends again                                |
//! | any other refusal              | records it, so that the driver rolls away, and waits one retry interval |
//!
//! # Invariants
//!
//! - This is the hot path of the writer, with `append`. A request that is acknowledged costs exactly three short locks
//!   of the state (look, plan, record) and one bump of the version. Nothing is copied here: the request body is the one
//!   reference-counted buffer that `plan_request` built, and the request takes it as it is. Nothing is done per record,
//!   and the steps are methods of one struct that the task owns, so the loop being split into them costs no
//!   allocation, lock or channel.
//! - The state is never locked across an `await`.
//! - A sender only speaks for the generation it was started for. Once the writer rolls, what the old segment's replicas
//!   say no longer changes anything, and the task ends at its next look.
//! - A silent replica is not this task's problem to solve: it keeps retrying, and the driver, seeing no contact, rolls
//!   away from it.
//! - While the writer rolls, "sealed" is no verdict. The roll seals the open segment in the stream's record, and from
//!   that moment the controller may tell the segment's replicas, which can be seconds before the writer has opened
//!   the next segment: the roll reads the metadata store once more in between. A sender that took that seal for a
//!   takeover stopped a writer nobody had taken the stream from. If it is a takeover after all, the roll finds out:
//!   its conditional write is refused, or the replicas of the segment it opens are fenced. The Go client has the same
//!   rule (`src/go/bitacora/writer.go`).
//!
//! # See also
//!
//! - `state.rs`: `plan_request` and `record_acknowledgement`, which decide what is sent and what is committed.
//! - `driver.rs`: spawns these tasks and reacts to what they record.
//! - `bitacora-types/src/replica_api.rs`: `AppendRequest` and the errors a replica answers with.

use std::ops::ControlFlow;
use std::sync::Arc;

use bitacora_types::SequenceNumber;
use bitacora_types::replica_api::{AppendRequest, AppendResponse, ReplicaApi, ReplicaError};
use tokio::time::error::Elapsed;
use tokio::time::{Instant, sleep, timeout, timeout_at};

use super::Shared;
use super::state::{RequestPlan, Status};
use crate::error::AppendError;

/// What a replica answered to an append request, or that it did not answer in time.
type AppendAnswer = Result<Result<AppendResponse, ReplicaError>, Elapsed>;

/// The variables of one sender task.
struct ReplicaSender {
    /// What the writer's tasks share.
    shared: Arc<Shared>,
    /// The generation of the segment this sender works for.
    generation: u64,
    /// The position of the replica in the segment's replica set.
    replica_index: usize,
    /// The replica.
    replica: Arc<dyn ReplicaApi>,
    /// The durable sequence the replica was last told and acknowledged, if any. A heartbeat is due early whenever the
    /// writer's durable sequence differs from it.
    last_durable_sent: Option<SequenceNumber>,
    /// When the next request is due even without news.
    heartbeat_due_at: Instant,
}

/// Keeps one replica of the segment opened at `generation` up to date, until the writer stops or rolls.
///
/// See the file header for the loop and for what each answer means.
///
/// # Arguments
///
/// * `shared` - What the writer's tasks share.
/// * `generation` - The generation of the segment this sender works for.
/// * `replica_index` - The position of the replica in the segment's replica set.
/// * `replica` - The replica.
pub(super) async fn run_sender(
    shared: Arc<Shared>,
    generation: u64,
    replica_index: usize,
    replica: Arc<dyn ReplicaApi>,
) {
    let mut changes = shared.changes.subscribe();
    let mut sender = ReplicaSender {
        shared,
        generation,
        replica_index,
        replica,
        last_durable_sent: None,
        heartbeat_due_at: Instant::now(),
    };
    loop {
        let Some(has_news) = sender.has_news() else {
            return;
        };
        if !has_news && Instant::now() < sender.heartbeat_due_at {
            // Wait for news or for the heartbeat to fall due.
            let _woken_by = timeout_at(sender.heartbeat_due_at, changes.changed()).await;
            continue;
        }
        let Some(plan) = sender.plan_request() else {
            return;
        };
        let durable_sent = plan.durable_sequence;
        let answer = sender.send(plan).await;
        if sender.handle_answer(answer, durable_sent).await.is_break() {
            return;
        }
    }
}

impl ReplicaSender {
    /// Whether there is something to tell the replica: batches it has not acknowledged, or a durable sequence it has
    /// not been told. `None` means the sender's work is over: the writer stopped or moved to a newer segment.
    fn has_news(&self) -> Option<bool> {
        let state = self.shared.lock();
        if matches!(state.status(), Status::Stopped(_)) || state.generation() != self.generation {
            return None;
        }
        let has_batches = state.has_batches_for(self.generation, self.replica_index);
        Some(has_batches || Some(state.durable_sequence()) != self.last_durable_sent)
    }

    /// Asks the state for the next request to this replica. `None` means the writer moved to a newer segment.
    fn plan_request(&self) -> Option<RequestPlan> {
        self.shared.lock().plan_request(
            self.generation,
            self.replica_index,
            self.shared.settings.max_request_bytes,
        )
    }

    /// Sends `plan` to the replica and waits for the answer, at most one request timeout. Whatever the answer, the next
    /// heartbeat is due one heartbeat interval after it.
    async fn send(&mut self, plan: RequestPlan) -> AppendAnswer {
        let request = AppendRequest {
            segment: plan.segment,
            writer_epoch: self.shared.epoch,
            batch_frames: plan.batch_frames,
            durable_sequence: plan.durable_sequence,
        };
        let answer = timeout(self.shared.settings.request_timeout, self.replica.append(request)).await;
        self.heartbeat_due_at = Instant::now() + self.shared.settings.heartbeat_interval;
        answer
    }

    /// Reports `answer` to the state and waits where the answer calls for a pause.
    ///
    /// `durable_sent` is the durable sequence the request carried. Breaks if the task must end: the answer shows that
    /// the writer lost the stream.
    async fn handle_answer(&mut self, answer: AppendAnswer, durable_sent: SequenceNumber) -> ControlFlow<()> {
        match answer {
            Ok(Ok(acknowledged)) => {
                self.last_durable_sent = Some(durable_sent);
                self.record_local_end(acknowledged.local_end);
            }
            Ok(Err(ReplicaError::OutOfOrder { local_end, .. })) => {
                // The replica's copy ends elsewhere than assumed, typically after a request whose answer was lost.
                // Resume from there.
                let has_progressed = self.record_local_end(local_end);
                if !has_progressed {
                    // The replica holds less than it acknowledged, so the same request would be refused again. Pause
                    // until the driver rolls away from it.
                    sleep(self.shared.settings.retry_interval).await;
                }
            }
            // This writer's own roll sealed the segment, and the controller told the replica before the roll finished
            // here: see the invariants of this file.
            Ok(Err(ReplicaError::Sealed { .. })) if self.shared.lock().is_rolling() => {
                sleep(self.shared.settings.retry_interval).await;
            }
            // A fence, or a seal this writer did not make, on the open segment can only come from a newer writer's
            // takeover.
            Ok(Err(ReplicaError::Fenced { .. } | ReplicaError::Sealed { .. })) => {
                if self.shared.stop_if_current(self.generation, &AppendError::Fenced) {
                    tracing::warn!(stream = %self.shared.stream_name, "lost the stream");
                }
                return ControlFlow::Break(());
            }
            Ok(Err(ReplicaError::Unavailable { .. })) | Err(_) => {
                sleep(self.shared.settings.retry_interval).await;
            }
            Ok(Err(lasting_refusal)) => {
                tracing::warn!(stream = %self.shared.stream_name, %lasting_refusal, "replica refuses appends");
                self.record_refusal();
                sleep(self.shared.settings.retry_interval).await;
            }
        }
        ControlFlow::Continue(())
    }

    /// Records that the replica's copy ends at `local_end` and wakes whoever waits for a commit or for capacity.
    /// Returns whether the replica moved forward.
    fn record_local_end(&self, local_end: SequenceNumber) -> bool {
        let has_progressed = self.shared.lock().record_acknowledgement(
            self.generation,
            self.replica_index,
            local_end,
            Instant::now(),
            &self.shared.settings,
        );
        self.shared.announce_change();
        has_progressed
    }

    /// Records that the replica answered but will keep refusing appends, and wakes the driver, which rolls away.
    fn record_refusal(&self) {
        {
            let mut state = self.shared.lock();
            state.record_contact(self.generation, self.replica_index, Instant::now());
            state.record_refusal(self.generation, self.replica_index);
        }
        self.shared.announce_change();
    }
}
