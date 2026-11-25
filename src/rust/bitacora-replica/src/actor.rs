//! The task that owns one segment copy.
//!
//! Every state-changing operation on a copy runs here, in arrival order and
//! to completion (design, section 17, "Cancellation safety"). A request
//! handler only sends a command and awaits the answer, so a handler dropped
//! by a disconnecting client can never leave a half-applied change.

use std::sync::Arc;
use std::time::Duration;

use bitacora_types::replica_api::{
    AppendRequest, AppendResponse, FenceResponse, ReadRequest, ReadResponse, RepairRequest, RepairResponse, ReplicaApi,
    ReplicaError, SealResponse, SegmentStatus,
};
use bitacora_types::{Epoch, SegmentAddress, SequenceNumber};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::copy::{Admission, RepairPace, SegmentCopy};

/// How many commands may queue for one segment before senders wait.
const COMMAND_QUEUE_LENGTH: usize = 64;

/// Called when the disk refuses a write or a flush, with the segment and the
/// disk's reason. The production handler stops the process.
pub type WriteFailureHandler = Arc<dyn Fn(SegmentAddress, &str) + Send + Sync>;

type Reply<Response> = oneshot::Sender<Result<Response, ReplicaError>>;

enum Command {
    Append {
        request: AppendRequest,
        admission: Admission,
        reply: Reply<AppendResponse>,
    },
    Read {
        request: ReadRequest,
        reply: Reply<ReadResponse>,
    },
    Fence {
        epoch: Epoch,
        reply: Reply<FenceResponse>,
    },
    Seal {
        sealed_end: SequenceNumber,
        reply: Reply<SealResponse>,
    },
    Repair {
        request: RepairRequest,
        source: Arc<dyn ReplicaApi>,
        admission: Admission,
        pace: RepairPace,
        reply: Reply<RepairResponse>,
    },
    Verify {
        reply: Reply<()>,
    },
    Status {
        reply: Reply<Option<SegmentStatus>>,
    },
    Delete {
        reply: Reply<()>,
    },
}

/// What readers waiting on a segment need to know to stop waiting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Progress {
    /// The last sequence number readers may be served, if the copy has data.
    readable_end: Option<SequenceNumber>,
    /// The segment's final sequence number, once sealed.
    sealed_end: Option<SequenceNumber>,
}

impl Progress {
    fn of(copy: &SegmentCopy) -> Self {
        Self {
            readable_end: copy.readable_end(),
            sealed_end: copy.sealed_end(),
        }
    }
}

/// The way to reach the task that owns a segment copy.
#[derive(Clone)]
pub(crate) struct SegmentHandle {
    commands: mpsc::Sender<Command>,
    progress: watch::Receiver<Progress>,
}

impl SegmentHandle {
    /// Starts a task that owns `copy` and returns the handle to it. The task
    /// lives in `tasks`, so dropping the set stops it.
    pub(crate) fn spawn(
        address: SegmentAddress,
        copy: SegmentCopy,
        tasks: &mut JoinSet<()>,
        on_write_failure: WriteFailureHandler,
    ) -> Self {
        let (command_sender, command_receiver) = mpsc::channel(COMMAND_QUEUE_LENGTH);
        let (progress_sender, progress_receiver) = watch::channel(Progress::of(&copy));
        tasks.spawn(run(address, copy, command_receiver, progress_sender, on_write_failure));
        Self {
            commands: command_sender,
            progress: progress_receiver,
        }
    }

    pub(crate) async fn append(
        &self,
        request: AppendRequest,
        admission: Admission,
    ) -> Result<AppendResponse, ReplicaError> {
        self.ask(|reply| Command::Append {
            request,
            admission,
            reply,
        })
        .await
    }

    pub(crate) async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.ask(|reply| Command::Read { request, reply }).await
    }

    pub(crate) async fn fence(&self, epoch: Epoch) -> Result<FenceResponse, ReplicaError> {
        self.ask(|reply| Command::Fence { epoch, reply }).await
    }

    pub(crate) async fn seal(&self, sealed_end: SequenceNumber) -> Result<SealResponse, ReplicaError> {
        self.ask(|reply| Command::Seal { sealed_end, reply }).await
    }

    pub(crate) async fn repair(
        &self,
        request: RepairRequest,
        source: Arc<dyn ReplicaApi>,
        admission: Admission,
        pace: RepairPace,
    ) -> Result<RepairResponse, ReplicaError> {
        self.ask(|reply| Command::Repair {
            request,
            source,
            admission,
            pace,
            reply,
        })
        .await
    }

    pub(crate) async fn verify(&self) -> Result<(), ReplicaError> {
        self.ask(|reply| Command::Verify { reply }).await
    }

    /// Reports the copy's status, or `None` if nothing is stored for it.
    pub(crate) async fn status(&self) -> Result<Option<SegmentStatus>, ReplicaError> {
        self.ask(|reply| Command::Status { reply }).await
    }

    /// Deletes the copy and stops its task.
    pub(crate) async fn delete(&self) -> Result<(), ReplicaError> {
        self.ask(|reply| Command::Delete { reply }).await
    }

    /// Waits until a reader at `from_sequence` has something to read or the
    /// segment is sealed, for at most `longest_wait`.
    pub(crate) async fn wait_until_readable(&self, from_sequence: SequenceNumber, longest_wait: Duration) {
        let mut progress = self.progress.clone();
        let has_news = |progress: &Progress| {
            progress.sealed_end.is_some()
                || progress
                    .readable_end
                    .is_some_and(|readable_end| readable_end >= from_sequence)
        };
        // Both outcomes lead to the same next step: the caller reads and
        // answers with whatever is readable, which may be nothing.
        let _timed_out_or_stopped = tokio::time::timeout(longest_wait, progress.wait_for(has_news)).await;
    }

    /// Sends one command and awaits its answer.
    async fn ask<Response>(
        &self,
        command_for: impl FnOnce(Reply<Response>) -> Command,
    ) -> Result<Response, ReplicaError> {
        let (reply_sender, reply_receiver) = oneshot::channel();
        self.commands
            .send(command_for(reply_sender))
            .await
            .map_err(|_| stopped())?;
        reply_receiver.await.map_err(|_| stopped())?
    }
}

fn stopped() -> ReplicaError {
    ReplicaError::Unavailable {
        reason: "the segment's task has stopped".to_owned(),
    }
}

/// Owns `copy` until it is deleted or every handle is gone.
async fn run(
    address: SegmentAddress,
    mut copy: SegmentCopy,
    mut commands: mpsc::Receiver<Command>,
    progress: watch::Sender<Progress>,
    on_write_failure: WriteFailureHandler,
) {
    let mut has_reported_write_failure = false;
    while let Some(command) = commands.recv().await {
        // A reply can only fail to be delivered if the requester went away,
        // and the change it asked for is applied either way.
        match command {
            Command::Append {
                request,
                admission,
                reply,
            } => drop(reply.send(copy.append(&request, admission).await)),
            Command::Read { request, reply } => drop(reply.send(copy.read(&request).await)),
            Command::Fence { epoch, reply } => drop(reply.send(copy.fence(epoch).await)),
            Command::Seal { sealed_end, reply } => drop(reply.send(copy.seal(sealed_end).await)),
            Command::Repair {
                request,
                source,
                admission,
                pace,
                reply,
            } => drop(reply.send(copy.repair(&request, source.as_ref(), admission, pace).await)),
            Command::Verify { reply } => {
                copy.verify().await;
                drop(reply.send(Ok(())));
            }
            Command::Status { reply } => {
                drop(reply.send(Ok(copy.is_stored().then(|| copy.status()))));
            }
            Command::Delete { reply } => {
                drop(reply.send(copy.delete().await));
                return;
            }
        }
        progress.send_replace(Progress::of(&copy));
        if let (false, Some(reason)) = (has_reported_write_failure, copy.failed_write()) {
            has_reported_write_failure = true;
            on_write_failure(address, reason);
        }
    }
}
