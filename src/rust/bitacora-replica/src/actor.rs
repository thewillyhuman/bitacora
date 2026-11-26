//! The task that owns one segment copy.
//!
//! # Why this file exists
//!
//! Requests for one segment arrive from many callers at once: a writer appending, readers reading, a
//! recovering writer fencing, the controller sealing and repairing. A copy's rules only hold if those
//! requests are applied one at a time, and a request that has started must finish even if its caller
//! hangs up, or a dropped connection could leave a write without its flush (design, section 17,
//! "Cancellation safety"). This file gives every copy a task of its own that does exactly that, and gives
//! the rest of the replica a handle to talk to it.
//!
//! # What it contains
//!
//! - [`SegmentHandle`]: the way to reach the task of a copy. Cheap to clone; one per caller is fine.
//! - `Command`: one request to the task, with the channel its answer goes back on.
//! - `Progress`: what a waiting reader needs to know to stop waiting.
//! - `run`: the task itself.
//!
//! # How it works
//!
//! A handle turns each operation into a command, sends it on the task's queue and awaits the answer on a
//! one-shot channel. The task takes commands in arrival order and runs each to completion on the copy
//! before it takes the next. If the caller has gone away meanwhile, the answer is dropped and the change
//! stays applied.
//!
//! After every command the task publishes the copy's progress on a watch channel. A read that may wait
//! for records does not queue behind anything: it watches that channel through its handle until there is
//! something to read, and only then sends its read command.
//!
//! After every command the task also asks the copy whether its disk refused a write, and records the
//! first refusal for the replica's host, once.
//!
//! A task ends when its copy is deleted, when every handle to it is gone, or when the replica stops and
//! aborts it.
//!
//! # Invariants
//!
//! - One task per copy, and only that task touches the copy. No lock guards a copy.
//! - Commands are applied in the order they were queued, one at a time.
//! - A command that was queued is applied even if nobody waits for its answer any more.
//! - This is on the path of every append and every read. A request costs one queue hop to the task and
//!   one back, one allocation for the reply channel, and nothing else here: the request is moved into the
//!   command, not copied, and the copy is called with a borrow of it. `run` is one loop with one `match`,
//!   so that this can be seen at a glance.
//!
//! # See also
//!
//! - `copy.rs` and `copy/`: what each command does to the copy.
//! - `replica.rs`: which handles exist, and `replica/api.rs`, which uses them.
//! - `write_failure.rs`: where a refused write is recorded.
//! - `replica/api_test.rs`: the tests of waiting reads, which drive this file through a replica.

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
use crate::write_failure::WriteFailureReport;

/// How many commands may queue for one segment before senders wait. Large enough that a writer's
/// pipeline and a few readers never wait on the queue itself, small enough to push back on a flood.
const COMMAND_QUEUE_LENGTH: usize = 64;

/// The channel an answer to a command goes back on.
type Reply<Response> = oneshot::Sender<Result<Response, ReplicaError>>;

/// One request to the task of a copy, with where to send its answer.
enum Command {
    /// Append batches, or take a heartbeat.
    Append {
        /// What to append and what the writer declares durable.
        request: AppendRequest,
        /// What the replica's disk allows right now.
        admission: Admission,
        /// Where the answer goes.
        reply: Reply<AppendResponse>,
    },
    /// Read batches.
    Read {
        /// What to read and how far the read may go.
        request: ReadRequest,
        /// Where the answer goes.
        reply: Reply<ReadResponse>,
    },
    /// Refuse writers below an epoch from now on.
    Fence {
        /// The epoch of the writer that is taking over.
        epoch: Epoch,
        /// Where the answer goes.
        reply: Reply<FenceResponse>,
    },
    /// Record where the segment ends for good.
    Seal {
        /// The segment's final sequence number.
        sealed_end: SequenceNumber,
        /// Where the answer goes.
        reply: Reply<SealResponse>,
    },
    /// Make the copy complete from another replica.
    Repair {
        /// The sealed segment and its sealed end.
        request: RepairRequest,
        /// A replica that holds a good copy.
        source: Arc<dyn ReplicaApi>,
        /// What the replica's disk allows right now.
        admission: Admission,
        /// How fast the repair may copy.
        pace: RepairPace,
        /// Where the answer goes.
        reply: Reply<RepairResponse>,
    },
    /// Reread the copy and mark it damaged if a checksum fails.
    Verify {
        /// Where the answer goes, once the copy was reread.
        reply: Reply<()>,
    },
    /// Report what the replica holds for the segment.
    Status {
        /// Where the answer goes: `None` if nothing is stored for the segment.
        reply: Reply<Option<SegmentStatus>>,
    },
    /// Remove everything stored for the segment and end the task.
    Delete {
        /// Where the answer goes.
        reply: Reply<()>,
    },
}

/// What readers waiting on a segment need to know to stop waiting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Progress {
    /// The last sequence number readers may be served, if the copy has records.
    readable_end: Option<SequenceNumber>,
    /// The segment's final sequence number, once the copy knows it is sealed.
    sealed_end: Option<SequenceNumber>,
}

impl Progress {
    /// Reads the progress of `copy` as it is now.
    fn of(copy: &SegmentCopy) -> Self {
        Self {
            readable_end: copy.readable_end(),
            sealed_end: copy.sealed_end(),
        }
    }

    /// Tells whether a reader waiting for `from_sequence` has a reason to stop waiting: there is a record
    /// for it to read, or the segment is sealed and no record will ever come.
    fn has_news_for(&self, from_sequence: SequenceNumber) -> bool {
        if self.sealed_end.is_some() {
            return true;
        }
        match self.readable_end {
            Some(readable_end) => readable_end >= from_sequence,
            None => false,
        }
    }
}

/// The way to reach the task that owns a segment copy.
///
/// Every method sends one command and awaits its answer. A handle is two channel ends, so cloning one is
/// cheap, and any number of clones may be used at once.
#[derive(Clone)]
pub(crate) struct SegmentHandle {
    /// The queue of the task.
    commands: mpsc::Sender<Command>,
    /// The copy's progress as the task last published it, for reads that wait.
    progress: watch::Receiver<Progress>,
}

impl SegmentHandle {
    /// Starts a task that owns `copy` and returns the handle to it.
    ///
    /// The task lives in `tasks`: aborting or dropping the set stops it.
    ///
    /// # Arguments
    ///
    /// * `address` - The segment `copy` is a copy of, to name it when a write is refused.
    /// * `copy` - The copy the task will own.
    /// * `tasks` - The set the task is spawned into.
    /// * `write_failures` - Where the task records that the disk refused a write to the copy.
    ///
    /// # Returns
    ///
    /// The handle to the new task.
    pub(crate) fn spawn(
        address: SegmentAddress,
        copy: SegmentCopy,
        tasks: &mut JoinSet<()>,
        write_failures: WriteFailureReport,
    ) -> Self {
        let (command_sender, command_receiver) = mpsc::channel(COMMAND_QUEUE_LENGTH);
        let (progress_sender, progress_receiver) = watch::channel(Progress::of(&copy));
        tasks.spawn(run(address, copy, command_receiver, progress_sender, write_failures));
        Self {
            commands: command_sender,
            progress: progress_receiver,
        }
    }

    /// Appends batches to the copy, or delivers a heartbeat.
    ///
    /// # Arguments
    ///
    /// * `request` - What to append and what the writer declares durable.
    /// * `admission` - What the replica's disk allows right now.
    ///
    /// # Returns
    ///
    /// The copy's end after the request.
    ///
    /// # Errors
    ///
    /// Returns what `SegmentCopy::append` returns, or [`ReplicaError::Unavailable`] if the task has
    /// stopped.
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

    /// Reads batches from the copy.
    ///
    /// # Arguments
    ///
    /// * `request` - What to read and how far the read may go.
    ///
    /// # Returns
    ///
    /// The frames, the last readable sequence number and the sealed end if known.
    ///
    /// # Errors
    ///
    /// Returns what `SegmentCopy::read` returns, or [`ReplicaError::Unavailable`] if the task has stopped.
    pub(crate) async fn read(&self, request: ReadRequest) -> Result<ReadResponse, ReplicaError> {
        self.ask(|reply| Command::Read { request, reply }).await
    }

    /// Fences the copy against writers below `epoch`.
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch of the writer that is taking over.
    ///
    /// # Returns
    ///
    /// The copy's end and its sealed end if known.
    ///
    /// # Errors
    ///
    /// Returns what `SegmentCopy::fence` returns, or [`ReplicaError::Unavailable`] if the task has
    /// stopped.
    pub(crate) async fn fence(&self, epoch: Epoch) -> Result<FenceResponse, ReplicaError> {
        self.ask(|reply| Command::Fence { epoch, reply }).await
    }

    /// Seals the copy at `sealed_end`.
    ///
    /// # Arguments
    ///
    /// * `sealed_end` - The segment's final sequence number.
    ///
    /// # Returns
    ///
    /// The copy's end after the seal.
    ///
    /// # Errors
    ///
    /// Returns what `SegmentCopy::seal` returns, or [`ReplicaError::Unavailable`] if the task has stopped.
    pub(crate) async fn seal(&self, sealed_end: SequenceNumber) -> Result<SealResponse, ReplicaError> {
        self.ask(|reply| Command::Seal { sealed_end, reply }).await
    }

    /// Makes the copy a complete copy of a sealed segment from `source`.
    ///
    /// The task does nothing else for this segment until the repair is over.
    ///
    /// # Arguments
    ///
    /// * `request` - The sealed segment and its sealed end.
    /// * `source` - A replica that holds a good copy.
    /// * `admission` - What the replica's disk allows right now.
    /// * `pace` - How fast the repair may copy.
    ///
    /// # Returns
    ///
    /// The end of the repaired copy.
    ///
    /// # Errors
    ///
    /// Returns what `SegmentCopy::repair` returns, or [`ReplicaError::Unavailable`] if the task has
    /// stopped.
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

    /// Rereads the whole copy and marks it damaged if a checksum fails.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if the task has stopped. Finding damage is not an error: it
    /// shows in the copy's status.
    pub(crate) async fn verify(&self) -> Result<(), ReplicaError> {
        self.ask(|reply| Command::Verify { reply }).await
    }

    /// Reports what the replica holds for the segment.
    ///
    /// # Returns
    ///
    /// The copy's status, or `None` if nothing is stored for the segment.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Unavailable`] if the task has stopped.
    pub(crate) async fn status(&self) -> Result<Option<SegmentStatus>, ReplicaError> {
        self.ask(|reply| Command::Status { reply }).await
    }

    /// Removes everything stored for the segment and ends the task.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaError::Storage`] if the disk refuses to remove a file, or
    /// [`ReplicaError::Unavailable`] if the task has stopped. The task ends in every case.
    pub(crate) async fn delete(&self) -> Result<(), ReplicaError> {
        self.ask(|reply| Command::Delete { reply }).await
    }

    /// Waits until a reader at `from_sequence` has something to read or the segment is sealed, for at
    /// most `longest_wait`.
    ///
    /// Sends nothing to the task: it watches the progress the task publishes. Returns in every case; the
    /// caller then reads and answers with whatever is readable, which may be nothing.
    ///
    /// # Arguments
    ///
    /// * `from_sequence` - The first sequence number the reader wants.
    /// * `longest_wait` - How long to wait at most.
    pub(crate) async fn wait_until_readable(&self, from_sequence: SequenceNumber, longest_wait: Duration) {
        let mut progress = self.progress.clone();
        let has_news = |progress: &Progress| progress.has_news_for(from_sequence);
        // Both outcomes lead to the same next step, as does a task that stopped: the caller reads.
        let _timed_out_or_stopped = tokio::time::timeout(longest_wait, progress.wait_for(has_news)).await;
    }

    /// Sends one command and awaits its answer. `command_for` builds the command around the channel the
    /// answer comes back on.
    async fn ask<Response>(
        &self,
        command_for: impl FnOnce(Reply<Response>) -> Command,
    ) -> Result<Response, ReplicaError> {
        let (reply_sender, reply_receiver) = oneshot::channel();
        if self.commands.send(command_for(reply_sender)).await.is_err() {
            return Err(task_stopped());
        }
        match reply_receiver.await {
            Ok(answer) => answer,
            Err(_reply_dropped) => Err(task_stopped()),
        }
    }
}

/// Builds the answer to a request whose segment task is gone: the replica was stopped, or the copy was
/// deleted while the request was on its way.
fn task_stopped() -> ReplicaError {
    ReplicaError::Unavailable {
        reason: "the segment's task has stopped".to_owned(),
    }
}

/// Owns `copy` until it is deleted or every handle is gone.
///
/// Takes commands from `commands` in order and runs each to completion. After each one it publishes the
/// copy's progress on `progress` and, the first time the copy's disk refused a write, records that in
/// `write_failures`.
///
/// An answer can only fail to be delivered if the requester went away, and the change it asked for is
/// applied either way; that is why every send of an answer ignores its result.
async fn run(
    address: SegmentAddress,
    mut copy: SegmentCopy,
    mut commands: mpsc::Receiver<Command>,
    progress: watch::Sender<Progress>,
    write_failures: WriteFailureReport,
) {
    let mut has_reported_refused_write = false;
    while let Some(command) = commands.recv().await {
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
            Command::Status { reply } => drop(reply.send(Ok(status_if_stored(&copy)))),
            Command::Delete { reply } => {
                drop(reply.send(copy.delete().await));
                return;
            }
        }
        progress.send_replace(Progress::of(&copy));
        if !has_reported_refused_write && let Some(reason) = copy.refused_write() {
            has_reported_refused_write = true;
            write_failures.record(address, reason);
        }
    }
}

/// Reports the status of `copy`, or `None` if the replica has nothing stored for its segment: a copy
/// that only exists because somebody asked about it is not listed.
fn status_if_stored(copy: &SegmentCopy) -> Option<SegmentStatus> {
    if copy.is_stored() { Some(copy.status()) } else { None }
}

#[cfg(test)]
#[path = "actor_test.rs"]
mod tests;
