//! One round of a soak: fresh streams, a workload under faults, healing, the audit, the verdict.
//!
//! # Why this file exists
//!
//! A soak that kept one history for a week would need the memory of a week, and a cluster that kept its streams
//! would need the disks. So a soak is cut into rounds. Each round is a whole experiment of its own, like one seed of
//! the simulator: it makes its streams, drives them while the nemesis breaks things, lets the cluster heal, reads
//! everything back and judges it. A clean round then removes what it made, and the next one starts from nothing.
//! This file is one such round.
//!
//! # What it contains
//!
//! - [`RoundSettings`]: how long a round lasts, how many streams it writes, and how.
//! - [`RoundContext`]: what a round is told about the run it belongs to.
//! - [`run_round`]: plays one round and reports it.
//! - `Round`: the state of a round, with one method per step.
//! - `Clients`: the writers and readers of a round.
//!
//! # How it works
//!
//! 1. The round creates its streams, named after the run, the round and a number.
//! 2. Every stream gets a writer and a reader, which record what they see (`workload.rs`). Meanwhile the nemesis
//!    injects faults until the round's time is up (`nemesis.rs`).
//! 3. Every cut is removed, and the round waits until the cluster is healthy again (`health.rs`). The writers are
//!    still writing: a cluster that has recovered must be able to take writes.
//! 4. The writers are told to stop, and every stream is read back whole (`audit.rs`).
//! 5. The history of every stream is judged (`judge.rs`). A clean round deletes its streams. A round that is not
//!    clean keeps them and writes its histories to files.
//!
//! A round that cannot get through steps 1, 3 or 4 in time is not audited: the cluster did not recover, and nothing
//! is known about its invariants. That is a verdict of its own, never a clean one.
//!
//! # Invariants
//!
//! - A round is reported as clean only if every one of its streams was read back whole and the checker found
//!   nothing in any of them.
//! - When a round ends, however it ends, no cut that the tool made is in place, unless removing it failed for the
//!   whole healing time; the report then says so.
//! - The histories of a round live in the round and die with it. Nothing of them reaches the next round.
//! - The steps await in the order above. `Round::play` is one function for that reason.
//!
//! # See also
//!
//! - `soak.rs`: runs round after round.
//! - `report.rs`: the report a round returns.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bitacora::StreamOptions;
use bitacora_meta::MetaError;
use bitacora_sim::{AppendOutcome, TimedEvent};
use bitacora_types::{Epoch, ReplicaId, SequenceNumber, Timestamp};
use bytes::Bytes;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep, timeout};

use crate::audit::audit_stream;
use crate::cluster::SoakedCluster;
use crate::faults::FaultKind;
use crate::health::wait_until_healthy;
use crate::judge::{violations_of, write_dump};
use crate::nemesis::{Nemesis, NemesisMemory, NemesisSettings, NemesisTally, is_told_to_stop, remove_cuts_within};
use crate::report::{HistoryCounts, RoundReport, RoundVerdict};
use crate::workload::{SoakedStream, WorkloadCounters, WorkloadSettings, run_reader, run_writer};

/// How long a round waits before it tries again to create a stream.
const CREATE_RETRY_PAUSE: Duration = Duration::from_secs(1);

/// How long the writers of a round may take to finish their append and step down once they are told to stop.
const WRITER_STOP_TIME_LIMIT: Duration = Duration::from_secs(60);

/// Where a drill says a record was committed: far beyond the end of any stream a round writes.
const DRILL_SEQUENCE: SequenceNumber = SequenceNumber::new(1 << 40);

/// The epoch a drill's record claims: above that of every writer of the round, so that only its absence is wrong.
const DRILL_EPOCH: Epoch = Epoch::new(1 << 40);

/// How a round is played.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoundSettings {
    /// How long the workload and the nemesis run.
    pub(crate) duration: Duration,
    /// How many streams a round writes.
    pub(crate) stream_count: u64,
    /// How the writers write.
    pub(crate) workload: WorkloadSettings,
    /// What the nemesis does.
    pub(crate) nemesis: NemesisSettings,
    /// How long the cluster is given to recover once the faults are gone, and each stream to be read back.
    pub(crate) heal_time_limit: Duration,
    /// Where the histories of a round that is not clean are written.
    pub(crate) dump_directory: PathBuf,
}

/// What a round is told about the run it belongs to.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RoundContext<'run> {
    /// The name of the run, which starts the name of every stream.
    pub(crate) run: &'run str,
    /// The number of the round within the run, from 1.
    pub(crate) number: u64,
    /// The replicas the run started with, which must all be alive again before the round is judged.
    pub(crate) expected_replicas: &'run BTreeSet<ReplicaId>,
    /// Whether the round claims a commit that never happened, to prove that a violation ends the run.
    pub(crate) plants_lost_record: bool,
}

/// Plays one round against `cluster` and reports it.
///
/// # Arguments
///
/// * `cluster` - The cluster under test.
/// * `settings` - How the round is played.
/// * `context` - The run, the number of the round, and what the run expects of the cluster.
/// * `memory` - What the nemesis carries from round to round: its random source and the kinds of fault it gave up.
/// * `stop` - Becomes `true`, or is dropped, when the tool must end.
///
/// # Returns
///
/// The report of the round. Its verdict says whether the round was clean, broke an invariant, could not be audited,
/// or was interrupted.
pub(crate) async fn run_round(
    cluster: &SoakedCluster,
    settings: &RoundSettings,
    context: RoundContext<'_>,
    memory: &mut NemesisMemory,
    stop: &mut watch::Receiver<bool>,
) -> RoundReport {
    let started_at = cluster.clock.now();
    let mut round = Round::new(cluster, settings, context);
    let played = round.play(memory, stop).await;
    for (kind, reason) in &memory.disabled {
        round.faults_disabled.insert(kind.report_name(), reason.clone());
    }
    round.conclude(played, started_at).await
}

/// How far a round got.
enum Played {
    /// Every stream was read back whole, under these reading sessions, one per stream.
    Audited(Vec<u64>),
    /// The cluster did not recover in time, for this reason.
    NotAudited(String),
    /// The tool was told to stop. If a cut could not be removed on the way out, this is why.
    Interrupted(Option<String>),
}

/// How a round was judged.
struct Judgement {
    /// The verdict.
    verdict: RoundVerdict,
    /// What the checker found, each text starting with the stream.
    violations: Vec<String>,
    /// Why the round was not audited, if it was not.
    reason: Option<String>,
}

impl Judgement {
    /// The judgement of a round that no checker looked at: it ended as `verdict`, for `reason`.
    fn without_audit(verdict: RoundVerdict, reason: Option<String>) -> Self {
        Self {
            verdict,
            violations: Vec::new(),
            reason,
        }
    }
}

/// One round and what it has done so far.
struct Round<'run> {
    /// The cluster under test.
    cluster: &'run SoakedCluster,
    /// How the round is played.
    settings: &'run RoundSettings,
    /// The run and the number of the round.
    context: RoundContext<'run>,
    /// The streams of the round, with their histories.
    streams: Vec<SoakedStream>,
    /// What the clients count as it happens.
    counters: Arc<WorkloadCounters>,
    /// What the nemesis did.
    tally: NemesisTally,
    /// The kinds of fault the nemesis has given up by the end of the round, each with the reason.
    faults_disabled: BTreeMap<&'static str, String>,
}

impl<'run> Round<'run> {
    /// A round with its streams named and nothing done yet.
    fn new(cluster: &'run SoakedCluster, settings: &'run RoundSettings, context: RoundContext<'run>) -> Self {
        let mut streams = Vec::new();
        for stream_number in 0..settings.stream_count {
            let name = format!("{}-{}-{stream_number}", context.run, context.number);
            streams.push(SoakedStream::new(name));
        }
        Self {
            cluster,
            settings,
            context,
            streams,
            counters: Arc::new(WorkloadCounters::default()),
            tally: NemesisTally::default(),
            faults_disabled: BTreeMap::new(),
        }
    }

    /// Takes the round through its steps, up to the audit. See the file header for the order.
    async fn play(&mut self, memory: &mut NemesisMemory, stop: &mut watch::Receiver<bool>) -> Played {
        if self.streams.is_empty() {
            return Played::NotAudited("a round without streams has nothing to audit".to_owned());
        }
        if let Err(reason) = self.create_streams().await {
            return Played::NotAudited(reason);
        }
        let clients = Clients::start(self.cluster, &self.streams, self.settings.workload, &self.counters);
        let nemesis = Nemesis::new(
            self.cluster,
            &self.settings.nemesis,
            self.context.expected_replicas,
            &self.streams,
        );
        let workload_ends_at = Instant::now() + self.settings.duration;
        self.tally = nemesis.run(memory, workload_ends_at, stop).await;
        let healed = self.heal(stop).await;
        clients.stop().await;
        if is_told_to_stop(stop) {
            return Played::Interrupted(healed.err());
        }
        if let Err(reason) = healed {
            return Played::NotAudited(reason);
        }
        if self.context.plants_lost_record {
            self.plant_lost_record();
        }
        match self.audit().await {
            Ok(audit_sessions) => Played::Audited(audit_sessions),
            Err(reason) => Played::NotAudited(reason),
        }
    }

    /// Creates the streams of the round, trying again until the healing time is over.
    ///
    /// # Errors
    ///
    /// Returns why a stream could not be created in time.
    async fn create_streams(&self) -> Result<(), String> {
        let deadline = Instant::now() + self.settings.heal_time_limit;
        for stream in &self.streams {
            loop {
                let meta_error = match self.create_stream(&stream.name).await {
                    Ok(()) => break,
                    Err(meta_error) => meta_error,
                };
                if Instant::now() >= deadline {
                    return Err(format!("stream {} could not be created: {meta_error}", stream.name));
                }
                sleep(CREATE_RETRY_PAUSE).await;
            }
        }
        Ok(())
    }

    /// Creates one stream with the default replication. A stream that exists already is this round's own: its name
    /// is of this run and this round, and an earlier attempt may have created it without hearing so.
    async fn create_stream(&self, name: &str) -> Result<(), MetaError> {
        let created = self
            .cluster
            .client
            .admin()
            .create_stream(name, StreamOptions::default())
            .await;
        match created {
            Ok(_) | Err(MetaError::StreamAlreadyExists { .. }) => Ok(()),
            Err(meta_error) => Err(meta_error),
        }
    }

    /// Removes every cut and, unless the tool was told to stop, waits until the cluster is healthy again.
    ///
    /// # Errors
    ///
    /// Returns why a cut could not be removed or the cluster did not recover in time.
    async fn heal(&self, stop: &watch::Receiver<bool>) -> Result<(), String> {
        remove_cuts_within(self.cluster, self.settings.heal_time_limit).await?;
        if is_told_to_stop(stop) {
            return Ok(());
        }
        let time_limit = self.settings.heal_time_limit;
        match wait_until_healthy(self.cluster, self.context.expected_replicas, time_limit).await {
            Ok(()) => Ok(()),
            Err(reason) => Err(format!("the cluster did not recover within {time_limit:?}: {reason}")),
        }
    }

    /// Records, in the first stream's history, a commit that never happened: the drill that proves a lost record
    /// ends the run.
    fn plant_lost_record(&self) {
        let Some(stream) = self.streams.first() else {
            return;
        };
        let payload = Bytes::from(format!("{}/drill", stream.name));
        let operation = stream.history.append_invoked(payload);
        let claimed = AppendOutcome::Committed {
            sequence: DRILL_SEQUENCE,
            epoch: DRILL_EPOCH,
        };
        stream.history.append_finished(operation, claimed);
    }

    /// Reads every stream back whole.
    ///
    /// # Errors
    ///
    /// Returns why the first stream that could not be read back in time could not.
    async fn audit(&self) -> Result<Vec<u64>, String> {
        let mut audit_sessions = Vec::with_capacity(self.streams.len());
        for stream in &self.streams {
            let session = audit_stream(&self.cluster.client, stream, self.settings.heal_time_limit).await?;
            audit_sessions.push(session);
        }
        Ok(audit_sessions)
    }

    /// Judges the round, keeps the evidence or removes the streams, and builds the report.
    async fn conclude(self, played: Played, started_at: Timestamp) -> RoundReport {
        let mut histories = Vec::with_capacity(self.streams.len());
        for stream in &self.streams {
            histories.push(stream.history.events());
        }
        let judgement = self.judge(played, &histories);
        let dumps = match judgement.verdict {
            RoundVerdict::Violated | RoundVerdict::NotAudited => self.write_dumps(&histories),
            RoundVerdict::Clean | RoundVerdict::Interrupted => Vec::new(),
        };
        if matches!(judgement.verdict, RoundVerdict::Clean | RoundVerdict::Interrupted) {
            self.delete_streams().await;
        }
        self.report(judgement, dumps, &histories, started_at)
    }

    /// Turns how far the round got into a verdict, judging every history if the round was audited.
    fn judge(&self, played: Played, histories: &[Vec<TimedEvent>]) -> Judgement {
        let audit_sessions = match played {
            Played::Audited(audit_sessions) => audit_sessions,
            Played::NotAudited(reason) => return Judgement::without_audit(RoundVerdict::NotAudited, Some(reason)),
            Played::Interrupted(reason) => return Judgement::without_audit(RoundVerdict::Interrupted, reason),
        };
        let mut violations = Vec::new();
        for (position, events) in histories.iter().enumerate() {
            for violation in violations_of(events, audit_sessions[position]) {
                violations.push(format!("{}: {violation:?}", self.streams[position].name));
            }
        }
        let verdict = if violations.is_empty() {
            RoundVerdict::Clean
        } else {
            RoundVerdict::Violated
        };
        Judgement {
            verdict,
            violations,
            reason: None,
        }
    }

    /// Writes the history of every stream to a file of its own and returns the paths of those that were written.
    fn write_dumps(&self, histories: &[Vec<TimedEvent>]) -> Vec<String> {
        let mut dumps = Vec::new();
        for (position, events) in histories.iter().enumerate() {
            let file_name = format!("{}.history", self.streams[position].name);
            match write_dump(&self.settings.dump_directory, &file_name, events) {
                Ok(path) => dumps.push(path.display().to_string()),
                Err(io_error) => eprintln!("bita-soak: the history {file_name} could not be written: {io_error}"),
            }
        }
        dumps
    }

    /// Deletes the streams of the round, so that the next round finds the disks as this one did.
    async fn delete_streams(&self) {
        for stream in &self.streams {
            match self.cluster.client.admin().delete_stream(&stream.name).await {
                Ok(()) | Err(MetaError::StreamNotFound { .. }) => {}
                Err(meta_error) => eprintln!("bita-soak: stream {} was not deleted: {meta_error}", stream.name),
            }
        }
    }

    /// Builds the report of the round from its verdict and from what its histories hold.
    fn report(
        &self,
        judgement: Judgement,
        dumps: Vec<String>,
        histories: &[Vec<TimedEvent>],
        started_at: Timestamp,
    ) -> RoundReport {
        let counts = counts_of(histories);
        RoundReport {
            kind: "round",
            run: self.context.run.to_owned(),
            round: self.context.number,
            started_at_unix_millis: started_at.unix_millis(),
            ended_at_unix_millis: self.cluster.clock.now().unix_millis(),
            streams: self.settings.stream_count,
            appends_committed: counts.appends_committed,
            appends_unknown: counts.appends_unknown,
            appends_refused: counts.appends_refused,
            records_read: counts.records_read,
            faults: faults_by_kind(&self.tally),
            faults_not_injected: self.tally.not_injected,
            faults_disabled: self.faults_disabled.clone(),
            takeovers: self.takeovers(),
            worst_append_latency_milliseconds: whole_milliseconds(counts.worst_append_latency),
            events: counts.events,
            verdict: judgement.verdict,
            violations: judgement.violations,
            reason: judgement.reason,
            dumps,
            drill: self.context.plants_lost_record,
        }
    }

    /// How many times a writer had to open its stream again: every opening but the first of each stream.
    fn takeovers(&self) -> u64 {
        self.counters
            .writer_openings()
            .saturating_sub(self.settings.stream_count)
    }
}

/// Adds up what the histories of a round hold.
fn counts_of(histories: &[Vec<TimedEvent>]) -> HistoryCounts {
    let mut counts = HistoryCounts::default();
    for events in histories {
        counts.add(&HistoryCounts::of(events));
    }
    counts
}

/// How many faults of each kind the nemesis injected, with a zero for the kinds it did not.
fn faults_by_kind(tally: &NemesisTally) -> BTreeMap<&'static str, u64> {
    let mut faults = BTreeMap::new();
    for kind in FaultKind::ALL {
        let injected = tally.injected.get(&kind).copied().unwrap_or(0);
        faults.insert(kind.report_name(), injected);
    }
    faults
}

/// The whole milliseconds of `duration`, for a report.
fn whole_milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The writers and readers of a round.
struct Clients {
    /// One writer per stream. They end by themselves once told to stop.
    writers: JoinSet<()>,
    /// One reader per stream. They run until they are aborted.
    readers: JoinSet<()>,
    /// Tells the writers to stop.
    stop_writers: watch::Sender<bool>,
}

impl Clients {
    /// Starts a writer and a reader for every stream.
    fn start(
        cluster: &SoakedCluster,
        streams: &[SoakedStream],
        workload: WorkloadSettings,
        counters: &Arc<WorkloadCounters>,
    ) -> Self {
        let (stop_writers, writers_stopped) = watch::channel(false);
        let mut writers = JoinSet::new();
        let mut readers = JoinSet::new();
        for stream in streams {
            writers.spawn(run_writer(
                cluster.client.clone(),
                stream.clone(),
                workload,
                Arc::clone(counters),
                writers_stopped.clone(),
            ));
            readers.spawn(run_reader(cluster.client.clone(), stream.clone()));
        }
        Self {
            writers,
            readers,
            stop_writers,
        }
    }

    /// Tells the writers to stop and waits for them, for a bounded time; then ends the readers.
    ///
    /// A writer that does not stop in time is ended in the middle of its append, which the history then holds as
    /// an append that never returned: of unknown outcome, as it is.
    async fn stop(mut self) {
        let _writers_were_listening = self.stop_writers.send(true);
        let every_writer_ended = timeout(WRITER_STOP_TIME_LIMIT, async {
            while self.writers.join_next().await.is_some() {}
        })
        .await;
        if every_writer_ended.is_err() {
            self.writers.shutdown().await;
        }
        self.readers.shutdown().await;
    }
}

#[cfg(test)]
#[path = "round_test.rs"]
mod tests;
