//! Tests of one round, played against the in-process cluster.
//!
//! # What is covered
//!
//! - A round without faults: it is clean, it wrote and read records, and it deleted its streams.
//! - A round with each kind of fault, and one with all of them: clean, with the faults counted.
//! - A planted violation: the round is not clean, says what was found, writes its histories and keeps its streams.
//! - A cluster that does not recover: the round is not audited, for a stated reason, and is never reported as
//!   clean. That covers a cut that cannot be removed, a replica that stays down, streams that cannot be created
//!   and streams that cannot be read back.
//! - Network cuts: none is in place when a round ends, also when the round was interrupted.
//! - Cuts that the cluster accepts and the network ignores: the round reports no cut as injected and the kind as
//!   given up, with the reason.
//!
//! # How the fixtures work
//!
//! `Stage` is the in-process cluster of `test_support.rs` with a controller leading, a temporary directory for
//! dumps, and a stop that fires only if the test fires it. `Stage::play` plays the first round of the run
//! [`RUN`] with the settings of `quick_round`: a virtual minute, two streams at five appends a second, a fault
//! every ten seconds, cuts of five. The seed is fixed.

use tempfile::TempDir;

use super::*;
use crate::health::alive_replicas;
use crate::test_support::{InProcessCluster, quick_round};

/// The name of the run the rounds belong to.
const RUN: &str = "soak-test";

/// The seed of every round in these tests.
const SEED: u64 = 7;

/// What a round needs, around the in-process cluster.
struct Stage {
    /// The cluster.
    cluster: InProcessCluster,
    /// The replicas the cluster started with.
    expected_replicas: BTreeSet<ReplicaId>,
    /// Where dumps go.
    dump_directory: TempDir,
    /// What the nemesis carries from round to round.
    memory: NemesisMemory,
    /// Tells the round to stop.
    told_to_stop: watch::Sender<bool>,
    /// What the round listens to.
    stop: watch::Receiver<bool>,
}

impl Stage {
    /// The cluster, with a controller leading.
    async fn start() -> Self {
        let cluster = InProcessCluster::start().await;
        let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
        sleep(Duration::from_secs(2)).await;
        let (told_to_stop, stop) = watch::channel(false);
        Self {
            cluster,
            expected_replicas,
            dump_directory: tempfile::tempdir().expect("a temporary directory"),
            memory: NemesisMemory::from_seed(SEED),
            told_to_stop,
            stop,
        }
    }

    /// The settings of a quick round with faults of `kinds`.
    fn settings(&self, kinds: Vec<FaultKind>) -> RoundSettings {
        quick_round(kinds, self.dump_directory.path())
    }

    /// Plays the first round of the run with `settings`.
    async fn play_with(&mut self, settings: &RoundSettings, plants_lost_record: bool) -> RoundReport {
        let context = RoundContext {
            run: RUN,
            number: 1,
            expected_replicas: &self.expected_replicas,
            plants_lost_record,
        };
        run_round(
            &self.cluster.soaked,
            settings,
            context,
            &mut self.memory,
            &mut self.stop,
        )
        .await
    }

    /// Plays the first round of the run with faults of `kinds`.
    async fn play(&mut self, kinds: Vec<FaultKind>) -> RoundReport {
        let settings = self.settings(kinds);
        self.play_with(&settings, false).await
    }

    /// The names of the streams the cluster holds.
    async fn streams_left(&self) -> Vec<String> {
        let streams = self
            .cluster
            .soaked
            .client
            .admin()
            .list_streams()
            .await
            .expect("the metadata store answers");
        let mut names = Vec::new();
        for stream in streams {
            names.push(stream.name);
        }
        names
    }
}

#[tokio::test(start_paused = true)]
async fn round_without_faults_is_clean() {
    let mut stage = Stage::start().await;

    let report = stage.play(Vec::new()).await;

    assert_eq!(report.verdict, RoundVerdict::Clean, "{report:?}");
}

#[tokio::test(start_paused = true)]
async fn round_writes_at_its_pace_and_reads_everything_twice() {
    let mut stage = Stage::start().await;

    let report = stage.play(Vec::new()).await;

    // Two streams, five appends a second, for a minute and the moment it takes to stop; plus a final record each.
    assert!((580..=640).contains(&report.appends_committed), "{report:?}");
    // The reader follows the stream, and the audit reads all of it once more.
    assert!(report.records_read >= report.appends_committed * 2 - 20, "{report:?}");
}

#[tokio::test(start_paused = true)]
async fn clean_round_deletes_its_streams() {
    let mut stage = Stage::start().await;

    stage.play(Vec::new()).await;

    assert_eq!(stage.streams_left().await, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn clean_round_writes_no_dump() {
    let mut stage = Stage::start().await;

    let report = stage.play(Vec::new()).await;

    assert_eq!(report.dumps, Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn round_is_named_after_its_run_and_its_number() {
    let mut stage = Stage::start().await;

    let report = stage.play(Vec::new()).await;

    assert_eq!((report.run.as_str(), report.round, report.streams), (RUN, 1, 2));
}

/// Plays a round with faults of one kind and requires it to be clean with at least one such fault injected.
async fn round_with_one_kind_of_fault_is_clean(kind: FaultKind) {
    let mut stage = Stage::start().await;

    let report = stage.play(vec![kind]).await;

    assert_eq!(report.verdict, RoundVerdict::Clean, "{report:?}");
    assert!(report.faults[kind.report_name()] >= 1, "{report:?}");
}

#[tokio::test(start_paused = true)]
async fn round_with_killed_replicas_is_clean() {
    round_with_one_kind_of_fault_is_clean(FaultKind::KillReplica).await;
}

#[tokio::test(start_paused = true)]
async fn round_with_killed_controllers_is_clean() {
    round_with_one_kind_of_fault_is_clean(FaultKind::KillController).await;
}

#[tokio::test(start_paused = true)]
async fn round_with_killed_etcd_members_is_clean() {
    round_with_one_kind_of_fault_is_clean(FaultKind::KillEtcdMember).await;
}

#[tokio::test(start_paused = true)]
async fn round_with_cut_replicas_is_clean() {
    round_with_one_kind_of_fault_is_clean(FaultKind::CutReplica).await;
}

#[tokio::test(start_paused = true)]
async fn round_with_cut_zones_is_clean() {
    round_with_one_kind_of_fault_is_clean(FaultKind::CutZone).await;
}

#[tokio::test(start_paused = true)]
async fn round_with_every_kind_of_fault_is_clean() {
    let mut stage = Stage::start().await;

    let report = stage.play(FaultKind::ALL.to_vec()).await;

    assert_eq!(report.verdict, RoundVerdict::Clean, "{report:?}");
}

#[tokio::test(start_paused = true)]
async fn report_counts_every_kind_of_fault_also_those_not_injected() {
    let mut stage = Stage::start().await;

    let report = stage.play(vec![FaultKind::KillController]).await;

    let mut kinds = Vec::new();
    for kind in report.faults.keys() {
        kinds.push(*kind);
    }
    assert_eq!(
        kinds,
        [
            "cut_replica",
            "cut_zone",
            "kill_controller",
            "kill_etcd_member",
            "kill_replica"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn no_cut_is_in_place_when_a_round_ends() {
    let mut stage = Stage::start().await;

    stage.play(vec![FaultKind::CutReplica, FaultKind::CutZone]).await;

    assert_eq!(stage.cluster.faults.cuts_in_place(), 0);
}

/// The drill: a commit is claimed that never happened. If this round were clean, no real loss would be noticed.
#[tokio::test(start_paused = true)]
async fn round_with_a_planted_lost_record_is_violated() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());

    let report = stage.play_with(&settings, true).await;

    assert_eq!(report.verdict, RoundVerdict::Violated, "{report:?}");
    assert!(report.drill);
}

#[tokio::test(start_paused = true)]
async fn violated_round_says_what_was_found_and_in_which_stream() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());

    let report = stage.play_with(&settings, true).await;

    assert_eq!(report.violations.len(), 1, "{report:?}");
    assert!(
        report.violations[0].starts_with("soak-test-1-0: CommittedRecordMissing"),
        "{report:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn violated_round_keeps_its_streams() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());

    stage.play_with(&settings, true).await;

    assert_eq!(stage.streams_left().await, ["soak-test-1-0", "soak-test-1-1"]);
}

#[tokio::test(start_paused = true)]
async fn violated_round_writes_the_history_of_every_stream() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());

    let report = stage.play_with(&settings, true).await;

    let expected_dumps = [
        stage
            .dump_directory
            .path()
            .join("soak-test-1-0.history")
            .display()
            .to_string(),
        stage
            .dump_directory
            .path()
            .join("soak-test-1-1.history")
            .display()
            .to_string(),
    ];
    assert_eq!(report.dumps, expected_dumps);
    assert!(stage.dump_directory.path().join("soak-test-1-0.history").exists());
}

#[tokio::test(start_paused = true)]
async fn round_whose_cut_cannot_be_removed_is_not_audited() {
    let mut stage = Stage::start().await;
    stage.cluster.faults.refuse_to_remove_cuts(true);

    let report = stage.play(vec![FaultKind::CutReplica]).await;

    assert_eq!(report.verdict, RoundVerdict::NotAudited, "{report:?}");
    let reason = report.reason.expect("a round that was not audited says why");
    assert!(reason.starts_with("a network cut could not be removed"), "{reason}");
}

#[tokio::test(start_paused = true)]
async fn round_after_which_a_replica_stays_down_is_not_audited() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());
    let faults = Arc::clone(&stage.cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_secs(30)).await;
        faults.crash_for(0, Duration::from_secs(36_000));
    });

    let report = stage.play_with(&settings, false).await;

    assert_eq!(report.verdict, RoundVerdict::NotAudited, "{report:?}");
    let reason = report.reason.expect("a round that was not audited says why");
    assert!(
        reason.starts_with("the cluster did not recover within 120s: replica replica-0"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn round_that_was_not_audited_keeps_its_streams_and_writes_its_histories() {
    let mut stage = Stage::start().await;
    stage.cluster.faults.refuse_to_remove_cuts(true);

    let report = stage.play(vec![FaultKind::CutReplica]).await;

    assert_eq!(report.dumps.len(), 2, "{report:?}");
    assert_eq!(stage.streams_left().await, ["soak-test-1-0", "soak-test-1-1"]);
}

#[tokio::test(start_paused = true)]
async fn round_that_cannot_create_its_streams_is_not_audited() {
    let mut stage = Stage::start().await;
    stage.cluster.faults.set_metadata_store_unavailable(true);

    let report = stage.play(Vec::new()).await;

    assert_eq!(report.verdict, RoundVerdict::NotAudited, "{report:?}");
    let reason = report.reason.expect("a round that was not audited says why");
    assert!(
        reason.starts_with("stream soak-test-1-0 could not be created"),
        "{reason}"
    );
}

/// Every replica dies half a second before the round's time is up. Their registrations are still fresh, so the
/// cluster passes for healthy; but no stream can be read back. Whatever the round saw until then, it has not been
/// audited, so it is not clean.
#[tokio::test(start_paused = true)]
async fn round_whose_streams_cannot_be_read_back_is_never_clean() {
    let mut stage = Stage::start().await;
    let settings = stage.settings(Vec::new());
    let faults = Arc::clone(&stage.cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_millis(59_500)).await;
        for node_number in 0..6 {
            faults.crash_for(node_number, Duration::from_secs(36_000));
        }
    });

    let report = stage.play_with(&settings, false).await;

    assert_eq!(report.verdict, RoundVerdict::NotAudited, "{report:?}");
    let reason = report.reason.expect("a round that was not audited says why");
    assert!(
        reason.starts_with("no final record could be committed to soak-test-1-0"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn interrupted_round_is_reported_as_interrupted() {
    let mut stage = Stage::start().await;
    let told_to_stop = stage.told_to_stop.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(25)).await;
        told_to_stop.send(true).expect("the round listens");
    });

    let report = stage.play(Vec::new()).await;

    assert_eq!(report.verdict, RoundVerdict::Interrupted, "{report:?}");
}

/// A cut that is in place when the tool is told to stop must not outlive the tool: nobody else would remove it.
#[tokio::test(start_paused = true)]
async fn interrupted_round_removes_the_cut_that_was_in_place() {
    let mut stage = Stage::start().await;
    let mut settings = stage.settings(vec![FaultKind::CutZone]);
    settings.nemesis.cut_duration = Duration::from_secs(40);
    let told_to_stop = stage.told_to_stop.clone();
    let faults = Arc::clone(&stage.cluster.faults);
    let cuts_when_told_to_stop = tokio::spawn(async move {
        sleep(Duration::from_secs(25)).await;
        let cuts_in_place = faults.cuts_in_place();
        told_to_stop.send(true).expect("the round listens");
        cuts_in_place
    });

    stage.play_with(&settings, false).await;

    assert_eq!(cuts_when_told_to_stop.await.expect("the task ends"), 2);
    assert_eq!(stage.cluster.faults.cuts_in_place(), 0);
}

#[tokio::test(start_paused = true)]
async fn interrupted_round_deletes_its_streams() {
    let mut stage = Stage::start().await;
    let told_to_stop = stage.told_to_stop.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(25)).await;
        told_to_stop.send(true).expect("the round listens");
    });

    stage.play(Vec::new()).await;

    assert_eq!(stage.streams_left().await, Vec::<String>::new());
}

/// A replica is down for the whole round: the nemesis adds nothing to that, and the report says how often it held
/// back.
#[tokio::test(start_paused = true)]
async fn faults_that_were_due_while_the_cluster_was_not_healthy_are_counted_as_not_injected() {
    let mut stage = Stage::start().await;
    stage.cluster.faults.crash_for(1, Duration::from_secs(36_000));
    sleep(Duration::from_secs(20)).await;

    let report = stage.play(vec![FaultKind::KillController]).await;

    assert_eq!(report.faults["kill_controller"], 0, "{report:?}");
    assert_eq!(report.faults_not_injected, 5, "{report:?}");
}

/// Nothing was read back, so nothing may be called clean.
#[tokio::test(start_paused = true)]
async fn round_without_streams_is_not_audited() {
    let mut stage = Stage::start().await;
    let mut settings = stage.settings(Vec::new());
    settings.stream_count = 0;

    let report = stage.play_with(&settings, false).await;

    assert_eq!(report.verdict, RoundVerdict::NotAudited, "{report:?}");
}

/// The tool is on its way out and could not take its cut with it: the last line it prints says so.
#[tokio::test(start_paused = true)]
async fn interrupted_round_that_could_not_remove_its_cut_says_so() {
    let mut stage = Stage::start().await;
    let mut settings = stage.settings(vec![FaultKind::CutZone]);
    settings.nemesis.cut_duration = Duration::from_secs(40);
    settings.heal_time_limit = Duration::from_secs(10);
    let told_to_stop = stage.told_to_stop.clone();
    let faults = Arc::clone(&stage.cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_secs(25)).await;
        faults.refuse_to_remove_cuts(true);
        told_to_stop.send(true).expect("the round listens");
    });

    let report = stage.play_with(&settings, false).await;

    assert_eq!(report.verdict, RoundVerdict::Interrupted, "{report:?}");
    let reason = report.reason.expect("a cut that is left in place is reported");
    assert!(reason.starts_with("a network cut could not be removed"), "{reason}");
}

/// The round is clean, and its line must say that it survived no cut: none happened.
#[tokio::test(start_paused = true)]
async fn round_whose_cuts_do_nothing_reports_no_cut_and_the_kind_as_given_up() {
    let mut stage = Stage::start().await;
    stage.cluster.faults.let_cuts_do_nothing(true);

    let report = stage.play(vec![FaultKind::CutZone]).await;

    assert_eq!(report.faults["cut_zone"], 0, "{report:?}");
    let reason = report.faults_disabled.get("cut_zone").expect("the kind was given up");
    assert!(
        reason.starts_with("network cuts are not enforced on this cluster: zone zone-"),
        "{reason}"
    );
}

#[tokio::test(start_paused = true)]
async fn round_whose_faults_all_happened_reports_no_kind_as_given_up() {
    let mut stage = Stage::start().await;

    let report = stage.play(vec![FaultKind::CutZone]).await;

    assert_eq!(report.faults_disabled, BTreeMap::new(), "{report:?}");
}
