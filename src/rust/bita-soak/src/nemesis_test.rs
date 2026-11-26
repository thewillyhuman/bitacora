//! Tests of the nemesis.
//!
//! # What is covered
//!
//! - The wait: it lasts its duration, ends at the deadline, and ends when the tool is told to stop.
//! - Removing the cuts: at once when it works, again and again while it is refused, and with the reason when the
//!   time runs out.
//! - The loop against the in-process cluster: without kinds nothing is injected; a kind is injected once per
//!   interval; a cut is removed when its time is up; nothing is injected while the cluster is not healthy; every
//!   fault is recorded in the history of every stream; a cut that could not be removed is left to the caller.
//! - A cut that the cluster accepts and the network ignores: it is not counted, recorded in no history, removed,
//!   and its kind is given up for the rest of the run, with the reason kept; other kinds go on; the settings can
//!   ask for the kind to be tried again instead.
//!
//! # How the fixtures work
//!
//! `Arena` is the in-process cluster of `test_support.rs` with a controller leading, two streams' histories, and a
//! stop that never fires unless the test fires it. `Arena::run` lets a nemesis run for a virtual minute with a
//! fault every ten seconds and cuts of five. A cut is checked for six seconds first, so three cuts fit into the
//! minute. The seed is fixed, so the same targets are hit every time.

use std::sync::Arc;

use bitacora_sim::Event;

use super::*;
use crate::health::alive_replicas;
use crate::test_support::{InProcessCluster, QUICK_CUT_CHECK};

/// The seed of every choice in these tests.
const SEED: u64 = 7;

/// How long the nemesis runs in these tests.
const MINUTE: Duration = Duration::from_secs(60);

/// What a nemesis needs, around the in-process cluster.
struct Arena {
    /// The cluster.
    cluster: InProcessCluster,
    /// The replicas the cluster started with.
    expected_replicas: BTreeSet<ReplicaId>,
    /// Two streams, of which only the histories are used.
    streams: Vec<SoakedStream>,
    /// What the nemesis remembers from one run of its loop to the next.
    memory: NemesisMemory,
    /// Tells the nemesis to stop.
    told_to_stop: watch::Sender<bool>,
    /// What the nemesis listens to.
    stop: watch::Receiver<bool>,
}

impl Arena {
    /// The cluster, with a controller leading.
    async fn start() -> Self {
        let cluster = InProcessCluster::start().await;
        let expected_replicas = alive_replicas(&cluster.soaked).await.expect("the registry answers");
        sleep(Duration::from_secs(2)).await;
        let (told_to_stop, stop) = watch::channel(false);
        Self {
            cluster,
            expected_replicas,
            streams: vec![
                SoakedStream::new("soak-test-1-0".to_owned()),
                SoakedStream::new("soak-test-1-1".to_owned()),
            ],
            memory: NemesisMemory::from_seed(SEED),
            told_to_stop,
            stop,
        }
    }

    /// Runs a nemesis with `settings` for a minute.
    async fn run_with(&mut self, settings: &NemesisSettings) -> NemesisTally {
        let nemesis = Nemesis::new(&self.cluster.soaked, settings, &self.expected_replicas, &self.streams);
        nemesis
            .run(&mut self.memory, Instant::now() + MINUTE, &mut self.stop)
            .await
    }

    /// Runs a nemesis of `kinds` for a minute: a fault every ten seconds, cuts of five once they are in effect.
    async fn run(&mut self, kinds: Vec<FaultKind>) -> NemesisTally {
        self.run_with(&settings_with(kinds)).await
    }

    /// The descriptions of the faults recorded in the history of the stream at `position`.
    fn recorded_faults(&self, position: usize) -> Vec<String> {
        let mut descriptions = Vec::new();
        for timed in self.streams[position].history.events() {
            if let Event::Fault { description } = timed.event {
                descriptions.push(description);
            }
        }
        descriptions
    }
}

/// A fault of `kinds` every ten seconds, cuts of five once they are in effect, checked with [`QUICK_CUT_CHECK`].
fn settings_with(kinds: Vec<FaultKind>) -> NemesisSettings {
    NemesisSettings {
        kinds,
        fault_interval: Duration::from_secs(10),
        cut_duration: Duration::from_secs(5),
        cut_check: QUICK_CUT_CHECK,
        retries_cuts_not_in_effect: false,
    }
}

/// A stop that nobody fires, and the sender that keeps it that way.
fn no_stop() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

#[tokio::test(start_paused = true)]
async fn pause_lasts_its_whole_duration_when_nothing_ends_it() {
    let (_told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();

    let whole_duration_passed = pause(Duration::from_secs(10), started_at + MINUTE, &mut stop).await;

    assert!(whole_duration_passed);
    assert_eq!(started_at.elapsed(), Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn pause_ends_at_the_deadline() {
    let (_told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();

    let whole_duration_passed = pause(MINUTE, started_at + Duration::from_secs(10), &mut stop).await;

    assert!(!whole_duration_passed);
    assert_eq!(started_at.elapsed(), Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn pause_ends_when_the_tool_is_told_to_stop() {
    let (told_to_stop, mut stop) = no_stop();
    let started_at = Instant::now();
    tokio::spawn(async move {
        sleep(Duration::from_secs(3)).await;
        told_to_stop.send(true).expect("the pause listens");
    });

    let whole_duration_passed = pause(MINUTE, started_at + MINUTE, &mut stop).await;

    assert!(!whole_duration_passed);
    assert_eq!(started_at.elapsed(), Duration::from_secs(3));
}

/// Whoever would tell the tool to stop is gone: nothing will ever end the wait in an orderly way, so it ends now.
#[tokio::test(start_paused = true)]
async fn pause_ends_when_nobody_is_left_to_say_stop() {
    let (told_to_stop, mut stop) = no_stop();
    drop(told_to_stop);

    let whole_duration_passed = pause(MINUTE, Instant::now() + MINUTE, &mut stop).await;

    assert!(!whole_duration_passed);
}

#[tokio::test(start_paused = true)]
async fn cuts_are_removed_at_once_when_nothing_refuses() {
    let arena = Arena::start().await;
    let cut = Fault::CutZone {
        zone: "zone-1".to_owned(),
    };
    arena.cluster.soaked.faults.inject(&cut).await.expect("the cut is made");

    let removed = remove_cuts_within(&arena.cluster.soaked, MINUTE).await;

    assert_eq!(removed, Ok(()));
    assert_eq!(arena.cluster.faults.cuts_in_place(), 0);
}

#[tokio::test(start_paused = true)]
async fn removal_is_tried_again_until_it_works() {
    let arena = Arena::start().await;
    arena.cluster.faults.refuse_to_remove_cuts(true);
    let faults = Arc::clone(&arena.cluster.faults);
    tokio::spawn(async move {
        sleep(Duration::from_secs(9)).await;
        faults.refuse_to_remove_cuts(false);
    });

    let removed = remove_cuts_within(&arena.cluster.soaked, MINUTE).await;

    assert_eq!(removed, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn removal_that_keeps_being_refused_gives_the_reason_when_the_time_is_up() {
    let arena = Arena::start().await;
    arena.cluster.faults.refuse_to_remove_cuts(true);

    let removed = remove_cuts_within(&arena.cluster.soaked, Duration::from_secs(10)).await;

    assert_eq!(
        removed,
        Err("a network cut could not be removed: the test cluster was told to refuse".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn nemesis_without_kinds_injects_nothing() {
    let mut arena = Arena::start().await;

    let tally = arena.run(Vec::new()).await;

    assert_eq!(tally, NemesisTally::default());
    assert_eq!(arena.cluster.faults.injected(), []);
}

#[tokio::test(start_paused = true)]
async fn nemesis_runs_until_its_deadline() {
    let mut arena = Arena::start().await;
    let started_at = Instant::now();

    arena.run(Vec::new()).await;

    assert_eq!(started_at.elapsed(), MINUTE);
}

/// Five intervals of ten seconds fit before the deadline of a minute; the sixth ends at it.
#[tokio::test(start_paused = true)]
async fn nemesis_injects_one_fault_per_interval() {
    let mut arena = Arena::start().await;

    let tally = arena.run(vec![FaultKind::KillController]).await;

    assert_eq!(tally.injected, BTreeMap::from([(FaultKind::KillController, 5)]));
    assert_eq!(tally.not_injected, 0);
}

#[tokio::test(start_paused = true)]
async fn nemesis_injects_only_the_kinds_it_was_given() {
    let mut arena = Arena::start().await;

    arena.run(vec![FaultKind::CutReplica]).await;

    let mut kinds = BTreeSet::new();
    for fault in arena.cluster.faults.injected() {
        kinds.insert(fault.kind());
    }
    assert_eq!(kinds, BTreeSet::from([FaultKind::CutReplica]));
}

#[tokio::test(start_paused = true)]
async fn cut_is_removed_when_its_time_is_up() {
    let mut arena = Arena::start().await;

    arena.run(vec![FaultKind::CutZone]).await;

    assert_eq!(arena.cluster.faults.cuts_in_place(), 0);
}

#[tokio::test(start_paused = true)]
async fn fault_and_its_end_are_recorded_in_the_history_of_every_stream() {
    let mut arena = Arena::start().await;

    arena.run(vec![FaultKind::CutZone]).await;

    let recorded = arena.recorded_faults(0);
    assert_eq!(recorded.len(), 2 * arena.cluster.faults.injected().len());
    assert!(recorded[0].ends_with("is cut off the network (6s ago)"), "{recorded:?}");
    assert!(recorded[1].ends_with("is cut off the network: healed"), "{recorded:?}");
    assert_eq!(arena.recorded_faults(1), recorded);
}

/// A replica that stays down is a fault that is still being recovered from: the nemesis does not add another.
#[tokio::test(start_paused = true)]
async fn nothing_is_injected_while_the_cluster_is_not_healthy() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.crash_for(0, Duration::from_secs(3_600));
    sleep(Duration::from_secs(20)).await;

    let tally = arena.run(vec![FaultKind::KillController]).await;

    assert_eq!(tally.injected, BTreeMap::new());
    assert_eq!(tally.not_injected, 5);
}

#[tokio::test(start_paused = true)]
async fn nemesis_ends_when_the_tool_is_told_to_stop() {
    let mut arena = Arena::start().await;
    let started_at = Instant::now();
    arena.told_to_stop.send(true).expect("the nemesis listens");

    arena.run(vec![FaultKind::KillController]).await;

    assert_eq!(started_at.elapsed(), Duration::ZERO);
}

/// The nemesis gives up on a cut after a while; whoever runs it heals once more and knows whether that worked.
#[tokio::test(start_paused = true)]
async fn cut_that_cannot_be_removed_is_left_in_place_for_the_caller() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.refuse_to_remove_cuts(true);

    let tally = arena.run(vec![FaultKind::CutReplica]).await;

    assert_eq!(tally.injected, BTreeMap::from([(FaultKind::CutReplica, 1)]));
    assert_eq!(arena.cluster.faults.cuts_in_place(), 1);
}

/// What the cluster's management accepts and the network ignores did not happen.
#[tokio::test(start_paused = true)]
async fn cut_that_does_nothing_is_not_counted() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);

    let tally = arena.run(vec![FaultKind::CutReplica]).await;

    assert_eq!(tally.injected, BTreeMap::new());
}

#[tokio::test(start_paused = true)]
async fn cut_that_does_nothing_is_recorded_in_no_history() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);

    arena.run(vec![FaultKind::CutZone]).await;

    assert_eq!(arena.recorded_faults(0), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn cut_that_does_nothing_is_removed() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);

    arena.run(vec![FaultKind::CutZone]).await;

    assert_eq!(arena.cluster.faults.cuts_in_place(), 0);
}

/// One failed check is the evidence; asking again every ten seconds for a week would add nothing to it.
#[tokio::test(start_paused = true)]
async fn kind_whose_cut_does_nothing_is_given_up_after_one_attempt() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);

    let tally = arena.run(vec![FaultKind::CutReplica]).await;

    assert_eq!(arena.cluster.faults.injected().len(), 1);
    assert_eq!(tally.not_injected, 3);
}

#[tokio::test(start_paused = true)]
async fn kind_that_was_given_up_is_remembered_with_the_reason() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);

    arena.run(vec![FaultKind::CutReplica]).await;

    let reason = arena
        .memory
        .disabled
        .get(&FaultKind::CutReplica)
        .expect("the kind was given up");
    assert!(
        reason.starts_with("network cuts are not enforced on this cluster: replica replica-"),
        "{reason}"
    );
}

/// What the memory holds, the next round's nemesis does not try: that is how a kind stays given up for a run.
#[tokio::test(start_paused = true)]
async fn kind_that_was_given_up_is_not_tried_by_the_next_nemesis() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);
    arena.run(vec![FaultKind::CutReplica]).await;

    let tally = arena.run(vec![FaultKind::CutReplica]).await;

    assert_eq!(arena.cluster.faults.injected().len(), 1);
    assert_eq!(tally.not_injected, 5);
}

#[tokio::test(start_paused = true)]
async fn other_kinds_go_on_after_one_was_given_up() {
    let mut arena = Arena::start().await;
    arena
        .memory
        .disabled
        .insert(FaultKind::CutZone, "given up by an earlier round".to_owned());

    let tally = arena.run(vec![FaultKind::CutZone, FaultKind::KillController]).await;

    assert_eq!(tally.injected, BTreeMap::from([(FaultKind::KillController, 5)]));
}

#[tokio::test(start_paused = true)]
async fn kind_is_tried_again_when_the_settings_say_so() {
    let mut arena = Arena::start().await;
    arena.cluster.faults.let_cuts_do_nothing(true);
    let mut settings = settings_with(vec![FaultKind::CutReplica]);
    settings.retries_cuts_not_in_effect = true;

    let tally = arena.run_with(&settings).await;

    assert_eq!(arena.cluster.faults.injected().len(), 2);
    assert_eq!(tally.injected, BTreeMap::new());
    assert_eq!(arena.memory.disabled, BTreeMap::new());
}

/// A fault that the cluster's management refused did not happen either.
#[tokio::test(start_paused = true)]
async fn fault_that_the_cluster_refuses_is_not_counted_and_not_recorded() {
    let mut arena = Arena::start().await;
    let cluster_without_faults = SoakedCluster {
        faults: Arc::new(crate::faults::NoFaults),
        ..arena.cluster.soaked.clone()
    };
    let settings = settings_with(vec![FaultKind::KillController]);
    let nemesis = Nemesis::new(
        &cluster_without_faults,
        &settings,
        &arena.expected_replicas,
        &arena.streams,
    );

    let tally = nemesis
        .run(&mut arena.memory, Instant::now() + MINUTE, &mut arena.stop)
        .await;

    assert_eq!(tally.injected, BTreeMap::new());
    assert_eq!(tally.not_injected, 5);
    assert_eq!(arena.recorded_faults(0), Vec::<String>::new());
}
