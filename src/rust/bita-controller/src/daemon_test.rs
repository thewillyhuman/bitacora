//! Tests of the controller daemon, inside the test process.
//!
//! # What is covered
//!
//! - The loop: rounds run at once and after every pass interval, and each is recorded.
//! - Readiness: after the first round that completes, as leader or as standby, and not before.
//! - Two instances: one leads, the other stands by and takes over when the leader stops.
//! - Shutdown: at once while resting; a running round is finished within the grace period, and
//!   abandoned after it.
//! - The daemon as a whole: its endpoint reports readiness and metrics, and a signal stops it.
//!
//! # How the fixtures work
//!
//! The loop tests run `run_rounds` against the in-process cluster of `bitacora-testkit`: real replicas
//! on in-memory disks behind links that can be slowed down, an in-memory metadata store, and a clock
//! that follows Tokio's paused time. A test that lets a minute pass takes no time.
//!
//! The tests of `run` use real sockets for the endpoint and therefore real time, with a pass interval
//! of 20 milliseconds and every wait bounded by `GENEROUS_WAIT`.

use bitacora_controller::ControllerSettings;
use bitacora_meta::{MemoryKeyValueStore, RetentionPolicy};
use bitacora_process::{LogSettings, http_get};
use bitacora_testkit::{Cluster, LinkFault};
use bitacora_types::ReplicationSettings;
use bitacora_types::replica_api::StaticReplicaConnector;
use tokio::task::JoinHandle;

use super::*;

/// The stream the loop tests let the controller look after.
const STREAM: &str = "orders";

/// Far longer than anything here takes; a test that waits this long has found a hang.
const GENEROUS_WAIT: Duration = Duration::from_secs(30);

/// Long enough, in paused time, for a round against a healthy in-process cluster to finish.
const A_MOMENT: Duration = Duration::from_millis(1);

/// A loop of rounds that runs in the background, with what a test needs to observe and stop it.
struct RunningLoop {
    /// The registry the loop's metrics are in.
    registry: MetricsRegistry,
    /// The readiness flag the loop sets.
    readiness: Readiness,
    /// Sends the shutdown signal.
    shutdown_sender: oneshot::Sender<ShutdownSignal>,
    /// The task that runs the loop, and the signal `run_rounds` returns.
    running: JoinHandle<ShutdownSignal>,
}

impl RunningLoop {
    /// Starts the loop of a controller called `identity` over `cluster`, with the default settings.
    fn start(cluster: &Cluster, identity: &str) -> Self {
        let settings = ControllerSettings {
            identity: identity.to_owned(),
            ..ControllerSettings::default()
        };
        let pass_interval = settings.pass_interval;
        let controller = Controller::new(
            cluster.metadata().clone(),
            cluster.connector(),
            cluster.clock(),
            settings,
        );
        let registry = MetricsRegistry::new();
        let metrics = ControllerMetrics::register(&registry);
        let readiness = Readiness::new();
        let readiness_of_the_loop = readiness.clone();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<ShutdownSignal>();
        let running = tokio::spawn(async move {
            let shutdown = async move {
                match shutdown_receiver.await {
                    Ok(signal) => signal,
                    Err(_sender_dropped) => ShutdownSignal::Terminate,
                }
            };
            run_rounds(&controller, &metrics, &readiness_of_the_loop, pass_interval, shutdown).await
        });
        Self {
            registry,
            readiness,
            shutdown_sender,
            running,
        }
    }

    /// The value of the exposition line that starts with `series`, if there is such a line.
    fn metric(&self, series: &str) -> Option<String> {
        for line in self.registry.encode().lines() {
            if let Some(value) = line.strip_prefix(series) {
                return Some(value.trim().to_owned());
            }
        }
        None
    }

    /// Sends `signal` and waits for the loop to end.
    ///
    /// # Returns
    ///
    /// The signal `run_rounds` returned.
    async fn stop_with(self, signal: ShutdownSignal) -> ShutdownSignal {
        self.shutdown_sender.send(signal).expect("the loop is still running");
        self.running.await.expect("the loop does not panic")
    }
}

/// An in-process cluster of three replicas with [`STREAM`] created on it, still without a standby
/// segment: placing one is the first thing a controller does.
async fn cluster_with_a_new_stream() -> Cluster {
    let cluster = Cluster::start(3).await;
    cluster
        .metadata()
        .create_stream(STREAM, ReplicationSettings::DEFAULT, RetentionPolicy::default(), None)
        .await
        .expect("the stream is created");
    cluster
}

/// Whether [`STREAM`] has a standby segment placed.
async fn has_a_standby(cluster: &Cluster) -> bool {
    cluster
        .metadata()
        .get_stream(STREAM)
        .await
        .expect("the stream exists")
        .standby_replicas
        .is_some()
}

/// Holds every request to every replica of `cluster` for `delay` before it arrives.
fn slow_down_every_link(cluster: &Cluster, delay: Duration) {
    for node in cluster.nodes() {
        node.link().set_fault(LinkFault {
            delay,
            ..LinkFault::default()
        });
    }
}

/// Lets `duration` of paused time pass.
async fn let_pass(duration: Duration) {
    sleep(duration).await;
}

#[tokio::test(start_paused = true)]
async fn first_round_runs_at_once_and_does_the_controllers_work() {
    let cluster = cluster_with_a_new_stream().await;

    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    assert!(has_a_standby(&cluster).await);
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn daemon_is_ready_after_its_first_round() {
    let cluster = cluster_with_a_new_stream().await;

    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    assert!(running_loop.readiness.is_ready());
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn daemon_is_not_ready_while_its_rounds_fail() {
    let cluster = cluster_with_a_new_stream().await;
    cluster.key_value_store().set_unavailable(true);
    let pass_interval = ControllerSettings::default().pass_interval;

    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(pass_interval * 3).await;

    assert!(!running_loop.readiness.is_ready());
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn daemon_becomes_ready_once_a_round_completes() {
    let cluster = cluster_with_a_new_stream().await;
    cluster.key_value_store().set_unavailable(true);
    let pass_interval = ControllerSettings::default().pass_interval;
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(pass_interval * 2).await;

    cluster.key_value_store().set_unavailable(false);
    let_pass(pass_interval * 2).await;

    assert!(running_loop.readiness.is_ready());
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

/// An instance that briefly loses the metadata store is still the one that should take over.
#[tokio::test(start_paused = true)]
async fn daemon_stays_ready_when_a_later_round_fails() {
    let cluster = cluster_with_a_new_stream().await;
    let pass_interval = ControllerSettings::default().pass_interval;
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    cluster.key_value_store().set_unavailable(true);
    let_pass(pass_interval * 2).await;

    assert!(running_loop.readiness.is_ready());
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn rounds_repeat_after_every_pass_interval() {
    let cluster = cluster_with_a_new_stream().await;
    let pass_interval = ControllerSettings::default().pass_interval;

    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(pass_interval * 3 + A_MOMENT).await;

    assert_eq!(
        running_loop.metric("bitacora_controller_rounds_total "),
        Some("4".to_owned())
    );
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn failed_rounds_are_recorded() {
    let cluster = cluster_with_a_new_stream().await;
    cluster.key_value_store().set_unavailable(true);

    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    assert_eq!(
        running_loop.metric("bitacora_controller_round_failures_total "),
        Some("1".to_owned())
    );
    running_loop.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn first_instance_reports_that_it_leads() {
    let cluster = cluster_with_a_new_stream().await;

    let leader = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    assert_eq!(leader.metric("bitacora_controller_leading "), Some("1".to_owned()));
    leader.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn second_instance_stands_by_and_reports_that_it_does_not_lead() {
    let cluster = cluster_with_a_new_stream().await;
    let leader = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    let standby = RunningLoop::start(&cluster, "controller-b");
    let_pass(A_MOMENT).await;

    assert_eq!(standby.metric("bitacora_controller_leading "), Some("0".to_owned()));
    leader.stop_with(ShutdownSignal::Terminate).await;
    standby.stop_with(ShutdownSignal::Terminate).await;
}

/// A standby is ready: it has reached the metadata store and knows where it stands.
#[tokio::test(start_paused = true)]
async fn standby_instance_is_ready() {
    let cluster = cluster_with_a_new_stream().await;
    let leader = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    let standby = RunningLoop::start(&cluster, "controller-b");
    let_pass(A_MOMENT).await;

    assert!(standby.readiness.is_ready());
    leader.stop_with(ShutdownSignal::Terminate).await;
    standby.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn standby_takes_over_once_the_leadership_of_a_stopped_leader_lapses() {
    let cluster = cluster_with_a_new_stream().await;
    let settings = ControllerSettings::default();
    let leader = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;
    let standby = RunningLoop::start(&cluster, "controller-b");
    let_pass(A_MOMENT).await;

    leader.stop_with(ShutdownSignal::Terminate).await;
    let_pass(settings.leadership_duration + settings.pass_interval * 2).await;

    assert_eq!(standby.metric("bitacora_controller_leading "), Some("1".to_owned()));
    standby.stop_with(ShutdownSignal::Terminate).await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_while_resting_stops_the_loop_without_waiting_for_the_interval() {
    let cluster = cluster_with_a_new_stream().await;
    let pass_interval = ControllerSettings::default().pass_interval;
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;
    let asked_at = Instant::now();

    running_loop.stop_with(ShutdownSignal::Terminate).await;

    assert!(
        asked_at.elapsed() < pass_interval,
        "stopping took {:?}",
        asked_at.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn loop_returns_the_signal_that_stopped_it() {
    let cluster = cluster_with_a_new_stream().await;
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(A_MOMENT).await;

    let returned_signal = running_loop.stop_with(ShutdownSignal::Interrupt).await;

    assert_eq!(returned_signal, ShutdownSignal::Interrupt);
}

/// Three replicas that each take two seconds to answer make a pass of about six seconds: longer than
/// the moment before the signal, shorter than the grace period.
#[tokio::test(start_paused = true)]
async fn round_that_is_running_at_shutdown_is_finished_and_recorded() {
    let cluster = cluster_with_a_new_stream().await;
    slow_down_every_link(&cluster, Duration::from_secs(2));
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(Duration::from_secs(1)).await;
    let registry_text_before = running_loop.registry.encode();
    let RunningLoop {
        registry,
        shutdown_sender,
        running,
        ..
    } = running_loop;

    shutdown_sender
        .send(ShutdownSignal::Terminate)
        .expect("the loop is still running");
    running.await.expect("the loop does not panic");

    assert!(
        registry_text_before.contains("bitacora_controller_rounds_total 0\n"),
        "{registry_text_before}"
    );
    assert!(
        registry.encode().contains("bitacora_controller_rounds_total 1\n"),
        "{}",
        registry.encode()
    );
}

/// Replicas that never answer make a pass wait for three request timeouts, fifteen seconds: longer than
/// the grace period. Half a pass is safe, because every write of a pass is conditional.
#[tokio::test(start_paused = true)]
async fn round_that_outlasts_the_grace_period_is_abandoned() {
    let cluster = cluster_with_a_new_stream().await;
    slow_down_every_link(&cluster, Duration::from_hours(1));
    let running_loop = RunningLoop::start(&cluster, "controller-a");
    let_pass(Duration::from_secs(1)).await;
    let asked_at = Instant::now();
    let RunningLoop {
        registry,
        shutdown_sender,
        running,
        ..
    } = running_loop;

    shutdown_sender
        .send(ShutdownSignal::Terminate)
        .expect("the loop is still running");
    running.await.expect("the loop does not panic");

    assert_eq!(asked_at.elapsed(), SHUTDOWN_GRACE);
    assert!(
        registry.encode().contains("bitacora_controller_rounds_total 0\n"),
        "{}",
        registry.encode()
    );
}

/// A whole daemon that runs inside the test, on in-memory stores and a real loopback socket.
struct RunningDaemon {
    /// Where the observability endpoint listens, as `host:port`.
    observability_address: String,
    /// Sends the shutdown signal.
    shutdown_sender: oneshot::Sender<ShutdownSignal>,
    /// The task that runs the daemon.
    running: JoinHandle<()>,
}

impl RunningDaemon {
    /// Starts a daemon over an empty cluster: an in-memory metadata store and no replicas.
    async fn start() -> Self {
        let observability_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port can be bound");
        let observability_address = observability_listener
            .local_addr()
            .expect("a bound socket has an address");
        let settings = ControllerDaemonSettings {
            metrics_listen_address: observability_address,
            cluster: bitacora::ClusterConfig {
                etcd_endpoints: Vec::new(),
                etcd_key_prefix: bitacora::DEFAULT_ETCD_KEY_PREFIX.to_owned(),
                object_store: bitacora::ObjectStoreConfig::None,
            },
            log: LogSettings::default(),
            controller: ControllerSettings {
                identity: "controller-a".to_owned(),
                pass_interval: Duration::from_millis(20),
                ..ControllerSettings::default()
            },
        };
        let environment = Environment {
            key_value_store: Arc::new(MemoryKeyValueStore::new()),
            object_store: None,
            connector: Arc::new(StaticReplicaConnector::new()),
            clock: Arc::new(SystemClock),
        };
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<ShutdownSignal>();
        let shutdown = async move {
            match shutdown_receiver.await {
                Ok(signal) => signal,
                Err(_sender_dropped) => ShutdownSignal::Terminate,
            }
        };
        let running = tokio::spawn(run(settings, environment, observability_listener, shutdown));
        Self {
            observability_address: observability_address.to_string(),
            shutdown_sender,
            running,
        }
    }

    /// Waits until `/readyz` answers with success.
    async fn wait_until_ready(&self) {
        let deadline = Instant::now() + GENEROUS_WAIT;
        loop {
            let answer = http_get(&self.observability_address, "/readyz", GENEROUS_WAIT)
                .await
                .expect("the endpoint answers");
            if answer.status == 200 {
                return;
            }
            assert!(Instant::now() < deadline, "the daemon did not become ready");
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// The daemon's metrics, as Prometheus would scrape them.
    async fn scraped_metrics(&self) -> String {
        http_get(&self.observability_address, "/metrics", GENEROUS_WAIT)
            .await
            .expect("the endpoint answers")
            .body
    }

    /// Sends the terminate signal and waits for the daemon to end.
    async fn terminate(self) {
        self.shutdown_sender
            .send(ShutdownSignal::Terminate)
            .expect("the daemon is still running");
        timeout(GENEROUS_WAIT, self.running)
            .await
            .expect("the daemon stops in time")
            .expect("the daemon does not panic");
    }
}

#[tokio::test]
async fn daemon_reports_ready_on_its_endpoint_once_a_round_completed() {
    let daemon = RunningDaemon::start().await;

    daemon.wait_until_ready().await;

    daemon.terminate().await;
}

#[tokio::test]
async fn daemon_exposes_that_it_leads() {
    let daemon = RunningDaemon::start().await;
    daemon.wait_until_ready().await;

    let metrics = daemon.scraped_metrics().await;

    assert!(metrics.contains("bitacora_controller_leading 1\n"), "{metrics}");
    daemon.terminate().await;
}

#[tokio::test]
async fn daemon_exposes_its_requests_to_the_metadata_store() {
    let daemon = RunningDaemon::start().await;
    daemon.wait_until_ready().await;

    let metrics = daemon.scraped_metrics().await;

    assert!(
        metrics.contains("bitacora_metadata_store_requests_total{operation=\"transact\"}"),
        "{metrics}"
    );
    daemon.terminate().await;
}

#[tokio::test]
async fn shutdown_signal_stops_the_daemon_and_its_endpoint() {
    let daemon = RunningDaemon::start().await;
    daemon.wait_until_ready().await;
    let observability_address = daemon.observability_address.clone();

    daemon.terminate().await;

    let answer = http_get(&observability_address, "/healthz", Duration::from_secs(2)).await;
    assert!(answer.is_err(), "{answer:?}");
}
