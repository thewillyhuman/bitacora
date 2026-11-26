//! A cluster of real processes, used the way an application and an operator use it.
//!
//! # Why this test exists
//!
//! Everything else in the repository tests the protocol in one process, on in-memory disks and a
//! virtual clock. This test runs what is actually deployed: the `bita-replica` and `bita-controller`
//! programs with configuration files, a real etcd, gRPC over loopback sockets, real files, real
//! signals. It is the last layer of the design's testing plan (section 15, "Layer 4: real clusters"),
//! kept small: it shows that the pieces work together as processes, not that the protocol is correct.
//!
//! # What is covered
//!
//! - One scenario from start to stop: a stream is created, written and read; a replica is killed with
//!   SIGKILL and appends go on while the writer rolls; the replica is restarted and the controller
//!   completes its copies; every daemon answers its probes and exposes its metrics; SIGTERM stops
//!   every daemon with exit code 0.
//! - What a daemon does with a wrong command line or configuration: exit code 2 and a message.
//!
//! # How the fixtures work
//!
//! `bitacora-clusterkit` starts the processes on loopback ports of its own in a temporary directory and
//! kills them when the test ends, also when it fails. The programs come from environment variables that
//! the Bazel target sets. Under Cargo the tests pass without running unless the variables are set; they
//! say so on standard error.
//!
//! Nothing here sleeps to wait for the cluster: every wait polls for the state it needs and is bounded
//! by `GENEROUS_WAIT`.

use std::process::Command;
use std::time::Duration;

use bitacora::{
    BitacoraClient, ClientSettings, SequenceNumber, SequenceRange, StreamOptions, StreamWriter, TakeoverPolicy,
    WriterSettings,
};
use bitacora_clusterkit::{Binaries, ChildProcess, RealCluster};
use bitacora_meta::{CopyState, MetadataStore, StreamRecord};
use bitacora_process::http_get;
use bytes::Bytes;
use tokio::time::{Instant, sleep};

/// The stream the scenario writes to.
const STREAM: &str = "orders";

/// How many replica daemons the scenario runs: what a stream with the default replication needs.
const REPLICA_COUNT: usize = 3;

/// The replica the scenario kills and restarts.
const LOST_REPLICA: usize = 1;

/// How many records each phase of the scenario appends.
const RECORDS_PER_PHASE: u64 = 40;

/// Far longer than any step takes on an idle machine; a step that waits this long has failed.
const GENEROUS_WAIT: Duration = Duration::from_secs(45);

/// How long to wait between two looks at something that is about to change.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Client settings with a writer that gives up on a silent replica after one second instead of ten, so
/// that the scenario sees the roll without waiting.
fn client_settings() -> ClientSettings {
    ClientSettings {
        writer: WriterSettings {
            writer_identity: "real-cluster-test".to_owned(),
            replica_failure_timeout: Duration::from_secs(1),
            ..WriterSettings::default()
        },
        ..ClientSettings::default()
    }
}

/// The payload of the record at `sequence`.
fn payload_of(sequence: u64) -> Bytes {
    Bytes::from(format!("record-{sequence}"))
}

/// Appends `count` records after the last one the writer committed, each naming its own sequence
/// number, and checks that each commits at the sequence number it expects.
async fn append_records(writer: &StreamWriter, count: u64) {
    let first_sequence = writer.durable_sequence().get() + 1;
    for sequence in first_sequence..first_sequence + count {
        let committed_at = writer.append(payload_of(sequence)).await.expect("the record commits");
        assert_eq!(committed_at, SequenceNumber::new(sequence));
    }
}

/// Reads records 1 to `last_sequence` and returns their payloads.
async fn read_payloads(client: &BitacoraClient, last_sequence: u64) -> Vec<Bytes> {
    let whole_stream =
        SequenceRange::new(SequenceNumber::FIRST, SequenceNumber::new(last_sequence)).expect("a valid range");
    let records = client
        .open_reader(STREAM)
        .read_range(whole_stream)
        .await
        .expect("the records can be read");
    let mut payloads = Vec::with_capacity(records.len());
    for record in records {
        payloads.push(record.payload);
    }
    payloads
}

/// The payloads of records 1 to `last_sequence`, as they were appended.
fn expected_payloads(last_sequence: u64) -> Vec<Bytes> {
    let mut payloads = Vec::new();
    for sequence in 1..=last_sequence {
        payloads.push(payload_of(sequence));
    }
    payloads
}

/// Whether the writer has rolled at least once: the stream has more than its first segment.
fn has_rolled(stream: &StreamRecord) -> bool {
    stream.segments.len() >= 2
}

/// Whether every sealed segment of the stream has a full set of good copies.
fn is_every_sealed_segment_fully_replicated(stream: &StreamRecord) -> bool {
    for segment in &stream.segments {
        if segment.sealed.is_none() {
            continue;
        }
        if segment.copies.len() != REPLICA_COUNT {
            return false;
        }
        for copy in &segment.copies {
            if copy.state != CopyState::Good {
                return false;
            }
        }
    }
    true
}

/// Waits until the stream's record satisfies `is_reached`, and returns that record.
///
/// # Panics
///
/// Panics with `description` if that does not happen within [`GENEROUS_WAIT`].
async fn wait_for_stream(
    metadata: &MetadataStore,
    description: &str,
    is_reached: fn(&StreamRecord) -> bool,
) -> StreamRecord {
    let deadline = Instant::now() + GENEROUS_WAIT;
    loop {
        let stream = metadata.get_stream(STREAM).await;
        if let Ok(stream) = &stream
            && is_reached(stream)
        {
            return stream.clone();
        }
        assert!(
            Instant::now() < deadline,
            "{description} did not happen within {GENEROUS_WAIT:?}; the stream is {stream:?}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

/// The HTTP status a daemon's observability endpoint answers `path` with.
async fn status_of(observability_address: &str, path: &str) -> u16 {
    http_get(observability_address, path, GENEROUS_WAIT)
        .await
        .expect("the endpoint answers")
        .status
}

/// A daemon's metrics, as Prometheus would scrape them.
async fn scraped_metrics(observability_address: &str) -> String {
    http_get(observability_address, "/metrics", GENEROUS_WAIT)
        .await
        .expect("the endpoint answers")
        .body
}

/// Waits until the metrics of the daemon at `observability_address` contain `expected_line`.
///
/// # Panics
///
/// Panics with the scraped metrics if that does not happen within [`GENEROUS_WAIT`].
async fn wait_for_metric(observability_address: &str, expected_line: &str) {
    let deadline = Instant::now() + GENEROUS_WAIT;
    loop {
        let metrics = scraped_metrics(observability_address).await;
        if metrics.contains(expected_line) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{expected_line:?} did not appear in the metrics:\n{metrics}"
        );
        sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_of_real_processes_survives_the_loss_and_the_return_of_a_replica() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };
    let mut cluster = RealCluster::start(&binaries, REPLICA_COUNT).await;
    let client = bitacora::connect(cluster.cluster_config(), client_settings())
        .await
        .expect("the cluster configuration is valid");
    let metadata = bitacora::connect_metadata(cluster.cluster_config())
        .await
        .expect("the cluster configuration is valid");

    // An application creates a stream, writes to it and reads it back.
    client
        .admin()
        .create_stream(STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    let writer = client
        .open_writer(STREAM, TakeoverPolicy::Force)
        .await
        .expect("the writer opens");
    append_records(&writer, RECORDS_PER_PHASE).await;
    assert_eq!(
        read_payloads(&client, RECORDS_PER_PHASE).await,
        expected_payloads(RECORDS_PER_PHASE),
        "what was written is read back"
    );

    // A replica is lost. Appends go on with the remaining quorum, and the writer rolls away from the
    // segment that has a silent replica.
    cluster.kill_replica(LOST_REPLICA);
    append_records(&writer, RECORDS_PER_PHASE).await;
    wait_for_stream(&metadata, "the roll after the loss of a replica", has_rolled).await;
    append_records(&writer, RECORDS_PER_PHASE).await;

    // The replica returns on its data directory. It registers again, and the controller completes
    // the copies it missed.
    cluster.restart_replica(LOST_REPLICA).await;
    wait_for_stream(
        &metadata,
        "three good copies of every sealed segment",
        is_every_sealed_segment_fully_replicated,
    )
    .await;
    append_records(&writer, RECORDS_PER_PHASE).await;
    let records_appended = RECORDS_PER_PHASE * 4;
    assert_eq!(
        read_payloads(&client, records_appended).await,
        expected_payloads(records_appended),
        "nothing was lost or reordered"
    );

    // An operator's view: every daemon answers its probes and exposes its metrics.
    for replica_number in 0..cluster.replica_count() {
        let observability_address = cluster.replica_observability_address(replica_number);
        assert_eq!(
            status_of(observability_address, "/healthz").await,
            200,
            "replica {replica_number} is alive"
        );
        assert_eq!(
            status_of(observability_address, "/readyz").await,
            200,
            "replica {replica_number} is ready"
        );
        let metrics = scraped_metrics(observability_address).await;
        assert!(
            metrics.contains("bitacora_replica_requests_total{operation=\"append\"}"),
            "replica {replica_number} counts its appends:\n{metrics}"
        );
        assert!(
            metrics.contains("bitacora_metadata_store_available 1\n"),
            "replica {replica_number} reaches the metadata store:\n{metrics}"
        );
    }
    let controller_address = cluster.controller_observability_address();
    assert_eq!(status_of(controller_address, "/readyz").await, 200);
    wait_for_metric(controller_address, "bitacora_controller_leading 1\n").await;
    wait_for_metric(controller_address, "bitacora_controller_segments_missing_copies 0\n").await;

    // A supervisor stops everything. Each daemon exits cleanly.
    writer.close().await.expect("the writer closes");
    drop(client);
    for exited_daemon in cluster.stop().await {
        let exit_code = exited_daemon.exit_status.and_then(|exit_status| exit_status.code());
        assert_eq!(exit_code, Some(0), "{} stops cleanly", exited_daemon.name);
    }
}

/// Runs `binary` with `arguments` until it exits by itself.
///
/// # Returns
///
/// Its exit code, if it exited with one, and everything it wrote.
async fn run_to_completion(binary: &std::path::Path, arguments: &[&str]) -> (Option<i32>, String) {
    let directory = tempfile::tempdir().expect("a temporary directory can be created");
    let mut command = Command::new(binary);
    command.args(arguments);
    let mut process = ChildProcess::spawn("daemon", &mut command, &directory.path().join("log"));
    let exit_status = process
        .wait_for_exit(GENEROUS_WAIT)
        .await
        .expect("the daemon exits by itself");
    (exit_status.code(), process.log())
}

#[tokio::test]
async fn replica_daemon_without_arguments_exits_with_code_two() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };

    let (exit_code, _output) = run_to_completion(&binaries.replica, &[]).await;

    assert_eq!(exit_code, Some(2));
}

#[tokio::test]
async fn controller_daemon_without_its_configuration_file_exits_with_code_two() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };

    let (exit_code, _output) = run_to_completion(
        &binaries.controller,
        &["--config", "/no/such/directory/controller.toml"],
    )
    .await;

    assert_eq!(exit_code, Some(2));
}

#[tokio::test]
async fn daemon_names_the_configuration_file_it_cannot_read() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };

    let (_exit_code, output) = run_to_completion(
        &binaries.controller,
        &["--config", "/no/such/directory/controller.toml"],
    )
    .await;

    assert!(output.contains("/no/such/directory/controller.toml"), "{output}");
}

#[tokio::test]
async fn replica_daemon_prints_its_version_and_exits_successfully() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };

    let (exit_code, output) = run_to_completion(&binaries.replica, &["--version"]).await;

    assert_eq!(exit_code, Some(0));
    assert!(output.starts_with("bita-replica "), "{output}");
}

#[tokio::test]
async fn controller_daemon_prints_its_help_and_exits_successfully() {
    let Some(binaries) = Binaries::from_environment() else {
        return;
    };

    let (exit_code, output) = run_to_completion(&binaries.controller, &["--help"]).await;

    assert_eq!(exit_code, Some(0));
    assert!(output.starts_with("Usage: bita-controller --config <path>"), "{output}");
}
