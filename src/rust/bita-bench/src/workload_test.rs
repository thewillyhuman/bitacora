//! Tests of the workloads, against an in-process cluster under a virtual clock.
//!
//! # What is covered
//!
//! - Append: the record limit is met exactly and in whole appends, batches carry their records, every
//!   committed append has a latency, the duration limit and the pace are kept, paced latency counts from
//!   when an append was due, several streams are written at once, and the writer is closed afterwards.
//! - Refusals and failures: numbers that make no workload, a stream that does not exist, an append the
//!   cluster rejects.
//! - Read: the scan reaches the end of the stream, stops at its limit, and starts where it is told.
//!
//! # How the fixtures work
//!
//! The cluster is the test kit's: three replicas on in-memory disks. In memory an append takes no time at
//! all, and a virtual clock only moves when every task waits for it, so tests that need appends to take
//! time give the links to the replicas a delay with `slow_links`.

use bitacora::{ClientSettings, StreamOptions};
use bitacora_testkit::{Cluster, LinkFault};
use bitacora_types::MAX_RECORD_PAYLOAD_BYTES;

use super::*;

/// The stream the tests write.
const STREAM: &str = "orders";

/// A client of the cluster with the default settings.
fn client_of(cluster: &Cluster) -> BitacoraClient {
    BitacoraClient::new(
        cluster.metadata().clone(),
        cluster.connector(),
        cluster.clock(),
        ClientSettings::default(),
    )
}

/// A cluster of three replicas with [`STREAM`] created, and a client of it.
async fn cluster_with_stream() -> (Cluster, BitacoraClient) {
    let cluster = Cluster::start(3).await;
    let client = client_of(&cluster);
    client
        .admin()
        .create_stream(STREAM, StreamOptions::default())
        .await
        .expect("the stream is created");
    (cluster, client)
}

/// Makes every request to a replica take `delay`.
fn slow_links(cluster: &Cluster, delay: Duration) {
    for node in cluster.nodes() {
        node.link().set_fault(LinkFault {
            delay,
            ..LinkFault::default()
        });
    }
}

/// An append workload on [`STREAM`] of single records of 100 bytes, four appends in flight, limited to
/// `records` records.
fn append_workload(records: u64) -> AppendWorkload {
    AppendWorkload {
        stream: STREAM.to_owned(),
        stream_count: 1,
        record_bytes: 100,
        records_per_append: 1,
        appends_in_flight: 4,
        request_bytes: 262_144,
        limit: WorkloadLimit::Records(records),
        target_records_per_second: None,
        takeover: TakeoverPolicy::Force,
    }
}

/// How many records `stream` holds.
async fn records_in(client: &BitacoraClient, stream: &str) -> usize {
    let everything = SequenceRange::new(SequenceNumber::FIRST, SequenceNumber::new(1_000_000)).expect("a valid range");
    client
        .open_reader(stream)
        .read_range(everything)
        .await
        .expect("the stream is read")
        .len()
}

#[test]
fn workload_on_one_stream_writes_that_stream() {
    assert_eq!(stream_names(&append_workload(1)), [STREAM]);
}

#[test]
fn workload_on_several_streams_numbers_them() {
    let mut workload = append_workload(1);
    workload.stream_count = 3;

    assert_eq!(stream_names(&workload), ["orders-0", "orders-1", "orders-2"]);
}

#[tokio::test(start_paused = true)]
async fn record_limited_workload_reports_exactly_its_records() {
    let (_cluster, client) = cluster_with_stream().await;

    let measured = run_append_workloads(&client, &append_workload(50))
        .await
        .expect("the workload runs");

    assert_eq!(measured.appends, 50);
    assert_eq!(measured.records, 50);
    assert_eq!(measured.payload_bytes, 5_000);
    assert_eq!(measured.failure, None);
}

#[tokio::test(start_paused = true)]
async fn records_the_workload_reports_are_in_the_stream() {
    let (_cluster, client) = cluster_with_stream().await;

    run_append_workloads(&client, &append_workload(50))
        .await
        .expect("the workload runs");

    assert_eq!(records_in(&client, STREAM).await, 50);
}

#[tokio::test(start_paused = true)]
async fn appends_of_several_records_carry_all_of_them() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(40);
    workload.records_per_append = 8;

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert_eq!(measured.appends, 5);
    assert_eq!(measured.records, 40);
}

#[tokio::test(start_paused = true)]
async fn record_limit_is_rounded_up_to_whole_appends() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(10);
    workload.records_per_append = 4;

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert_eq!(measured.appends, 3);
    assert_eq!(measured.records, 12);
}

#[tokio::test(start_paused = true)]
async fn every_committed_append_has_a_latency() {
    let (_cluster, client) = cluster_with_stream().await;

    let measured = run_append_workloads(&client, &append_workload(50))
        .await
        .expect("the workload runs");

    assert_eq!(measured.latencies.count(), 50);
}

#[tokio::test(start_paused = true)]
async fn latency_is_the_time_an_append_takes_to_commit() {
    let (cluster, client) = cluster_with_stream().await;
    slow_links(&cluster, Duration::from_millis(20));
    let mut workload = append_workload(5);
    workload.appends_in_flight = 1;

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert!(
        measured.latencies.quantile(500) >= Duration::from_millis(20),
        "{:?}",
        measured.latencies.quantile(500)
    );
}

#[tokio::test(start_paused = true)]
async fn duration_limited_workload_stops_when_its_time_is_up() {
    let (cluster, client) = cluster_with_stream().await;
    slow_links(&cluster, Duration::from_millis(5));
    let mut workload = append_workload(0);
    workload.limit = WorkloadLimit::Duration(Duration::from_secs(2));

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert!(measured.appends > 0);
    assert!(measured.elapsed >= Duration::from_secs(2), "{:?}", measured.elapsed);
    assert!(measured.elapsed < Duration::from_secs(3), "{:?}", measured.elapsed);
}

#[tokio::test(start_paused = true)]
async fn paced_workload_takes_as_long_as_its_rate_says() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(20);
    workload.target_records_per_second = Some(10);

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    // The twentieth record is due 1.9 seconds after the first.
    assert!(
        measured.elapsed >= Duration::from_millis(1_900),
        "{:?}",
        measured.elapsed
    );
    assert!(
        measured.elapsed < Duration::from_millis(2_500),
        "{:?}",
        measured.elapsed
    );
}

/// The cluster commits one append in 300 ms, the workload wants one every 100 ms: the appends queue up,
/// and the wait in the queue is part of what whoever asked for that rate would see.
#[tokio::test(start_paused = true)]
async fn paced_latency_counts_from_when_an_append_was_due() {
    let (cluster, client) = cluster_with_stream().await;
    slow_links(&cluster, Duration::from_millis(300));
    let mut workload = append_workload(10);
    workload.appends_in_flight = 1;
    workload.target_records_per_second = Some(10);

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert!(
        measured.latencies.largest() >= Duration::from_secs(2),
        "{:?}",
        measured.latencies.largest()
    );
}

#[tokio::test(start_paused = true)]
async fn several_streams_are_written_at_once_and_added_up() {
    let cluster = Cluster::start(3).await;
    let client = client_of(&cluster);
    let mut workload = append_workload(10);
    workload.stream_count = 2;
    create_missing_streams(&client, &workload)
        .await
        .expect("the streams are created");

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload runs");

    assert_eq!(measured.records, 20);
    assert_eq!(records_in(&client, "orders-0").await, 10);
    assert_eq!(records_in(&client, "orders-1").await, 10);
}

#[tokio::test(start_paused = true)]
async fn only_the_missing_streams_are_created() {
    let cluster = Cluster::start(3).await;
    let client = client_of(&cluster);
    let mut workload = append_workload(10);
    workload.stream_count = 2;
    let already_there = client
        .admin()
        .create_stream("orders-0", StreamOptions::default())
        .await
        .expect("the stream is created");

    create_missing_streams(&client, &workload)
        .await
        .expect("the streams are created");

    let streams = client.admin().list_streams().await.expect("lists");
    assert_eq!(streams.len(), 2);
    assert_eq!(streams[0].stream_id, already_there.stream_id);
}

#[tokio::test(start_paused = true)]
async fn writer_is_closed_when_the_workload_is_over() {
    let (_cluster, client) = cluster_with_stream().await;

    run_append_workloads(&client, &append_workload(5))
        .await
        .expect("the workload runs");

    let described = client.admin().describe_stream(STREAM).await.expect("the stream exists");
    assert_eq!(described.stream.writer, None);
}

#[tokio::test(start_paused = true)]
async fn workload_on_a_stream_that_does_not_exist_cannot_start() {
    let cluster = Cluster::start(3).await;
    let client = client_of(&cluster);

    let refused = run_append_workloads(&client, &append_workload(5)).await;

    assert!(matches!(refused, Err(WorkloadError::CannotOpen { .. })));
}

#[tokio::test(start_paused = true)]
async fn workload_with_requests_of_no_bytes_is_refused() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(5);
    workload.request_bytes = 0;

    let refused = run_append_workloads(&client, &workload).await;

    assert!(matches!(refused, Err(WorkloadError::InvalidWorkload { .. })));
}

#[tokio::test(start_paused = true)]
async fn workload_without_appends_in_flight_is_refused() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(5);
    workload.appends_in_flight = 0;

    let refused = run_append_workloads(&client, &workload).await;

    assert!(matches!(refused, Err(WorkloadError::InvalidWorkload { .. })));
}

#[tokio::test(start_paused = true)]
async fn workload_with_a_rate_of_zero_is_refused() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(5);
    workload.target_records_per_second = Some(0);

    let refused = run_append_workloads(&client, &workload).await;

    assert!(matches!(refused, Err(WorkloadError::InvalidWorkload { .. })));
}

#[tokio::test(start_paused = true)]
async fn append_the_cluster_rejects_stops_the_workload_and_is_reported() {
    let (_cluster, client) = cluster_with_stream().await;
    let mut workload = append_workload(5);
    workload.record_bytes = MAX_RECORD_PAYLOAD_BYTES + 1;

    let measured = run_append_workloads(&client, &workload)
        .await
        .expect("the workload starts");

    assert_eq!(measured.appends, 0);
    assert!(measured.failure.is_some());
}

/// A read workload over the whole of [`STREAM`], limited to `records` records.
fn read_workload(from: Option<u64>, records: u64) -> ReadWorkload {
    ReadWorkload {
        stream: STREAM.to_owned(),
        from,
        limit: WorkloadLimit::Records(records),
    }
}

/// A cluster whose stream holds thirty records of 100 bytes.
async fn cluster_with_thirty_records() -> (Cluster, BitacoraClient) {
    let (cluster, client) = cluster_with_stream().await;
    run_append_workloads(&client, &append_workload(30))
        .await
        .expect("the workload runs");
    (cluster, client)
}

#[tokio::test(start_paused = true)]
async fn scan_reads_to_the_end_of_the_stream() {
    let (_cluster, client) = cluster_with_thirty_records().await;

    let measured = run_read_workload(&client, &read_workload(None, 1_000_000))
        .await
        .expect("the scan runs");

    assert_eq!(measured.records, 30);
    assert_eq!(measured.payload_bytes, 3_000);
    assert_eq!(measured.failure, None);
}

#[tokio::test(start_paused = true)]
async fn scan_stops_at_its_record_limit() {
    let (_cluster, client) = cluster_with_thirty_records().await;

    let measured = run_read_workload(&client, &read_workload(None, 7))
        .await
        .expect("the scan runs");

    assert_eq!(measured.records, 7);
}

#[tokio::test(start_paused = true)]
async fn scan_starts_at_the_sequence_it_is_given() {
    let (_cluster, client) = cluster_with_thirty_records().await;

    let measured = run_read_workload(&client, &read_workload(Some(21), 1_000_000))
        .await
        .expect("the scan runs");

    assert_eq!(measured.records, 10);
}

#[tokio::test(start_paused = true)]
async fn scan_limited_by_a_duration_still_ends_at_the_end_of_the_stream() {
    let (_cluster, client) = cluster_with_thirty_records().await;
    let workload = ReadWorkload {
        stream: STREAM.to_owned(),
        from: None,
        limit: WorkloadLimit::Duration(Duration::from_mins(1)),
    };

    let measured = run_read_workload(&client, &workload).await.expect("the scan runs");

    assert_eq!(measured.records, 30);
}

#[tokio::test(start_paused = true)]
async fn scan_of_a_stream_that_does_not_exist_cannot_start() {
    let cluster = Cluster::start(3).await;
    let client = client_of(&cluster);

    let refused = run_read_workload(&client, &read_workload(None, 10)).await;

    assert!(matches!(refused, Err(WorkloadError::CannotRead { .. })));
}
