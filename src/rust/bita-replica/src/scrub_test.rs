//! Tests of the scrub schedule.
//!
//! # What is covered
//!
//! - Nothing is scrubbed before the first interval has passed.
//! - After an interval a rotten copy of a sealed segment is found and reported as damaged.
//! - Each completed scrub is counted, and scrubs repeat every interval.
//! - Intact copies are left alone.
//!
//! # How the fixtures work
//!
//! A real replica on an in-memory disk holds one sealed segment. `rot_a_byte` flips a bit in its file
//! behind the replica's back, as a failing disk would. Time is paused, so a test that waits a week takes
//! no time.

use bitacora_process::MetricsRegistry;
use bitacora_segment::MemoryDisk;
use bitacora_segment::format::SEGMENT_HEADER_BYTES;
use bitacora_types::replica_api::ReplicaApi as _;
use tokio::task::JoinHandle;

use super::*;
use crate::test_support::{SEGMENT_FILE_PATH, open_replica, write_sealed_segment};

/// The interval the schedule under test uses: the default of one week.
const INTERVAL: Duration = Duration::from_hours(7 * 24);

/// A replica with one sealed segment and a scrub schedule running on it.
struct Fixture {
    /// The disk of the replica, to damage a file on.
    disk: MemoryDisk,
    /// The replica that is scrubbed.
    replica: Arc<Replica>,
    /// The registry the daemon's metrics are in.
    registry: MetricsRegistry,
    /// The schedule, which runs until it is aborted.
    schedule: JoinHandle<()>,
}

impl Fixture {
    /// Writes a sealed segment and starts the schedule.
    async fn start() -> Self {
        let disk = MemoryDisk::new();
        let replica = open_replica(&disk, "replica-a").await;
        write_sealed_segment(replica.as_ref()).await;
        let registry = MetricsRegistry::new();
        let metrics = ReplicaDaemonMetrics::register(&registry);
        let schedule = tokio::spawn(run_scrub_schedule(Arc::clone(&replica), INTERVAL, metrics));
        Self {
            disk,
            replica,
            registry,
            schedule,
        }
    }

    /// Flips one bit inside the first batch of the sealed segment's file.
    fn rot_a_byte(&self) {
        let offset_inside_the_first_batch = SEGMENT_HEADER_BYTES + 30;
        self.disk
            .corrupt_byte(SEGMENT_FILE_PATH, offset_inside_the_first_batch, 0x01);
    }

    /// Whether the replica reports its copy of the segment as damaged.
    async fn is_copy_damaged(&self) -> bool {
        let copies = self
            .replica
            .list_segments()
            .await
            .expect("the replica lists its copies");
        copies[0].is_damaged
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
}

/// Lets the schedule run for `duration` of paused time.
async fn let_pass(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[tokio::test(start_paused = true)]
async fn nothing_is_scrubbed_before_the_first_interval_has_passed() {
    let fixture = Fixture::start().await;
    fixture.rot_a_byte();

    let one_second_short_of_the_interval = Duration::from_secs(INTERVAL.as_secs() - 1);
    let_pass(one_second_short_of_the_interval).await;
    fixture.schedule.abort();

    assert!(!fixture.is_copy_damaged().await);
}

#[tokio::test(start_paused = true)]
async fn rotten_copy_is_found_after_the_interval() {
    let fixture = Fixture::start().await;
    fixture.rot_a_byte();

    let_pass(INTERVAL + Duration::from_secs(1)).await;
    fixture.schedule.abort();

    assert!(fixture.is_copy_damaged().await);
}

#[tokio::test(start_paused = true)]
async fn damaged_copies_found_by_a_scrub_are_kept_in_the_gauge() {
    let fixture = Fixture::start().await;
    fixture.rot_a_byte();

    let_pass(INTERVAL + Duration::from_secs(1)).await;
    fixture.schedule.abort();

    assert_eq!(
        fixture.metric("bitacora_replica_damaged_segment_copies "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn intact_copy_is_left_alone() {
    let fixture = Fixture::start().await;

    let_pass(INTERVAL + Duration::from_secs(1)).await;
    fixture.schedule.abort();

    assert!(!fixture.is_copy_damaged().await);
}

#[tokio::test(start_paused = true)]
async fn completed_scrub_is_counted() {
    let fixture = Fixture::start().await;

    let_pass(INTERVAL + Duration::from_secs(1)).await;
    fixture.schedule.abort();

    assert_eq!(
        fixture.metric("bitacora_replica_scrubs_total "),
        Some("1".to_owned())
    );
}

#[tokio::test(start_paused = true)]
async fn scrubs_repeat_every_interval() {
    let fixture = Fixture::start().await;

    let_pass(INTERVAL * 3 + Duration::from_secs(1)).await;
    fixture.schedule.abort();

    assert_eq!(
        fixture.metric("bitacora_replica_scrubs_total "),
        Some("3".to_owned())
    );
}

/// Rot that sets in after one scrub is found by the next one.
#[tokio::test(start_paused = true)]
async fn copy_that_rots_later_is_found_by_the_next_scrub() {
    let fixture = Fixture::start().await;
    let_pass(INTERVAL + Duration::from_secs(1)).await;

    fixture.rot_a_byte();
    let_pass(INTERVAL).await;
    fixture.schedule.abort();

    assert!(fixture.is_copy_damaged().await);
}

/// A replica that is stopping fails the scrub; the schedule must survive that.
#[tokio::test(start_paused = true)]
async fn schedule_goes_on_after_a_scrub_that_could_not_run() {
    let fixture = Fixture::start().await;
    fixture.replica.stop();

    let_pass(INTERVAL * 2 + Duration::from_secs(1)).await;

    assert!(!fixture.schedule.is_finished());
}
