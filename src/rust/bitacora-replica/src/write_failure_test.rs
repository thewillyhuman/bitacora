//! Tests of the record of a write the disk refused.
//!
//! # What is covered
//!
//! - A report starts empty, keeps the first refusal and ignores later ones.
//! - Clones share one record, which is how the tasks of a replica report to it.
//! - Waiting returns at once when a refusal is already recorded, wakes when one arrives, and goes on
//!   waiting when none does.
//!
//! # How the fixtures work
//!
//! The tests use two segments, [`FIRST_REFUSED`] and [`LATER_REFUSED`], and record refusals by hand. The
//! waiting tests run on a paused clock, so the hour one of them waits takes no real time.

use std::time::Duration;

use bitacora_types::{SegmentId, StreamId};

use super::*;

/// The segment whose refusal is recorded first.
const FIRST_REFUSED: SegmentAddress = SegmentAddress::new(StreamId::new(7), SegmentId::new(3));

/// A segment whose refusal is recorded after another one.
const LATER_REFUSED: SegmentAddress = SegmentAddress::new(StreamId::new(8), SegmentId::new(1));

/// Longer than any test waits for something that does happen.
const A_LONG_WAIT: Duration = Duration::from_hours(1);

#[test]
fn new_report_holds_no_refusal() {
    let report = WriteFailureReport::new();

    assert_eq!(report.first(), None);
}

#[test]
fn recorded_refusal_names_the_segment_and_the_reason() {
    let report = WriteFailureReport::new();

    report.record(FIRST_REFUSED, "flush failed");

    assert_eq!(
        report.first(),
        Some(WriteFailure {
            segment: FIRST_REFUSED,
            reason: "flush failed".to_owned(),
        })
    );
}

/// The first refusal is the one that says the process must stop. What fails afterwards is a consequence.
#[test]
fn later_refusal_does_not_replace_the_first() {
    let report = WriteFailureReport::new();
    report.record(FIRST_REFUSED, "flush failed");

    report.record(LATER_REFUSED, "write failed");

    let kept = report.first().expect("a refusal is recorded");
    assert_eq!(kept.segment, FIRST_REFUSED);
}

#[test]
fn refusal_recorded_through_a_clone_is_seen_by_the_original() {
    let report = WriteFailureReport::new();
    let report_of_a_segment_task = report.clone();

    report_of_a_segment_task.record(FIRST_REFUSED, "flush failed");

    assert!(report.first().is_some());
}

#[tokio::test(start_paused = true)]
async fn waiting_returns_at_once_when_a_refusal_is_already_recorded() {
    let report = WriteFailureReport::new();
    report.record(FIRST_REFUSED, "flush failed");
    let asked_at = tokio::time::Instant::now();

    let refusal = report.wait_for_first().await;

    assert_eq!(refusal.segment, FIRST_REFUSED);
    assert_eq!(asked_at.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn waiting_wakes_when_a_refusal_is_recorded() {
    let report = WriteFailureReport::new();
    let waiting_host = {
        let report = report.clone();
        tokio::spawn(async move { report.wait_for_first().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;

    report.record(FIRST_REFUSED, "flush failed");

    let refusal = waiting_host.await.expect("the waiting task does not panic");
    assert_eq!(refusal.reason, "flush failed");
}

#[tokio::test(start_paused = true)]
async fn waiting_goes_on_while_nothing_is_refused() {
    let report = WriteFailureReport::new();

    let waited = tokio::time::timeout(A_LONG_WAIT, report.wait_for_first()).await;

    assert!(waited.is_err(), "{waited:?}");
}
