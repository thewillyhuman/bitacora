//! Tests of the report of a run.
//!
//! # What is covered
//!
//! - Judging: which checks run, in which order their violations are listed, what is counted, and how a run whose audit
//!   did not finish is reported.
//! - When a report is clean.
//! - The fingerprint: pinned for a known history, equal for equal histories, different when an event or its time
//!   differs.
//! - The failure summary: what it says and how much of a long run it shows.
//!
//! # How the fixtures work
//!
//! Histories are written by hand with the `Script` of the test support, or recorded through a `History` under a paused
//! runtime when the time of an event matters. No simulation is run.

use std::time::Duration;

use bitacora_types::SequenceNumber;
use bytes::Bytes;

use super::*;
use crate::history::{History, KvCall, KvOutcome};
use crate::test_support::{Script, committed, correct_history};

/// The seed the reports of these tests claim to come from.
const SEED: u64 = 7;

/// The session the correct history of the test support reads the whole stream in.
const AUDIT_SESSION: u64 = 1;

/// The fingerprint of the history of one fault, `node 2 crashes for 3s`, at time zero. It is the FNV-1a hash of the
/// `Debug` rendering of that one timed event, and was computed outside this crate.
const PINNED_FINGERPRINT: &str = "cc84525dd9f694f8";

/// A report of the given events with nothing wrong and nothing counted.
fn report_of(events: Vec<TimedEvent>) -> SimulationReport {
    SimulationReport {
        seed: SEED,
        events,
        violations: Vec::new(),
        stuck: None,
        committed_appends: 0,
        faults_injected: 0,
        tiered_segment_reads: 0,
    }
}

/// A history of one event: a fault described as `description`, at time zero.
fn one_fault(description: &str) -> Vec<TimedEvent> {
    let mut script = Script::default();
    script.push(Event::Fault {
        description: description.to_owned(),
    });
    script.events
}

/// A history in which a put finished and a later read does not see it, which is not linearizable.
fn history_with_a_stale_read() -> Script {
    let mut script = Script::default();
    let key = Bytes::from("k");
    script.push(Event::KvInvoked {
        operation: 101,
        call: KvCall::Put {
            key: key.clone(),
            value: Bytes::from("v"),
        },
    });
    script.push(Event::KvReturned {
        operation: 101,
        outcome: KvOutcome::Written,
    });
    script.push(Event::KvInvoked {
        operation: 102,
        call: KvCall::Get { key },
    });
    script.push(Event::KvReturned {
        operation: 102,
        outcome: KvOutcome::Read { value: None },
    });
    script
}

#[test]
fn correct_history_whose_audit_finished_is_clean() {
    let report = judge(SEED, &correct_history().events, Ok(AUDIT_SESSION));

    assert!(report.is_clean(), "{}", report.failure_summary());
}

#[test]
fn report_keeps_the_seed_and_the_events() {
    let events = correct_history().events;

    let report = judge(SEED, &events, Ok(AUDIT_SESSION));

    assert_eq!(report.seed, SEED);
    assert_eq!(report.events, events);
}

#[test]
fn committed_appends_are_counted() {
    let report = judge(SEED, &correct_history().events, Ok(AUDIT_SESSION));

    assert_eq!(report.committed_appends, 2);
}

#[test]
fn faults_are_counted() {
    let mut script = correct_history();
    script.push(Event::Fault {
        description: "node 0 crashes".to_owned(),
    });
    script.push(Event::Fault {
        description: "node 1 crashes".to_owned(),
    });

    let report = judge(SEED, &script.events, Ok(AUDIT_SESSION));

    assert_eq!(report.faults_injected, 2);
}

#[test]
fn tiered_segment_reads_are_left_for_the_caller_to_fill_in() {
    let report = judge(SEED, &correct_history().events, Ok(AUDIT_SESSION));

    assert_eq!(report.tiered_segment_reads, 0);
}

#[test]
fn run_whose_audit_did_not_finish_is_reported_as_stuck() {
    let report = judge(SEED, &correct_history().events, Err("it was starting".to_owned()));

    assert_eq!(report.stuck, Some("it was starting".to_owned()));
}

/// Without an audit there is no session to hold the committed records against, so the audit check is skipped and the
/// run fails for being stuck, not for records an audit never read.
#[test]
fn run_whose_audit_did_not_finish_has_no_audit_violation() {
    let mut script = Script::default();
    script.append("a", Some(committed(1, 1)));

    let report = judge(SEED, &script.events, Err("it was starting".to_owned()));

    assert_eq!(report.violations, Vec::new());
}

#[test]
fn audit_that_missed_a_committed_record_is_a_violation() {
    let mut script = Script::default();
    let lost = script.append("a", Some(committed(1, 1)));
    script.read(AUDIT_SESSION, 1, &[]);

    let report = judge(SEED, &script.events, Ok(AUDIT_SESSION));

    assert_eq!(
        report.violations,
        [Violation::CommittedRecordMissing {
            operation: lost,
            sequence: SequenceNumber::new(1),
        }]
    );
}

#[test]
fn history_that_is_not_linearizable_is_a_violation() {
    let report = judge(SEED, &history_with_a_stale_read().events, Err("stuck".to_owned()));

    assert_eq!(
        report.violations,
        [Violation::NotLinearizable {
            key: Bytes::from("k"),
            stuck_at_operation: 102,
        }]
    );
}

/// The order is fixed so that two reports of one history can be compared.
#[test]
fn violations_are_listed_log_first_then_linearizability_then_audit() {
    let mut script = history_with_a_stale_read();
    let lost = script.append("a", Some(committed(1, 1)));
    script.read(AUDIT_SESSION, 1, &[]);
    script.read(2, 1, &[(2, "a")]);

    let report = judge(SEED, &script.events, Ok(AUDIT_SESSION));

    assert_eq!(
        report.violations,
        [
            Violation::ReaderOutOfOrder {
                session: 2,
                expected: SequenceNumber::new(1),
                found: SequenceNumber::new(2),
            },
            Violation::NotLinearizable {
                key: Bytes::from("k"),
                stuck_at_operation: 102,
            },
            Violation::CommittedRecordMissing {
                operation: lost,
                sequence: SequenceNumber::new(1),
            },
        ]
    );
}

#[test]
fn report_with_a_violation_is_not_clean() {
    let mut report = report_of(Vec::new());
    report.violations.push(Violation::UnknownRecord {
        session: 1,
        sequence: SequenceNumber::new(1),
    });

    assert!(!report.is_clean());
}

#[test]
fn report_of_a_stuck_run_is_not_clean() {
    let mut report = report_of(Vec::new());
    report.stuck = Some("it was starting".to_owned());

    assert!(!report.is_clean());
}

#[test]
fn fingerprint_of_an_empty_history_is_the_offset_basis() {
    assert_eq!(report_of(Vec::new()).fingerprint(), 0xCBF2_9CE4_8422_2325);
}

/// Fingerprints are written down in commit messages and compared across versions of the code. The hash must never
/// change for a history that did not.
#[test]
fn fingerprint_of_a_known_history_is_pinned() {
    let report = report_of(one_fault("node 2 crashes for 3s"));

    assert_eq!(format!("{:016x}", report.fingerprint()), PINNED_FINGERPRINT);
}

#[test]
fn equal_histories_have_equal_fingerprints() {
    let first = report_of(correct_history().events);
    let second = report_of(correct_history().events);

    assert_eq!(first.fingerprint(), second.fingerprint());
}

#[test]
fn fingerprint_changes_with_what_an_event_says() {
    let first = report_of(one_fault("node 2 crashes for 3s"));
    let second = report_of(one_fault("node 3 crashes for 3s"));

    assert_ne!(first.fingerprint(), second.fingerprint());
}

#[tokio::test(start_paused = true)]
async fn fingerprint_changes_with_when_an_event_happened() {
    let early = History::new();
    early.fault("node 2 crashes for 3s");
    let late = History::new();
    tokio::time::advance(Duration::from_millis(1)).await;
    late.fault("node 2 crashes for 3s");

    assert_ne!(
        report_of(early.events()).fingerprint(),
        report_of(late.events()).fingerprint()
    );
}

/// What is counted and judged is derived from the events, and is not hashed again.
#[test]
fn fingerprint_depends_on_the_events_only() {
    let plain = report_of(correct_history().events);
    let mut with_other_counts = report_of(correct_history().events);
    with_other_counts.seed = SEED + 1;
    with_other_counts.committed_appends = 99;
    with_other_counts.stuck = Some("it was starting".to_owned());

    assert_eq!(plain.fingerprint(), with_other_counts.fingerprint());
}

#[test]
fn failure_summary_opens_with_the_seed_and_the_counts() {
    let mut report = report_of(Vec::new());
    report.committed_appends = 12;
    report.faults_injected = 3;

    let summary = report.failure_summary();

    assert_eq!(
        summary.lines().next(),
        Some("simulation seed 7 failed after 12 committed appends and 3 faults")
    );
}

#[test]
fn failure_summary_says_why_the_cluster_was_stuck() {
    let mut report = report_of(Vec::new());
    report.stuck = Some("it was starting".to_owned());

    let summary = report.failure_summary();

    assert!(summary.contains("\nstuck: it was starting\n"), "{summary}");
}

#[test]
fn failure_summary_lists_the_violations() {
    let mut report = report_of(Vec::new());
    report.violations.push(Violation::UnknownRecord {
        session: 1,
        sequence: SequenceNumber::new(4),
    });

    let summary = report.failure_summary();

    assert!(
        summary.contains("\nviolation: UnknownRecord { session: 1, sequence: SequenceNumber(4) }\n"),
        "{summary}"
    );
}

#[test]
fn failure_summary_lists_no_more_than_ten_violations() {
    let mut report = report_of(Vec::new());
    for session in 0..25 {
        report.violations.push(Violation::UnknownRecord {
            session,
            sequence: SequenceNumber::new(4),
        });
    }

    let summary = report.failure_summary();

    assert_eq!(summary.matches("violation: ").count(), 10);
}

#[test]
fn failure_summary_shows_the_events_with_their_times() {
    let report = report_of(one_fault("node 2 crashes for 3s"));

    let summary = report.failure_summary();

    assert!(
        summary.ends_with("last events:\n     0.000ns Fault { description: \"node 2 crashes for 3s\" }\n"),
        "{summary}"
    );
}

#[test]
fn failure_summary_shows_only_the_last_forty_events() {
    let mut script = Script::default();
    for fault_number in 0..100 {
        script.push(Event::Fault {
            description: format!("fault {fault_number}"),
        });
    }
    let report = report_of(script.events);

    let summary = report.failure_summary();

    assert_eq!(summary.matches("Fault {").count(), 40);
    assert!(summary.contains("\"fault 99\""), "{summary}");
    assert!(!summary.contains("\"fault 59\""), "{summary}");
}
