//! Tests of the verdict on one stream's history and of the dump that keeps the evidence.
//!
//! # What is covered
//!
//! - A history in which the audit found every committed record has no violation.
//! - A record that was reported as committed and is missing from the audit is a violation, and so is a reader that
//!   was handed something else than what was committed.
//! - A dump has one line per event, in order, and is written to the file it was asked for.
//!
//! # How the fixtures work
//!
//! `Told` is a history written by hand: `commit` records an append and its commit, `audit` the read of the whole
//! stream. The tests run on a paused clock, so every event is stamped with a time the test chose.

use bitacora_sim::{AppendOutcome, History};
use bitacora_types::{Epoch, SequenceNumber};
use bytes::Bytes;

use super::*;

/// A history told by hand.
struct Told {
    /// The history being told.
    history: History,
}

impl Told {
    /// A history with nothing in it.
    fn new() -> Self {
        Self {
            history: History::new(),
        }
    }

    /// Records that `payload` was appended and reported as committed at `sequence`, and returns the append's
    /// identifier.
    fn commit(&self, payload: &'static str, sequence: u64) -> u64 {
        let operation = self.history.append_invoked(Bytes::from_static(payload.as_bytes()));
        let outcome = AppendOutcome::Committed {
            sequence: SequenceNumber::new(sequence),
            epoch: Epoch::new(1),
        };
        self.history.append_finished(operation, outcome);
        operation
    }

    /// Records a read of the whole stream that was handed `payloads` from the first sequence number on, and returns
    /// its session.
    fn audit(&self, payloads: &[&'static str]) -> u64 {
        let session = self.history.reader_started(SequenceNumber::FIRST);
        let mut sequence = SequenceNumber::FIRST;
        for payload in payloads {
            self.history
                .record_read(session, sequence, Bytes::from_static(payload.as_bytes()));
            sequence = sequence.next();
        }
        session
    }
}

#[tokio::test(start_paused = true)]
async fn history_whose_audit_found_every_commit_has_no_violation() {
    let told = Told::new();
    told.commit("a", 1);
    told.commit("b", 2);
    let audit_session = told.audit(&["a", "b"]);

    let violations = violations_of(&told.history.events(), audit_session);

    assert_eq!(violations, []);
}

#[tokio::test(start_paused = true)]
async fn commit_that_the_audit_did_not_find_is_a_violation() {
    let told = Told::new();
    told.commit("a", 1);
    let lost = told.commit("b", 2);
    let audit_session = told.audit(&["a"]);

    let violations = violations_of(&told.history.events(), audit_session);

    assert_eq!(
        violations,
        [Violation::CommittedRecordMissing {
            operation: lost,
            sequence: SequenceNumber::new(2),
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn record_nobody_appended_is_a_violation() {
    let told = Told::new();
    told.commit("a", 1);
    let audit_session = told.audit(&["a", "from nowhere"]);

    let violations = violations_of(&told.history.events(), audit_session);

    assert_eq!(
        violations,
        [Violation::UnknownRecord {
            session: audit_session,
            sequence: SequenceNumber::new(2),
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn dump_has_one_line_per_event_in_order() {
    let told = Told::new();
    told.history.fault("replica bitacora-replica-1 is killed");
    tokio::time::advance(std::time::Duration::from_millis(1_500)).await;
    told.history.fault("zone zone-b is cut off the network");

    let text = dump_text(&told.history.events());

    assert_eq!(
        text,
        "   0.000ns Fault { description: \"replica bitacora-replica-1 is killed\" }\n    \
         1.500s Fault { description: \"zone zone-b is cut off the network\" }\n"
    );
}

#[tokio::test(start_paused = true)]
async fn dump_is_written_to_the_file_it_was_asked_for() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let told = Told::new();
    told.commit("a", 1);

    let path = write_dump(directory.path(), "soak-1-1-0.history", &told.history.events()).expect("the dump is written");

    assert_eq!(path, directory.path().join("soak-1-1-0.history"));
    let written = std::fs::read_to_string(&path).expect("the dump can be read");
    assert_eq!(written, dump_text(&told.history.events()));
}

#[tokio::test(start_paused = true)]
async fn dump_into_a_directory_that_does_not_exist_fails() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let told = Told::new();

    let written = write_dump(
        &directory.path().join("missing"),
        "soak.history",
        &told.history.events(),
    );

    assert!(written.is_err(), "{written:?}");
}
