//! The audit: reading a whole stream back once the faults are gone.
//!
//! # Why this file exists
//!
//! During a round, readers follow the streams, but nobody can say that they read everything: a record that no
//! reader happened to be handed could be gone without anyone noticing. And some appends ended without their writer
//! learning how. A round can only be judged once both are settled: once it is known what the stream holds, from its
//! first record to its last. This file finds that out, the way the simulator's audit does.
//!
//! # What it contains
//!
//! - [`audit_stream`]: commits a final record through a new writer and reads the stream back up to it.
//!
//! # How it works
//!
//! A new writer takes the stream by force. Taking over recovers the tail of the last segment, which settles every
//! append of unknown outcome: it is in the stream or it is not. The writer then appends one final record, whose
//! sequence number is the end of the stream, and steps down. The whole stream, from its low-water mark to that
//! record, is then read in one range read and recorded under a session of its own, which the checker judges
//! separately (`bitacora_sim::check_audit`).
//!
//! Each step is tried again until it works or the time is up. A final record whose append did not succeed is
//! recorded as what it was, and the next attempt appends another: no payload is ever used twice.
//!
//! # Invariants
//!
//! - The session of the audit is recorded only for a read that reached the final record, so a history never holds
//!   half an audit.
//! - A stream is reported as audited only if it was read back whole. If that cannot be done in time, the caller
//!   gets the reason, and the round is not clean.
//!
//! # See also
//!
//! - `round.rs`: calls the audit once the cluster is healthy and the writers have stopped.
//! - `judge.rs`: hands the session to the checker.
//! - `bitacora-sim/src/audit.rs`: the audit this one is modelled on.

use std::time::Duration;

use bitacora::{BitacoraClient, SequenceNumber, SequenceRange, StreamReader, TakeoverPolicy};
use bitacora_sim::AppendOutcome;
use bytes::Bytes;
use tokio::time::{Instant, sleep};

use crate::workload::SoakedStream;

/// How long the audit waits before it tries a step again.
const RETRY_PAUSE: Duration = Duration::from_secs(1);

/// Reads `stream` back whole and records what was read under a session of its own.
///
/// # Arguments
///
/// * `client` - The client of the cluster under test.
/// * `stream` - The stream to audit. Its writers must have stopped.
/// * `time_limit` - How long the audit may take.
///
/// # Returns
///
/// The reading session under which the whole stream was recorded, for the checker.
///
/// # Errors
///
/// Returns why the stream could not be read back whole, in words, if the time ran out.
pub(crate) async fn audit_stream(
    client: &BitacoraClient,
    stream: &SoakedStream,
    time_limit: Duration,
) -> Result<u64, String> {
    let deadline = Instant::now() + time_limit;
    let final_sequence = commit_final_record(client, stream, deadline).await?;
    read_whole_stream_back(&client.open_reader(&stream.name), stream, final_sequence, deadline).await
}

/// Appends the final record of `stream` through a new writer, trying again until it is committed or `deadline` has
/// passed.
///
/// # Errors
///
/// Returns the last reason no final record was committed.
async fn commit_final_record(
    client: &BitacoraClient,
    stream: &SoakedStream,
    deadline: Instant,
) -> Result<SequenceNumber, String> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let payload = Bytes::from(format!("{}/final/{attempt}", stream.name));
        let failure = match append_through_a_new_writer(client, stream, payload).await {
            Ok(final_sequence) => return Ok(final_sequence),
            Err(failure) => failure,
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "no final record could be committed to {}: {failure}",
                stream.name
            ));
        }
        sleep(RETRY_PAUSE).await;
    }
}

/// Takes `stream` by force, appends `payload`, records the append, and steps down.
///
/// # Errors
///
/// Returns why the stream could not be taken or the record was not committed.
async fn append_through_a_new_writer(
    client: &BitacoraClient,
    stream: &SoakedStream,
    payload: Bytes,
) -> Result<SequenceNumber, String> {
    let writer = match client.open_writer(&stream.name, TakeoverPolicy::Force).await {
        Ok(writer) => writer,
        Err(open_error) => return Err(open_error.to_string()),
    };
    let operation = stream.history.append_invoked(payload.clone());
    let appended = writer.append(payload).await;
    let epoch = writer.epoch();
    // Stepping down seals the segment; a writer that cannot is replaced by the next one that takes the stream.
    let _closed = writer.close().await;
    match appended {
        Ok(sequence) => {
            stream
                .history
                .append_finished(operation, AppendOutcome::Committed { sequence, epoch });
            Ok(sequence)
        }
        Err(append_error) => {
            stream.history.append_finished(operation, AppendOutcome::Unknown);
            Err(append_error.to_string())
        }
    }
}

/// Reads `stream` from its low-water mark to `final_sequence` and records the read, trying again until the read
/// reaches the final record or `deadline` has passed.
///
/// # Errors
///
/// Returns the last reason the stream could not be read back whole.
async fn read_whole_stream_back(
    reader: &StreamReader,
    stream: &SoakedStream,
    final_sequence: SequenceNumber,
    deadline: Instant,
) -> Result<u64, String> {
    loop {
        let failure = match read_up_to(reader, final_sequence).await {
            Ok(whole_stream) => return Ok(record_audit(stream, &whole_stream)),
            Err(failure) => failure,
        };
        if Instant::now() >= deadline {
            return Err(format!("{} could not be read back whole: {failure}", stream.name));
        }
        sleep(RETRY_PAUSE).await;
    }
}

/// What one read of a whole stream returned.
struct WholeStream {
    /// Where the read started: the stream's low-water mark.
    low_water_mark: SequenceNumber,
    /// Every record from there to the final record, in order.
    records: Vec<bitacora::LogRecord>,
}

/// Reads the stream from its low-water mark to `final_sequence`, once.
///
/// # Errors
///
/// Returns why the read failed, or that it ended before the final record.
async fn read_up_to(reader: &StreamReader, final_sequence: SequenceNumber) -> Result<WholeStream, String> {
    let low_water_mark = match reader.low_water_mark().await {
        Ok(low_water_mark) => low_water_mark,
        Err(read_error) => return Err(read_error.to_string()),
    };
    let Ok(whole_range) = SequenceRange::new(low_water_mark, final_sequence) else {
        return Err(format!(
            "the low-water mark {low_water_mark} is past the final record {final_sequence}"
        ));
    };
    let records = match reader.read_range(whole_range).await {
        Ok(records) => records,
        Err(read_error) => return Err(read_error.to_string()),
    };
    match records.last() {
        Some(last_record) if last_record.sequence == final_sequence => Ok(WholeStream {
            low_water_mark,
            records,
        }),
        _ended_early => Err(format!("the read ended before the final record {final_sequence}")),
    }
}

/// Records `whole_stream` in the history of `stream` under a new session, and returns the session.
fn record_audit(stream: &SoakedStream, whole_stream: &WholeStream) -> u64 {
    let session = stream.history.reader_started(whole_stream.low_water_mark);
    for record in &whole_stream.records {
        stream
            .history
            .record_read(session, record.sequence, record.payload.clone());
    }
    session
}

#[cfg(test)]
#[path = "audit_test.rs"]
mod tests;
