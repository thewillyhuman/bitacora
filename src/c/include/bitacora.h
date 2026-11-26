/*
 * bitacora.h - the C interface of the bitacora client.
 *
 * This file is generated from src/rust/bitacora-c. Do not edit it: change the
 * Rust source and run hack/update_c_header.sh. docs/clients/c.md explains how
 * to build against the library and has a complete example.
 *
 * The rules that hold for every function:
 *
 * Blocking. Every call blocks the calling thread until it has an answer. The
 * work is done on threads the client owns, and the library never calls back
 * into the caller's code.
 *
 * Status. Every call that can fail returns a bitacora_status. Anything other
 * than BITACORA_STATUS_OK leaves a message for people in
 * bitacora_last_error_message(), per thread. Decide on the status, log the
 * message.
 *
 * Ownership. A handle the library returns belongs to the caller until the
 * caller passes it to the matching _free function, exactly once. Text and
 * bytes the caller passes in are copied before the call returns and need not
 * outlive it. Pointers the library returns into its own memory say for how
 * long they are valid.
 *
 * Threads. A client and a reader may be used by any number of threads at
 * once. A writer may be appended to by any number of threads at once; close
 * and free need it to themselves. A follower serves one call at a time.
 * Records may be read by any number of threads at once. Freeing a handle
 * needs that handle to itself.
 *
 * Processes. A client must not be used in a child process after fork(): the
 * child has the handles and none of the threads behind them. Make no call on
 * an inherited handle, not even to free it, and connect again in the child.
 *
 * Signals. The library installs no signal handlers and changes no signal
 * masks. Every thread it starts descends from the thread that connects and
 * starts with the signal mask that thread has while it connects: block
 * signals around the connect if handlers must not run on those threads. A
 * signal does not interrupt a call; give calls a timeout instead. No function
 * here may be called from a signal handler.
 *
 * Arguments. A required pointer that is null, and text that is not valid
 * UTF-8, are reported as BITACORA_STATUS_INVALID_ARGUMENT; nothing is
 * dereferenced. A pointer that is not null must point to what the function
 * says, for the whole call: that cannot be checked.
 *
 * Structures that have a struct_size field: set it to sizeof() of the
 * structure before passing it, and zero the fields that are not needed. The
 * size is what lets a later version add fields without breaking programs
 * compiled against this header.
 *
 * In the description of a function, "Safety" lists what the caller must
 * guarantee and the library cannot check.
 */

#ifndef BITACORA_H
#define BITACORA_H

#include <stddef.h>
#include <stdint.h>

// The version of the C interface this header describes. Compare it with what `bitacora_abi_version`
// returns to make sure the library that was loaded is the one the program was compiled for.
#define BITACORA_ABI_VERSION 1

// Pass this as a timeout to wait for as long as the call takes.
#define BITACORA_WAIT_WITHOUT_LIMIT 0

// Takeover policy: wait for the previous writer's lease to lapse before taking the stream. This is the
// safe default: it cannot take a stream from a writer that is alive and well.
#define BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY 0

// Takeover policy: take the stream at once. Use it when the caller already knows that the previous
// writer is gone, for example because its own failover logic decided so.
#define BITACORA_TAKEOVER_FORCE 1

// What a call of the C interface resulted in.
//
// Every function that can fail returns one of these. Anything other than `BITACORA_STATUS_OK` leaves a
// message for people in `bitacora_last_error_message`. The numbers are stable: a status keeps its number
// in every later version, and new ones are added at the end.
typedef enum bitacora_status {
    // The call did what it was asked.
    BITACORA_STATUS_OK = 0,
    // An argument cannot be used: a null pointer where one is required, text that is not UTF-8, a range
    // that ends before it starts, a configuration that can never work, a name that is not allowed, a
    // record that is too large. Nothing was done. Calling again with the same arguments fails again.
    BITACORA_STATUS_INVALID_ARGUMENT = 1,
    // The stream, or the materializer, does not exist.
    BITACORA_STATUS_NOT_FOUND = 2,
    // A stream with this name exists already.
    BITACORA_STATUS_ALREADY_EXISTS = 3,
    // The cluster could not do it right now: the metadata store, the replicas or the object store could
    // not be reached, or too few of them answered. For an append this status is never returned; an append
    // whose outcome could not be learned is `BITACORA_STATUS_INDETERMINATE`. For every other call, nothing
    // was changed or the change is safe to repeat: try again later.
    BITACORA_STATUS_UNAVAILABLE = 4,
    // The wait the caller allowed has passed. For an append this means the outcome is not known YET: the
    // record keeps its place and may still be committed. Treat it like `BITACORA_STATUS_INDETERMINATE`.
    BITACORA_STATUS_TIMED_OUT = 5,
    // Another writer has taken the stream. This writer must stop: every later append fails the same way.
    // A record whose append returned this may or may not have been committed.
    BITACORA_STATUS_FENCED = 6,
    // The outcome of the append is unknown: the record may or may not have been committed. The log
    // decides. Open a new writer, which recovers the stream, and read the log to find out.
    BITACORA_STATUS_INDETERMINATE = 7,
    // The records asked for were deleted. The call reports the low-water mark, the first sequence number
    // that can still be read.
    BITACORA_STATUS_TRUNCATED = 8,
    // Not an error: no record arrived within the wait the caller allowed. Call again to keep following.
    BITACORA_STATUS_NOTHING_NEW = 9,
    // The handle was closed, or the follower has ended after reporting why. Free it.
    BITACORA_STATUS_CLOSED = 10,
    // A healthy writer still holds the stream's lease and the caller chose to wait for it to lapse. Try
    // again later, or take the stream by force if the old writer is known to be gone.
    BITACORA_STATUS_LEASE_HELD = 11,
    // A defect in bitacora itself: a panic that was caught at the boundary, or a stored record that
    // cannot be decoded. The handle the call was made on should be freed.
    BITACORA_STATUS_INTERNAL = 12,
} bitacora_status;

// A connection to one bitacora cluster.
//
// Every function that takes a client may be called from several threads at once with the same client.
typedef struct bitacora_client bitacora_client;

// A position in a stream that follows the records as they commit.
//
// A follower is for one thread at a time: calls from several threads take turns, each continuing where
// the one before stopped.
typedef struct bitacora_follower bitacora_follower;

// A reader of one stream.
//
// Every function that takes a reader may be called from several threads at once with the same reader.
typedef struct bitacora_reader bitacora_reader;

// Records returned by one read.
//
// The accessors may be called from several threads at once with the same handle. Freeing needs the
// handle to itself.
typedef struct bitacora_records bitacora_records;

// The writer of one stream.
//
// Appending and reading the durable sequence may be done from several threads at once with the same
// writer. Closing and freeing must be done by one thread while no other call on the writer is running.
typedef struct bitacora_writer bitacora_writer;

// How a client runs. Pass a null pointer to take every default.
//
// Set `struct_size` to `sizeof(bitacora_client_options)` and every field that is not wanted to zero. The
// size lets a later version add fields without breaking programs compiled against this one.
typedef struct bitacora_client_options {
    // `sizeof(bitacora_client_options)`, as the caller's compiler knows the structure.
    uint32_t struct_size;
    // How many threads run the client's work in the background. 0 takes the default, which is 2.
    uint32_t worker_threads;
    // A name for this program's writers, recorded with their lease so that an operator can see who holds
    // a stream. A NUL-terminated UTF-8 string; it is copied, so it need not outlive the call. Null takes
    // the default, `bitacora-writer`.
    const char *writer_identity;
} bitacora_client_options;

// One record of a batch: where its bytes are and how many there are.
typedef struct bitacora_payload {
    // The first byte of the record. May be null if `length` is 0.
    const uint8_t *bytes;
    // How many bytes the record has. A record may be empty.
    size_t length;
} bitacora_payload;

// How a new stream is replicated and how long it keeps records nothing needs. Pass a null pointer to
// take every default: three replicas, two of which must store a record before it is committed, and no
// minimum retention.
//
// Set `struct_size` to `sizeof(bitacora_stream_options)` and every field that is not wanted to zero.
typedef struct bitacora_stream_options {
    // `sizeof(bitacora_stream_options)`, as the caller's compiler knows the structure.
    uint32_t struct_size;
    // How many replicas hold each segment of the stream. 0, together with a `write_quorum` of 0, takes
    // the default of 3.
    uint8_t replica_count;
    // How many replicas must store a record before it is committed, from 1 to `replica_count`. 0,
    // together with a `replica_count` of 0, takes the default of 2.
    uint8_t write_quorum;
    // Not used. Set to 0.
    uint8_t reserved[2];
    // Keep every sealed segment at least this many seconds, for point-in-time recovery. 0 keeps nothing
    // longer than truncation allows.
    uint64_t keep_sealed_for_seconds;
} bitacora_stream_options;

// Where a stream stands: a snapshot of what the metadata store knows about it.
//
// Set `struct_size` to `sizeof(bitacora_stream_description)` before the call. The library fills the
// fields that fit and sets `struct_size` to the number of bytes it wrote.
typedef struct bitacora_stream_description {
    // In: `sizeof(bitacora_stream_description)`. Out: how many bytes were filled.
    uint32_t struct_size;
    // How many segments the stream has, the open one included.
    uint32_t segment_count;
    // The stream's identifier. It is never reused, not even by a later stream of the same name.
    uint64_t stream_id;
    // The stream's epoch: how many times a writer has taken it.
    uint64_t epoch;
    // The first sequence number that can still be read.
    uint64_t low_water_mark;
    // The last sequence number in a sealed segment, or 0 if no segment is sealed. Records after it are
    // in the open segment; how many of those are committed is known to the writer and the replicas, not
    // to the metadata store.
    uint64_t last_sealed_sequence;
    // How many materializers are registered. Each one keeps the log from being truncated past its
    // checkpoint.
    uint32_t materializer_count;
    // How many replicas hold each segment.
    uint8_t replica_count;
    // How many replicas must store a record before it is committed.
    uint8_t write_quorum;
    // 1 if a writer holds the stream's lease, 0 if not. A lease that has lapsed counts as held until the
    // next writer replaces it.
    uint8_t has_writer;
    // Not used. Always 0.
    uint8_t reserved;
} bitacora_stream_description;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

// Returns the message that explains why the last call on this thread did not return
// `BITACORA_STATUS_OK`.
//
// The message is for people and logs: it names the stream, the replica or the limit involved. A program
// decides what to do from the status alone.
//
// # Returns
//
// A NUL-terminated UTF-8 string, never null. It is empty if the last call on this thread succeeded, or
// if this thread has made no call. The string belongs to the library: do not free it. It stays valid
// until this thread calls any function of this library again, so copy it if it is needed for longer.
//
// This function itself does not change the message.
const char *bitacora_last_error_message(void);

// Returns the release of the library, for example `1.4.0`.
//
// # Returns
//
// A NUL-terminated string that is never null and stays valid for as long as the library is loaded. Do
// not free it.
const char *bitacora_version(void);

// Returns the version of the C interface the library implements.
//
// # Returns
//
// The number that `BITACORA_ABI_VERSION` had in the header the library was built from. A program
// compiled against a header with the same number can use this library.
uint32_t bitacora_abi_version(void);

// Connects to the cluster described by a configuration file.
//
// The file is the cluster configuration every bitacora program reads, in TOML: the etcd endpoints, the
// key prefix and the object store. For example:
//
// ```toml
// etcd_endpoints = ["http://10.0.0.1:2379", "http://10.0.0.2:2379", "http://10.0.0.3:2379"]
//
// [object_store]
// kind = "local_directory"
// path = "/var/lib/bitacora/objects"
// ```
//
// Connecting starts the client's threads and sends nothing: a cluster that is down is not an error
// here. Calls fail with `BITACORA_STATUS_UNAVAILABLE` until it is back.
//
// # Arguments
//
// * `configuration_path` - The path of the configuration file, NUL-terminated UTF-8.
// * `options` - How the client runs, or null for every default.
// * `client_out` - Where the new client is written. It is set to null if the call fails. Free the client
//   with `bitacora_client_free`.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the file cannot be read, it is not a valid
//   cluster configuration, or an option is out of range.
// * `BITACORA_STATUS_UNAVAILABLE` - The threads could not be started, or the object store could not be
//   opened.
//
// # Safety
//
// `configuration_path` must be null or a NUL-terminated string. `options` must be null or point to a
// `bitacora_client_options` with `struct_size` set. `client_out` must be null or point to a pointer that
// can be written.
enum bitacora_status bitacora_client_connect_file(const char *configuration_path,
                                                  const struct bitacora_client_options *options,
                                                  struct bitacora_client **client_out);

// Connects to the cluster described by configuration text.
//
// The same as `bitacora_client_connect_file`, for a program that keeps its settings elsewhere and builds
// the TOML itself. The smallest configuration is one line:
//
// ```toml
// etcd_endpoints = ["http://127.0.0.1:2379"]
// ```
//
// # Arguments
//
// * `configuration_text` - The cluster configuration in TOML, NUL-terminated UTF-8. It is copied.
// * `options` - How the client runs, or null for every default.
// * `client_out` - Where the new client is written. It is set to null if the call fails. Free the client
//   with `bitacora_client_free`.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the text is not a valid cluster
//   configuration, or an option is out of range.
// * `BITACORA_STATUS_UNAVAILABLE` - The threads could not be started, or the object store could not be
//   opened.
//
// # Safety
//
// `configuration_text` must be null or a NUL-terminated string. `options` must be null or point to a
// `bitacora_client_options` with `struct_size` set. `client_out` must be null or point to a pointer that
// can be written.
enum bitacora_status bitacora_client_connect_text(const char *configuration_text,
                                                  const struct bitacora_client_options *options,
                                                  struct bitacora_client **client_out);

// Frees a client.
//
// Writers, readers and followers opened from it keep working: they hold what they need, and the
// client's threads stop when the last of them is freed. A null pointer is ignored.
//
// # Arguments
//
// * `client` - The client to free, or null.
//
// # Safety
//
// `client` must be null or a client returned by this library that has not been freed. No other call may
// be using it, and it must not be used afterwards.
void bitacora_client_free(struct bitacora_client *client);

// Becomes the writer of a stream.
//
// This runs the takeover: it raises the stream's epoch, seals whatever segment the previous writer left
// open, and opens a new one. When it returns, every record the previous writer committed is in the
// stream, the previous writer can commit nothing more, and this writer can append. Whoever opens a
// stream last is its writer; bitacora does not choose which program that should be.
//
// The call blocks while it waits for a lease to lapse (with `BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY`,
// about ten seconds after a writer that did not close) and while it recovers the previous segment.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `takeover_policy` - `BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY` or `BITACORA_TAKEOVER_FORCE`.
// * `writer_out` - Where the new writer is written. It is set to null if the call fails. End the writer
//   with `bitacora_writer_close` and `bitacora_writer_free`.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the name is not UTF-8, or the policy is not
//   one of the two.
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_LEASE_HELD` - A writer that is alive still holds the stream, and the policy was to
//   wait. Try again later.
// * `BITACORA_STATUS_FENCED` - Another program took the stream while this one was opening it. Open again
//   only if this program should still be the writer.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store or too many replicas could not be reached, or the
//   controller has not placed the stream's next segment. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
// `writer_out` must be null or point to a pointer that can be written.
enum bitacora_status bitacora_writer_open(const struct bitacora_client *client,
                                          const char *stream_name,
                                          uint32_t takeover_policy,
                                          struct bitacora_writer **writer_out);

// Appends one record and waits until it is committed.
//
// Committed means stored on a write quorum of replicas and flushed to their disks: the record will be
// seen by every reader, at the sequence number this call reports, and will not be lost. The payload is
// copied before the wait begins.
//
// # Arguments
//
// * `writer` - The writer of the stream.
// * `payload` - The first byte of the record. May be null if `payload_length` is 0.
// * `payload_length` - How many bytes the record has, at most 4 MiB.
// * `timeout_milliseconds` - The longest to wait for the commit, or `BITACORA_WAIT_WITHOUT_LIMIT`.
// * `commit_sequence_out` - Where the record's sequence number is written on success.
//
// # Returns
//
// `BITACORA_STATUS_OK` if the record is committed, or:
//
// * `BITACORA_STATUS_FENCED` - Another writer has the stream. Stop writing. This record may or may not
//   have been committed.
// * `BITACORA_STATUS_INDETERMINATE` - The writer lost contact before it learned the outcome. The record
//   may or may not have been committed; read the log to find out.
// * `BITACORA_STATUS_TIMED_OUT` - The wait ended first. The record is still queued and may be committed
//   later; treat it like `BITACORA_STATUS_INDETERMINATE`.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or the record is larger than 4 MiB. The
//   record was not appended.
// * `BITACORA_STATUS_CLOSED` - The writer was closed. The record was not appended.
//
// # Safety
//
// `writer` must be null or a live writer. `payload` must be null or point to `payload_length` readable
// bytes. `commit_sequence_out` must be null or point to a `uint64_t` that can be written.
enum bitacora_status bitacora_writer_append(const struct bitacora_writer *writer,
                                            const uint8_t *payload,
                                            size_t payload_length,
                                            uint64_t timeout_milliseconds,
                                            uint64_t *commit_sequence_out);

// Appends several records as one unit and waits until they are committed.
//
// The records get consecutive sequence numbers and are committed together or not at all: no reader ever
// sees some of them without the others. They travel and are checksummed as one batch, which is also
// the cheapest way to append many small records. All payloads are copied before the wait begins.
//
// # Arguments
//
// * `writer` - The writer of the stream.
// * `payloads` - The records, in the order they are to be appended.
// * `payload_count` - How many records there are. At least 1.
// * `timeout_milliseconds` - The longest to wait for the commit, or `BITACORA_WAIT_WITHOUT_LIMIT`.
// * `first_sequence_out` - Where the sequence number of the first record is written on success.
// * `last_sequence_out` - Where the sequence number of the last record is written on success.
//
// # Returns
//
// `BITACORA_STATUS_OK` if every record is committed, or a status as for `bitacora_writer_append`, which
// then holds for all the records together. `BITACORA_STATUS_INVALID_ARGUMENT` is also returned for an
// empty batch and for a batch larger than 8 MiB.
//
// # Safety
//
// `writer` must be null or a live writer. `payloads` must be null or point to `payload_count` entries,
// each with `bytes` null or pointing to `length` readable bytes. The two outputs must each be null or
// point to a `uint64_t` that can be written.
enum bitacora_status bitacora_writer_append_batch(const struct bitacora_writer *writer,
                                                  const struct bitacora_payload *payloads,
                                                  size_t payload_count,
                                                  uint64_t timeout_milliseconds,
                                                  uint64_t *first_sequence_out,
                                                  uint64_t *last_sequence_out);

// Reports how far the stream is committed, as this writer knows it.
//
// Every record at or below the reported sequence number is committed. The value never goes back. It is
// 0 while nothing is committed in a new stream.
//
// # Arguments
//
// * `writer` - The writer of the stream.
// * `durable_sequence_out` - Where the sequence number is written on success.
//
// # Returns
//
// `BITACORA_STATUS_OK`, `BITACORA_STATUS_INVALID_ARGUMENT` if a pointer is null, or
// `BITACORA_STATUS_CLOSED` if the writer was closed. The call does not block.
//
// # Safety
//
// `writer` must be null or a live writer. `durable_sequence_out` must be null or point to a `uint64_t`
// that can be written.
enum bitacora_status bitacora_writer_durable_sequence(const struct bitacora_writer *writer,
                                                      uint64_t *durable_sequence_out);

// Closes a writer in good order.
//
// The writer stops accepting appends, waits for the ones it accepted to commit, seals its segment and
// gives up its lease. The next writer can then open the stream at once and without a recovery. Whatever
// this returns, the writer is closed afterwards and must still be freed with `bitacora_writer_free`.
//
// # Arguments
//
// * `writer` - The writer to close.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_FENCED` - Another writer took the stream before this one could close.
// * `BITACORA_STATUS_INDETERMINATE` - Accepted records could not be committed, or the segment could not
//   be sealed, in time. The next writer recovers the stream as after a crash.
// * `BITACORA_STATUS_CLOSED` - The writer was closed before.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - `writer` is null, or other threads still have calls in flight on
//   this writer. In the second case the writer is still open.
//
// # Safety
//
// `writer` must be null or a live writer. No other thread may be inside a call on this writer.
enum bitacora_status bitacora_writer_close(const struct bitacora_writer *writer);

// Frees a writer.
//
// A writer that was not closed is stopped as it is: appends in flight end with
// `BITACORA_STATUS_INDETERMINATE`, its segment stays open and its lease runs out on its own. That is
// what a crash looks like to the cluster, and the next writer recovers from it. A null pointer is
// ignored.
//
// # Arguments
//
// * `writer` - The writer to free, or null.
//
// # Safety
//
// `writer` must be null or a writer returned by this library that has not been freed. No other call may
// be using it, and it must not be used afterwards.
void bitacora_writer_free(struct bitacora_writer *writer);

// Reports how many records a read returned.
//
// # Arguments
//
// * `records` - The records of one read, or null.
//
// # Returns
//
// The number of records, 0 for a null pointer. Valid indexes for `bitacora_records_get` run from 0 to one
// below it.
//
// # Safety
//
// `records` must be null or records returned by this library that have not been freed.
size_t bitacora_records_count(const struct bitacora_records *records);

// Lends out one record of a read: its sequence number and its bytes.
//
// Nothing is copied. The payload pointer leads into memory the records own.
//
// # Arguments
//
// * `records` - The records of one read.
// * `record_index` - Which record: 0 for the first, up to one below `bitacora_records_count`.
// * `sequence_out` - Where the record's sequence number is written, or null if it is not wanted.
// * `payload_out` - Where the pointer to the record's first byte is written. It stays valid until
//   `bitacora_records_free`. Do not write through it, and do not read through it if the length is 0.
// * `payload_length_out` - Where the number of bytes of the record is written.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or `BITACORA_STATUS_INVALID_ARGUMENT` if a required pointer is null or the index
// is past the last record.
//
// # Safety
//
// `records` must be null or records returned by this library that have not been freed. Each output must
// be null or point to memory of its type that can be written.
enum bitacora_status bitacora_records_get(const struct bitacora_records *records,
                                          size_t record_index,
                                          uint64_t *sequence_out,
                                          const uint8_t **payload_out,
                                          size_t *payload_length_out);

// Frees the records of a read. Every payload pointer obtained from them becomes invalid. A null pointer
// is ignored.
//
// # Arguments
//
// * `records` - The records to free, or null.
//
// # Safety
//
// `records` must be null or records returned by this library that have not been freed. No other call may
// be using them, and neither they nor any pointer obtained from them may be used afterwards.
void bitacora_records_free(struct bitacora_records *records);

// Opens a reader of a stream.
//
// Opening contacts nobody and cannot tell whether the stream exists; the first read does. Any number of
// readers may be open on a stream, in any number of programs.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `reader_out` - Where the new reader is written. It is set to null if the call fails. Free the reader
//   with `bitacora_reader_free`.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or `BITACORA_STATUS_INVALID_ARGUMENT` if a pointer is null or the name is not
// UTF-8.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
// `reader_out` must be null or point to a pointer that can be written.
enum bitacora_status bitacora_reader_open(const struct bitacora_client *client,
                                          const char *stream_name,
                                          struct bitacora_reader **reader_out);

// Reads the committed records of a range of sequence numbers.
//
// The result holds the records from `first_sequence` to `last_sequence`, in order. It holds fewer only
// if the stream does not reach `last_sequence` yet: then it ends at the last committed record, and may
// be empty. Only committed records are ever returned.
//
// # Arguments
//
// * `reader` - The reader of the stream.
// * `first_sequence` - The first sequence number wanted, 1 or more.
// * `last_sequence` - The last sequence number wanted, not below `first_sequence`.
// * `records_out` - Where the records are written on success. It is set to null if the call fails. Free
//   them with `bitacora_records_free`.
// * `low_water_mark_out` - Where the low-water mark is written if the status is
//   `BITACORA_STATUS_TRUNCATED`: the first sequence number that can still be read. May be null.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_TRUNCATED` - The range starts below the low-water mark; those records were deleted.
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store, or every place that holds the records, could not
//   be reached. Try again later.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A required pointer is null, or the range is not valid.
//
// # Safety
//
// `reader` must be null or a live reader. `records_out` must be null or point to a pointer that can be
// written. `low_water_mark_out` must be null or point to a `uint64_t` that can be written.
enum bitacora_status bitacora_reader_read_range(const struct bitacora_reader *reader,
                                                uint64_t first_sequence,
                                                uint64_t last_sequence,
                                                struct bitacora_records **records_out,
                                                uint64_t *low_water_mark_out);

// Reports the first sequence number of the stream that can still be read.
//
// Everything below it has been truncated. It is 1 for a stream that was never truncated, and it never
// goes back.
//
// # Arguments
//
// * `reader` - The reader of the stream.
// * `low_water_mark_out` - Where the sequence number is written on success.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
//
// # Safety
//
// `reader` must be null or a live reader. `low_water_mark_out` must be null or point to a `uint64_t`
// that can be written.
enum bitacora_status bitacora_reader_low_water_mark(const struct bitacora_reader *reader,
                                                    uint64_t *low_water_mark_out);

// Starts following a stream from a sequence number.
//
// The follower hands out every committed record from `from_sequence` on, through
// `bitacora_follower_next`: first the ones that exist already, then new ones as they commit. It keeps
// going across segment changes and writer changes.
//
// # Arguments
//
// * `reader` - The reader of the stream.
// * `from_sequence` - The first sequence number wanted. 0 and 1 both mean the first record.
// * `follower_out` - Where the new follower is written. It is set to null if the call fails. Free the
//   follower with `bitacora_follower_free`.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
//
// # Safety
//
// `reader` must be null or a live reader. `follower_out` must be null or point to a pointer that can be
// written.
enum bitacora_status bitacora_reader_follow(const struct bitacora_reader *reader,
                                            uint64_t from_sequence,
                                            struct bitacora_follower **follower_out);

// Frees a reader. Followers started from it keep working. A null pointer is ignored.
//
// # Arguments
//
// * `reader` - The reader to free, or null.
//
// # Safety
//
// `reader` must be null or a reader returned by this library that has not been freed. No other call may
// be using it, and it must not be used afterwards.
void bitacora_reader_free(struct bitacora_reader *reader);

// Waits for the next committed records of a followed stream and moves the follower past them.
//
// On success the result holds at least one record: the next one after the follower's position, and
// whatever arrived with it. A wait that ends with nothing loses nothing; the next call continues from
// the same position.
//
// # Arguments
//
// * `follower` - The follower.
// * `timeout_milliseconds` - The longest to wait for a record, or `BITACORA_WAIT_WITHOUT_LIMIT`.
// * `records_out` - Where the records are written on success. It is set to null otherwise. Free them
//   with `bitacora_records_free`.
// * `low_water_mark_out` - Where the low-water mark is written if the status is
//   `BITACORA_STATUS_TRUNCATED`. May be null.
//
// # Returns
//
// `BITACORA_STATUS_OK` with records, or:
//
// * `BITACORA_STATUS_NOTHING_NEW` - No record arrived within the wait. Not an error: call again.
// * `BITACORA_STATUS_TRUNCATED` - The follower's position was truncated before it read it. The follower
//   has ended; start a new one at the low-water mark.
// * `BITACORA_STATUS_NOT_FOUND` - The stream was deleted. The follower has ended.
// * `BITACORA_STATUS_CLOSED` - The follower ended in an earlier call. Free it.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A required pointer is null.
//
// # Safety
//
// `follower` must be null or a live follower. `records_out` must be null or point to a pointer that can
// be written. `low_water_mark_out` must be null or point to a `uint64_t` that can be written.
enum bitacora_status bitacora_follower_next(const struct bitacora_follower *follower,
                                            uint64_t timeout_milliseconds,
                                            struct bitacora_records **records_out,
                                            uint64_t *low_water_mark_out);

// Frees a follower. A null pointer is ignored.
//
// # Arguments
//
// * `follower` - The follower to free, or null.
//
// # Safety
//
// `follower` must be null or a follower returned by this library that has not been freed. No other call
// may be using it, and it must not be used afterwards.
void bitacora_follower_free(struct bitacora_follower *follower);

// Creates a stream.
//
// The stream is empty and has no writer. Its first segment is placed on replicas at once if enough are
// registered, and by the controller as soon as there are, so a writer can usually open it right away.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the new stream: 1 to 255 ASCII letters, digits, `.`, `_` or `-`,
//   NUL-terminated.
// * `options` - How the stream is replicated and retained, or null for every default.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_ALREADY_EXISTS` - A stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, the name is not allowed, or the options do
//   not go together.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
// `options` must be null or point to a `bitacora_stream_options` with `struct_size` set.
enum bitacora_status bitacora_stream_create(const struct bitacora_client *client,
                                            const char *stream_name,
                                            const struct bitacora_stream_options *options);

// Reports where a stream stands.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `description_out` - The structure to fill. Set its `struct_size` to
//   `sizeof(bitacora_stream_description)` before the call.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or `struct_size` was not set.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
// `description_out` must be null or point to a `bitacora_stream_description` with `struct_size` set,
// writable for that many bytes.
enum bitacora_status bitacora_stream_describe(const struct bitacora_client *client,
                                              const char *stream_name,
                                              struct bitacora_stream_description *description_out);

// Allows every record below a sequence number to be deleted.
//
// This is how the owner of a stream says what it no longer needs. Records are deleted by whole segments,
// later, by the controller, and only as far as the checkpoints of the registered materializers and the
// retention of the stream allow. The low-water mark follows; read it with
// `bitacora_reader_low_water_mark`. The position only moves forward: asking for an earlier one changes
// nothing.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `sequence` - The first sequence number that must be kept.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
enum bitacora_status bitacora_stream_truncate_before(const struct bitacora_client *client,
                                                     const char *stream_name,
                                                     uint64_t sequence);

// Deletes a stream and everything in it.
//
// The stream is gone at once for writers and readers. Its segments on replicas and in the object store
// are removed afterwards by the controller. A stream created later under the same name is a new stream.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` must be null or a NUL-terminated string.
enum bitacora_status bitacora_stream_delete(const struct bitacora_client *client,
                                            const char *stream_name);

// Registers a materializer on a stream.
//
// From then on the log keeps every record above the materializer's checkpoint. A registered
// materializer that stops checkpointing therefore keeps the whole log from being truncated, until it is
// unregistered. Registering one that is registered already changes nothing.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `materializer` - The name of the materializer: 1 to 255 ASCII letters, digits, `.`, `_` or `-`,
//   NUL-terminated.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null, or the materializer's name is not allowed.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` and `materializer` must each be null or a
// NUL-terminated string.
enum bitacora_status bitacora_stream_register_materializer(const struct bitacora_client *client,
                                                           const char *stream_name,
                                                           const char *materializer);

// Unregisters a materializer, so that it no longer keeps the log from being truncated.
//
// Unregistering one that is not registered changes nothing.
//
// # Arguments
//
// * `client` - The client of the cluster.
// * `stream_name` - The name of the stream, NUL-terminated UTF-8.
// * `materializer` - The name of the materializer, NUL-terminated UTF-8.
//
// # Returns
//
// `BITACORA_STATUS_OK`, or:
//
// * `BITACORA_STATUS_NOT_FOUND` - No stream has this name.
// * `BITACORA_STATUS_INVALID_ARGUMENT` - A pointer is null.
// * `BITACORA_STATUS_UNAVAILABLE` - The metadata store could not be reached. Try again later.
//
// # Safety
//
// `client` must be null or a live client. `stream_name` and `materializer` must each be null or a
// NUL-terminated string.
enum bitacora_status bitacora_stream_unregister_materializer(const struct bitacora_client *client,
                                                             const char *stream_name,
                                                             const char *materializer);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* BITACORA_H */
