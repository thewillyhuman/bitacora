/*
 * end_to_end.c - what a database does with a stream, against a real cluster.
 *
 *     end_to_end <cluster configuration file>
 *
 * Why this file exists
 *
 * The other tests of the C interface stop where the network begins. This
 * program goes through the life of a stream the way a database embedding the
 * library would, in C, against etcd, replica daemons and a controller that
 * really run. It is what shows that the statuses a C program decides on
 * (fenced, truncated, nothing new, not found) come out of the real protocol
 * and not only out of a table in the library.
 *
 * What it covers, in the order it happens
 *
 *   1. A stream is created, once, and described.
 *   2. A writer appends one record, then a batch of three: bytes that are not
 *      text, an empty record, and text.
 *   3. The four records are read back and compared byte for byte.
 *   4. A follower at the end of the stream is told there is nothing new, and
 *      then receives the next record when it is appended.
 *   5. A second writer takes the stream. The first one is fenced, and what it
 *      tried to append is not in the log.
 *   6. The owner truncates. Reading below the low-water mark is reported as
 *      truncated, together with the mark.
 *   7. A materializer is registered and unregistered.
 *   8. Everything asked of a stream that does not exist is not found.
 *   9. The writer is closed and the stream is deleted.
 *
 * How it works
 *
 * //src/rust/bitacora-c:c_programs_test starts the cluster, writes its
 * configuration to a file and runs this program with it. A step that is not
 * as expected ends the program with exit code 1 and a line that says where;
 * see checks.h. Only one step waits for something the program does not do
 * itself: the controller moves the low-water mark on its own schedule, so
 * that step polls, with a limit.
 */

/* For nanosleep, which the C standard does not have. */
#define _POSIX_C_SOURCE 200809L

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "bitacora.h"
#include "checks.h"

/* Far longer than any step takes on an idle machine. A step that waits this long has failed. */
#define STEP_TIMEOUT_MILLISECONDS 20000

/* A wait in which no record is expected to arrive. */
#define QUIET_WAIT_MILLISECONDS 300

/* How long to wait between two looks at something the controller is about to change. */
#define POLL_INTERVAL_MILLISECONDS 25

/* The stream the scenario is about. */
static const char STREAM[] = "c-end-to-end";

/* A stream nobody created. */
static const char ABSENT_STREAM[] = "c-end-to-end-absent";

/* Bytes that are not text: a NUL inside, and a byte that is not valid UTF-8. */
static const uint8_t BINARY_RECORD[] = {0x00, 0xff, 0x10, 0x00, 0x7f};

/*
 * The records the scenario commits, in the order of their sequence numbers:
 * record n is at index n - 1. The third one is empty.
 */
static const bitacora_payload RECORDS[] = {
    {(const uint8_t *)"first", 5},
    {BINARY_RECORD, sizeof(BINARY_RECORD)},
    {NULL, 0},
    {(const uint8_t *)"fourth", 6},
    {(const uint8_t *)"fifth", 5},
    {(const uint8_t *)"sixth", 5},
};

/* What the fenced writer tries to append. It must never show up in the log. */
static const uint8_t LOST_RECORD[] = {'l', 'o', 's', 't'};

/* Pauses the calling thread. */
static void sleep_milliseconds(long milliseconds)
{
    struct timespec pause;

    pause.tv_sec = milliseconds / 1000;
    pause.tv_nsec = (milliseconds % 1000) * 1000000L;
    /* A pause cut short by a signal only makes the next look come sooner. */
    (void)nanosleep(&pause, NULL);
}

/* Fills `description_out` with where `stream_name` stands, and requires that this works. */
static void describe(const bitacora_client *client, const char *stream_name,
                     bitacora_stream_description *description_out)
{
    memset(description_out, 0, sizeof(*description_out));
    description_out->struct_size = (uint32_t)sizeof(*description_out);
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_stream_describe(client, stream_name, description_out));
    REQUIRE(description_out->struct_size == sizeof(*description_out));
}

/* Appends record number `sequence` of the scenario and requires that it commits there. */
static void append_record(const bitacora_writer *writer, uint64_t sequence)
{
    const bitacora_payload *record = &RECORDS[sequence - 1];
    uint64_t committed_at = 0;

    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_writer_append(writer, record->bytes, record->length,
                                          STEP_TIMEOUT_MILLISECONDS, &committed_at));
    REQUIRE(committed_at == sequence);
}

/*
 * Requires that `records` holds exactly the scenario's records from
 * `first_sequence` to `last_sequence`, with the bytes they were appended with.
 */
static void require_records(const bitacora_records *records, uint64_t first_sequence,
                            uint64_t last_sequence)
{
    size_t record_count = (size_t)(last_sequence - first_sequence + 1);
    size_t index;

    REQUIRE(records != NULL);
    REQUIRE(bitacora_records_count(records) == record_count);
    for (index = 0; index < record_count; index++) {
        const bitacora_payload *expected = &RECORDS[first_sequence - 1 + index];
        uint64_t sequence = 0;
        const uint8_t *payload = NULL;
        size_t payload_length = 99;

        REQUIRE_STATUS(BITACORA_STATUS_OK,
                       bitacora_records_get(records, index, &sequence, &payload, &payload_length));
        REQUIRE(sequence == first_sequence + index);
        REQUIRE(payload_length == expected->length);
        REQUIRE(payload_length == 0 || memcmp(payload, expected->bytes, payload_length) == 0);
    }
}

/* Reads the scenario's records `first_sequence` to `last_sequence` and requires them as appended. */
static void require_range(const bitacora_reader *reader, uint64_t first_sequence,
                          uint64_t last_sequence)
{
    bitacora_records *records = NULL;

    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_read_range(reader, first_sequence,
                                                                 last_sequence, &records, NULL));
    require_records(records, first_sequence, last_sequence);
    bitacora_records_free(records);
}

/* Step 1. */
static void stream_is_created_once(const bitacora_client *client)
{
    bitacora_stream_options options;
    bitacora_stream_description description;

    memset(&options, 0, sizeof(options));
    options.struct_size = (uint32_t)sizeof(options);
    options.replica_count = 3;
    options.write_quorum = 2;
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_stream_create(client, STREAM, &options));
    REQUIRE_STATUS(BITACORA_STATUS_ALREADY_EXISTS, bitacora_stream_create(client, STREAM, NULL));
    REQUIRE_STATUS(BITACORA_STATUS_INVALID_ARGUMENT,
                   bitacora_stream_create(client, "not/a/name", NULL));

    describe(client, STREAM, &description);
    REQUIRE(description.replica_count == 3);
    REQUIRE(description.write_quorum == 2);
    REQUIRE(description.low_water_mark == 1);
    REQUIRE(description.last_sealed_sequence == 0);
    REQUIRE(description.materializer_count == 0);
    REQUIRE(description.has_writer == 0);
}

/* Step 2. Returns the writer, which holds the stream. */
static bitacora_writer *first_writer_appends(const bitacora_client *client)
{
    bitacora_writer *writer = NULL;
    bitacora_stream_description description;
    uint64_t first_sequence = 0;
    uint64_t last_sequence = 0;
    uint64_t durable_sequence = 0;

    /* Nobody holds the stream, so there is no lease to wait for. */
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_writer_open(client, STREAM, BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY,
                                        &writer));
    REQUIRE(writer != NULL);

    append_record(writer, 1);
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_writer_append_batch(writer, &RECORDS[1], 3, STEP_TIMEOUT_MILLISECONDS,
                                                &first_sequence, &last_sequence));
    REQUIRE(first_sequence == 2);
    REQUIRE(last_sequence == 4);
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_writer_durable_sequence(writer, &durable_sequence));
    REQUIRE(durable_sequence == 4);

    /* A batch without records is refused, and takes no sequence number. */
    REQUIRE_STATUS(BITACORA_STATUS_INVALID_ARGUMENT,
                   bitacora_writer_append_batch(writer, NULL, 0, STEP_TIMEOUT_MILLISECONDS,
                                                &first_sequence, &last_sequence));

    describe(client, STREAM, &description);
    REQUIRE(description.has_writer == 1);
    REQUIRE(description.segment_count == 1);
    REQUIRE(description.epoch >= 1);
    return writer;
}

/* Step 3. Returns the reader, which the later steps read with. */
static bitacora_reader *records_are_read_back(const bitacora_client *client)
{
    bitacora_reader *reader = NULL;
    bitacora_records *records = NULL;
    const uint8_t *payload = NULL;
    size_t payload_length = 0;
    uint64_t low_water_mark = 0;

    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_open(client, STREAM, &reader));
    require_range(reader, 1, 4);

    /* A range that reaches past the end of the stream returns what is there. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_read_range(reader, 3, 100, &records, NULL));
    require_records(records, 3, 4);
    /* An index past the last record is refused. */
    REQUIRE_STATUS(BITACORA_STATUS_INVALID_ARGUMENT,
                   bitacora_records_get(records, 2, NULL, &payload, &payload_length));
    bitacora_records_free(records);

    /* A range entirely past the end returns no records, which is not an error. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_read_range(reader, 5, 9, &records, NULL));
    REQUIRE(records != NULL);
    REQUIRE(bitacora_records_count(records) == 0);
    bitacora_records_free(records);

    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_low_water_mark(reader, &low_water_mark));
    REQUIRE(low_water_mark == 1);
    return reader;
}

/* Step 4. */
static void follower_waits_for_the_next_record(const bitacora_reader *reader,
                                               const bitacora_writer *writer)
{
    bitacora_follower *follower = NULL;
    bitacora_records *records = NULL;
    uint64_t low_water_mark = 77;

    /* From the start: 0 and 1 both mean the first record. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_follow(reader, 0, &follower));
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_follower_next(follower, STEP_TIMEOUT_MILLISECONDS, &records, NULL));
    REQUIRE(bitacora_records_count(records) >= 1);
    require_records(records, 1, bitacora_records_count(records));
    bitacora_records_free(records);
    bitacora_follower_free(follower);

    /* At the end of the stream there is nothing to hand out. That is a status, not an error. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_follow(reader, 5, &follower));
    REQUIRE_STATUS(BITACORA_STATUS_NOTHING_NEW,
                   bitacora_follower_next(follower, QUIET_WAIT_MILLISECONDS, &records,
                                          &low_water_mark));
    REQUIRE(records == NULL);
    REQUIRE(low_water_mark == 77);
    REQUIRE(strlen(bitacora_last_error_message()) > 0);

    /* The wait that ended with nothing lost nothing: the next record arrives where it left off. */
    append_record(writer, 5);
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_follower_next(follower, STEP_TIMEOUT_MILLISECONDS, &records, NULL));
    require_records(records, 5, 5);
    bitacora_records_free(records);

    REQUIRE_STATUS(BITACORA_STATUS_NOTHING_NEW,
                   bitacora_follower_next(follower, QUIET_WAIT_MILLISECONDS, &records, NULL));
    bitacora_follower_free(follower);
}

/* Step 5. Frees the first writer and returns the second, which now holds the stream. */
static bitacora_writer *second_writer_fences_the_first(const bitacora_client *client,
                                                       const bitacora_reader *reader,
                                                       bitacora_writer *first_writer)
{
    bitacora_writer *second_writer = NULL;
    bitacora_stream_description before;
    bitacora_stream_description after;
    uint64_t sequence = 77;

    describe(client, STREAM, &before);

    /* The first writer is alive, so only a forced takeover gets the stream at once. */
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_writer_open(client, STREAM, BITACORA_TAKEOVER_FORCE, &second_writer));

    /* The first writer finds out on its next append, and every one after it. */
    REQUIRE_STATUS(BITACORA_STATUS_FENCED,
                   bitacora_writer_append(first_writer, LOST_RECORD, sizeof(LOST_RECORD),
                                          STEP_TIMEOUT_MILLISECONDS, &sequence));
    REQUIRE_STATUS(BITACORA_STATUS_FENCED,
                   bitacora_writer_append(first_writer, LOST_RECORD, sizeof(LOST_RECORD),
                                          STEP_TIMEOUT_MILLISECONDS, &sequence));
    REQUIRE(sequence == 77);
    REQUIRE(strlen(bitacora_last_error_message()) > 0);
    bitacora_writer_free(first_writer);

    /* The second writer continues where the log ends, not where the first writer thought it did. */
    append_record(second_writer, 6);
    require_range(reader, 1, 6);

    describe(client, STREAM, &after);
    REQUIRE(after.epoch > before.epoch);
    REQUIRE(after.segment_count == 2);
    REQUIRE(after.last_sealed_sequence == 5);
    REQUIRE(after.has_writer == 1);
    return second_writer;
}

/* Step 6. */
static void reading_below_the_low_water_mark_is_truncated(const bitacora_client *client,
                                                          const bitacora_reader *reader)
{
    bitacora_follower *follower = NULL;
    bitacora_records *records = NULL;
    bitacora_stream_description description;
    uint64_t low_water_mark = 0;
    long waited_milliseconds = 0;

    /* The owner no longer needs records 1 to 3. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_stream_truncate_before(client, STREAM, 4));

    /* The controller moves the mark on its next pass. */
    for (;;) {
        REQUIRE_STATUS(BITACORA_STATUS_OK,
                       bitacora_reader_low_water_mark(reader, &low_water_mark));
        if (low_water_mark == 4) {
            break;
        }
        REQUIRE(low_water_mark == 1);
        REQUIRE(waited_milliseconds < STEP_TIMEOUT_MILLISECONDS);
        sleep_milliseconds(POLL_INTERVAL_MILLISECONDS);
        waited_milliseconds += POLL_INTERVAL_MILLISECONDS;
    }

    /* What was truncated is reported as such, with the mark, and never as an empty result. */
    low_water_mark = 0;
    REQUIRE_STATUS(BITACORA_STATUS_TRUNCATED,
                   bitacora_reader_read_range(reader, 1, 3, &records, &low_water_mark));
    REQUIRE(records == NULL);
    REQUIRE(low_water_mark == 4);
    REQUIRE(strlen(bitacora_last_error_message()) > 0);

    /* A range that starts below the mark is truncated even if it ends above it. The mark is optional. */
    REQUIRE_STATUS(BITACORA_STATUS_TRUNCATED,
                   bitacora_reader_read_range(reader, 3, 6, &records, NULL));
    REQUIRE(records == NULL);

    /* From the mark on, everything is still there. */
    require_range(reader, 4, 6);

    /* A follower that starts below the mark is told once, and has ended. */
    low_water_mark = 0;
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_follow(reader, 1, &follower));
    REQUIRE_STATUS(BITACORA_STATUS_TRUNCATED,
                   bitacora_follower_next(follower, STEP_TIMEOUT_MILLISECONDS, &records,
                                          &low_water_mark));
    REQUIRE(low_water_mark == 4);
    REQUIRE_STATUS(BITACORA_STATUS_CLOSED,
                   bitacora_follower_next(follower, STEP_TIMEOUT_MILLISECONDS, &records, NULL));
    bitacora_follower_free(follower);

    /* Truncation only moves forward: asking for an earlier position changes nothing. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_stream_truncate_before(client, STREAM, 2));
    describe(client, STREAM, &description);
    REQUIRE(description.low_water_mark == 4);
}

/* Step 7. */
static void materializer_is_registered_and_unregistered(const bitacora_client *client)
{
    bitacora_stream_description description;

    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_stream_register_materializer(client, STREAM, "pages"));
    /* Registering it again changes nothing. */
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_stream_register_materializer(client, STREAM, "pages"));
    REQUIRE_STATUS(BITACORA_STATUS_INVALID_ARGUMENT,
                   bitacora_stream_register_materializer(client, STREAM, "not/a/name"));
    describe(client, STREAM, &description);
    REQUIRE(description.materializer_count == 1);

    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_stream_unregister_materializer(client, STREAM, "pages"));
    /* So does unregistering it again. */
    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_stream_unregister_materializer(client, STREAM, "pages"));
    describe(client, STREAM, &description);
    REQUIRE(description.materializer_count == 0);
}

/* Step 8. */
static void stream_that_does_not_exist_is_not_found(const bitacora_client *client)
{
    bitacora_stream_description description;
    bitacora_writer *writer = NULL;
    bitacora_reader *reader = NULL;
    bitacora_follower *follower = NULL;
    bitacora_records *records = NULL;
    uint64_t low_water_mark = 77;

    memset(&description, 0, sizeof(description));
    description.struct_size = (uint32_t)sizeof(description);
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_stream_describe(client, ABSENT_STREAM, &description));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_stream_truncate_before(client, ABSENT_STREAM, 10));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_stream_register_materializer(client, ABSENT_STREAM, "pages"));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_stream_unregister_materializer(client, ABSENT_STREAM, "pages"));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND, bitacora_stream_delete(client, ABSENT_STREAM));

    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_writer_open(client, ABSENT_STREAM, BITACORA_TAKEOVER_FORCE, &writer));
    REQUIRE(writer == NULL);

    /* Opening a reader contacts nobody. The first read finds out. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_open(client, ABSENT_STREAM, &reader));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_reader_read_range(reader, 1, 10, &records, &low_water_mark));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_reader_low_water_mark(reader, &low_water_mark));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND, bitacora_reader_follow(reader, 1, &follower));
    REQUIRE(follower == NULL);
    REQUIRE(low_water_mark == 77);
    bitacora_reader_free(reader);
}

/* Step 9. Frees the writer. */
static void writer_is_closed_and_the_stream_deleted(const bitacora_client *client,
                                                    const bitacora_reader *reader,
                                                    bitacora_writer *writer)
{
    bitacora_stream_description description;
    bitacora_records *records = NULL;
    uint64_t sequence = 77;

    /* Closing gives the stream up: the next writer would not have to wait or to recover. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_writer_close(writer));
    describe(client, STREAM, &description);
    REQUIRE(description.has_writer == 0);
    REQUIRE(description.last_sealed_sequence == 6);

    /* A closed writer says so, and can only be freed. */
    REQUIRE_STATUS(BITACORA_STATUS_CLOSED,
                   bitacora_writer_append(writer, LOST_RECORD, sizeof(LOST_RECORD),
                                          STEP_TIMEOUT_MILLISECONDS, &sequence));
    REQUIRE_STATUS(BITACORA_STATUS_CLOSED, bitacora_writer_close(writer));
    bitacora_writer_free(writer);

    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_stream_delete(client, STREAM));
    memset(&description, 0, sizeof(description));
    description.struct_size = (uint32_t)sizeof(description);
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_stream_describe(client, STREAM, &description));
    REQUIRE_STATUS(BITACORA_STATUS_NOT_FOUND,
                   bitacora_reader_read_range(reader, 4, 6, &records, NULL));
}

int main(int argument_count, char **arguments)
{
    bitacora_client *client = NULL;
    bitacora_reader *reader;
    bitacora_writer *first_writer;
    bitacora_writer *second_writer;

    if (argument_count != 2) {
        fprintf(stderr, "usage: %s <cluster configuration file>\n", arguments[0]);
        return 2;
    }
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_client_connect_file(arguments[1], NULL, &client));

    stream_is_created_once(client);
    first_writer = first_writer_appends(client);
    reader = records_are_read_back(client);
    follower_waits_for_the_next_record(reader, first_writer);
    second_writer = second_writer_fences_the_first(client, reader, first_writer);
    reading_below_the_low_water_mark_is_truncated(client, reader);
    materializer_is_registered_and_unregistered(client);
    stream_that_does_not_exist_is_not_found(client);
    writer_is_closed_and_the_stream_deleted(client, reader, second_writer);

    bitacora_reader_free(reader);
    bitacora_client_free(client);

    printf("end_to_end: every step passed\n");
    return 0;
}
