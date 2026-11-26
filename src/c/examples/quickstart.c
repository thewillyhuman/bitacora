/*
 * quickstart.c - the smallest complete program that uses bitacora from C.
 *
 * It connects to a cluster, creates a stream, becomes its writer, appends
 * three records, and reads them back.
 *
 *     quickstart <cluster configuration file> [stream name]
 *
 * It exits with 0 if all of that worked, with 1 if a step failed, after
 * saying which one and why, and with 2 if it was called the wrong way.
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "bitacora.h"

/* The longest an append may take before the program stops waiting for it. */
#define APPEND_TIMEOUT_MILLISECONDS 5000

/* Says which step failed and why. The status decides; the message is for people. */
static void report_failure(const char *step, bitacora_status status)
{
    fprintf(stderr, "%s failed with status %d: %s\n", step, (int)status,
            bitacora_last_error_message());
}

int main(int argument_count, char **arguments)
{
    static const char *const messages[] = {"first", "second", "third"};
    const size_t message_count = sizeof(messages) / sizeof(messages[0]);
    const char *stream_name = "quickstart";
    bitacora_client *client = NULL;
    bitacora_writer *writer = NULL;
    bitacora_reader *reader = NULL;
    bitacora_records *records = NULL;
    bitacora_status status;
    uint64_t first_appended = 0;
    uint64_t last_appended = 0;
    int exit_code = 1;
    size_t index;

    if (argument_count < 2 || argument_count > 3) {
        fprintf(stderr, "usage: %s <cluster configuration file> [stream name]\n", arguments[0]);
        return 2;
    }
    if (argument_count == 3) {
        stream_name = arguments[2];
    }

    /* One client per process is enough. It starts its own threads and sends nothing yet. */
    status = bitacora_client_connect_file(arguments[1], NULL, &client);
    if (status != BITACORA_STATUS_OK) {
        report_failure("connecting", status);
        goto cleanup;
    }

    /* A stream is created once. A second run finds it there, which is fine. */
    status = bitacora_stream_create(client, stream_name, NULL);
    if (status != BITACORA_STATUS_OK && status != BITACORA_STATUS_ALREADY_EXISTS) {
        report_failure("creating the stream", status);
        goto cleanup;
    }

    /*
     * A stream has one writer at a time. Opening one waits for the lease of the
     * writer before it to lapse; a writer that was closed has given its lease up.
     */
    status = bitacora_writer_open(client, stream_name, BITACORA_TAKEOVER_WAIT_FOR_LEASE_EXPIRY,
                                  &writer);
    if (status != BITACORA_STATUS_OK) {
        report_failure("opening the writer", status);
        goto cleanup;
    }

    /* An append returns once the record is committed, with its sequence number. */
    for (index = 0; index < message_count; index++) {
        const char *message = messages[index];

        status = bitacora_writer_append(writer, (const uint8_t *)message, strlen(message),
                                        APPEND_TIMEOUT_MILLISECONDS, &last_appended);
        if (status != BITACORA_STATUS_OK) {
            /*
             * FENCED: another writer took the stream, and this one must stop.
             * INDETERMINATE or TIMED_OUT: the record may or may not be in the
             * log. Open a new writer and read the log to find out.
             */
            report_failure("appending", status);
            goto cleanup;
        }
        if (index == 0) {
            first_appended = last_appended;
        }
        printf("appended \"%s\" as record %" PRIu64 "\n", message, last_appended);
    }

    /* Closing seals what was written and gives the stream up for the next writer. */
    status = bitacora_writer_close(writer);
    if (status != BITACORA_STATUS_OK) {
        report_failure("closing the writer", status);
        goto cleanup;
    }

    /* Readers need no writer and do not disturb one. */
    status = bitacora_reader_open(client, stream_name, &reader);
    if (status != BITACORA_STATUS_OK) {
        report_failure("opening the reader", status);
        goto cleanup;
    }
    status = bitacora_reader_read_range(reader, first_appended, last_appended, &records, NULL);
    if (status != BITACORA_STATUS_OK) {
        report_failure("reading", status);
        goto cleanup;
    }
    for (index = 0; index < bitacora_records_count(records); index++) {
        uint64_t sequence = 0;
        const uint8_t *payload = NULL;
        size_t payload_length = 0;

        status = bitacora_records_get(records, index, &sequence, &payload, &payload_length);
        if (status != BITACORA_STATUS_OK) {
            report_failure("looking at a record", status);
            goto cleanup;
        }
        /* The payload is bytes, not a string: it has a length and no terminator. */
        printf("record %" PRIu64 ": %.*s\n", sequence, (int)payload_length, (const char *)payload);
    }
    exit_code = 0;

cleanup:
    /* Every handle is freed exactly once. Freeing a null pointer does nothing. */
    bitacora_records_free(records);
    bitacora_reader_free(reader);
    bitacora_writer_free(writer);
    bitacora_client_free(client);
    return exit_code;
}
