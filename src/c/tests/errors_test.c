/*
 * errors_test.c - what the C interface answers when no cluster is involved.
 *
 * Why this file exists
 *
 * The Rust unit tests call the same functions, but from Rust. This program
 * is compiled by a C compiler against the generated header and linked
 * against the library a C program links. It proves that the header declares
 * what the library exports, that the structures have the layout a C compiler
 * gives them, and that a C program gets a status and a message, never a
 * crash, for everything it can get wrong.
 *
 * What it covers
 *
 * The version, the per-thread error message, every function called with null
 * arguments, configurations that can never work, freeing null, and what
 * works against a cluster that is not there.
 *
 * How it works
 *
 * Connecting sends nothing, so a client can be made for an etcd endpoint
 * nobody listens on. Every answer checked here is decided before the first
 * request would be sent, so the program needs no network and finishes at
 * once. The same source is linked twice: against the static library and
 * against the shared one.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "bitacora.h"
#include "checks.h"

/* The release the build gave the library, as text. Set on the command line by the build. */
#ifndef EXPECTED_RELEASE
#error "the build must define EXPECTED_RELEASE"
#endif

/*
 * Programs have these numbers compiled in. A change here breaks every program
 * that was not compiled again, so it must never happen.
 */
_Static_assert(BITACORA_STATUS_OK == 0, "a status changed its number");
_Static_assert(BITACORA_STATUS_INVALID_ARGUMENT == 1, "a status changed its number");
_Static_assert(BITACORA_STATUS_NOT_FOUND == 2, "a status changed its number");
_Static_assert(BITACORA_STATUS_ALREADY_EXISTS == 3, "a status changed its number");
_Static_assert(BITACORA_STATUS_UNAVAILABLE == 4, "a status changed its number");
_Static_assert(BITACORA_STATUS_TIMED_OUT == 5, "a status changed its number");
_Static_assert(BITACORA_STATUS_FENCED == 6, "a status changed its number");
_Static_assert(BITACORA_STATUS_INDETERMINATE == 7, "a status changed its number");
_Static_assert(BITACORA_STATUS_TRUNCATED == 8, "a status changed its number");
_Static_assert(BITACORA_STATUS_NOTHING_NEW == 9, "a status changed its number");
_Static_assert(BITACORA_STATUS_CLOSED == 10, "a status changed its number");
_Static_assert(BITACORA_STATUS_LEASE_HELD == 11, "a status changed its number");
_Static_assert(BITACORA_STATUS_INTERNAL == 12, "a status changed its number");

/*
 * The layout the library reads and writes, as this compiler sees it. The Rust
 * unit tests hold the library to the same numbers.
 */
_Static_assert(sizeof(bitacora_stream_options) == 16, "bitacora_stream_options changed");
_Static_assert(offsetof(bitacora_stream_options, keep_sealed_for_seconds) == 8,
               "bitacora_stream_options changed");
_Static_assert(sizeof(bitacora_stream_description) == 48, "bitacora_stream_description changed");
_Static_assert(offsetof(bitacora_stream_description, materializer_count) == 40,
               "bitacora_stream_description changed");
_Static_assert(offsetof(bitacora_client_options, writer_identity) == 8,
               "bitacora_client_options changed");

/* A cluster configuration that names an etcd endpoint nobody listens on. */
static const char ABSENT_CLUSTER[] = "etcd_endpoints = [\"http://127.0.0.1:1\"]";

/* What every argument check in this file is expected to return. */
#define REFUSED BITACORA_STATUS_INVALID_ARGUMENT

/* This must run before any other call: it looks at a thread that has made none. */
static void thread_that_made_no_call_has_an_empty_message(void)
{
    REQUIRE(bitacora_last_error_message() != NULL);
    REQUIRE_LAST_ERROR("");
}

static void library_is_the_one_the_header_describes(void)
{
    REQUIRE(bitacora_version() != NULL);
    REQUIRE(strcmp(bitacora_version(), EXPECTED_RELEASE) == 0);
    REQUIRE(bitacora_abi_version() == BITACORA_ABI_VERSION);
}

static void connecting_refuses_what_can_never_work(void)
{
    bitacora_client *client = NULL;
    bitacora_client_options options;

    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text(NULL, NULL, &client));
    REQUIRE_LAST_ERROR("configuration_text is null");
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text(ABSENT_CLUSTER, NULL, NULL));
    REQUIRE_LAST_ERROR("client_out is null");
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_file(NULL, NULL, &client));
    REQUIRE_LAST_ERROR("configuration_path is null");

    /* Text that is not TOML. The message says what is wrong with it. */
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text("this is not TOML", NULL, &client));
    REQUIRE(client == NULL);
    REQUIRE(strlen(bitacora_last_error_message()) > 0);

    /* A setting that is misspelled is refused, not ignored. */
    REQUIRE_STATUS(REFUSED,
                   bitacora_client_connect_text("etcd_endpoints = [\"http://127.0.0.1:1\"]\n"
                                                "etcd_key_prefx = \"/staging/\"",
                                                NULL, &client));
    REQUIRE(client == NULL);

    /* A cluster without a metadata store. */
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text("etcd_endpoints = []", NULL, &client));
    REQUIRE(client == NULL);

    /* A file that is not there. */
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_file("/nonexistent/bitacora/cluster.toml",
                                                         NULL, &client));
    REQUIRE(client == NULL);

    /* Options whose size was never set: the usual mistake with a structure like this one. */
    memset(&options, 0, sizeof(options));
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text(ABSENT_CLUSTER, &options, &client));
    REQUIRE_LAST_ERROR("options.struct_size is 0: set it to the size of the structure");

    /* More threads than any client needs. */
    options.struct_size = (uint32_t)sizeof(options);
    options.worker_threads = 100000;
    REQUIRE_STATUS(REFUSED, bitacora_client_connect_text(ABSENT_CLUSTER, &options, &client));
    REQUIRE(client == NULL);
}

/* Connects to the cluster that is not there, with every option set. */
static bitacora_client *connect_to_the_absent_cluster(void)
{
    bitacora_client *client = NULL;
    bitacora_client_options options;

    memset(&options, 0, sizeof(options));
    options.struct_size = (uint32_t)sizeof(options);
    options.worker_threads = 1;
    options.writer_identity = "errors-test";

    REQUIRE_STATUS(BITACORA_STATUS_OK,
                   bitacora_client_connect_text(ABSENT_CLUSTER, &options, &client));
    REQUIRE(client != NULL);
    /* A call that succeeds leaves no message behind, whatever failed before it. */
    REQUIRE_LAST_ERROR("");
    return client;
}

static void writer_functions_refuse_what_they_cannot_use(const bitacora_client *client)
{
    static const uint8_t record[] = {'r', 'e', 'd', 'o'};
    const bitacora_payload batch[] = {{record, sizeof(record)}};
    bitacora_writer *writer = NULL;
    uint64_t first_sequence = 77;
    uint64_t last_sequence = 77;

    REQUIRE_STATUS(REFUSED, bitacora_writer_open(NULL, "orders", BITACORA_TAKEOVER_FORCE, &writer));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_writer_open(client, NULL, BITACORA_TAKEOVER_FORCE, &writer));
    REQUIRE_LAST_ERROR("stream_name is null");
    REQUIRE_STATUS(REFUSED, bitacora_writer_open(client, "orders", BITACORA_TAKEOVER_FORCE, NULL));
    REQUIRE_LAST_ERROR("writer_out is null");
    /* 7 is not a takeover policy. */
    REQUIRE_STATUS(REFUSED, bitacora_writer_open(client, "orders", 7, &writer));
    REQUIRE(writer == NULL);

    REQUIRE_STATUS(REFUSED,
                   bitacora_writer_append(NULL, record, sizeof(record), 0, &first_sequence));
    REQUIRE_LAST_ERROR("writer is null");
    REQUIRE_STATUS(REFUSED, bitacora_writer_append_batch(NULL, batch, 1, 0, &first_sequence,
                                                         &last_sequence));
    REQUIRE_LAST_ERROR("writer is null");
    REQUIRE_STATUS(REFUSED, bitacora_writer_durable_sequence(NULL, &first_sequence));
    REQUIRE_LAST_ERROR("writer is null");
    REQUIRE_STATUS(REFUSED, bitacora_writer_close(NULL));
    REQUIRE_LAST_ERROR("writer is null");

    /* A call that was refused wrote nothing. */
    REQUIRE(first_sequence == 77);
    REQUIRE(last_sequence == 77);
}

static void reader_functions_refuse_what_they_cannot_use(const bitacora_client *client)
{
    bitacora_reader *reader = NULL;
    bitacora_follower *follower = NULL;
    bitacora_records *records = NULL;
    uint64_t low_water_mark = 77;

    REQUIRE_STATUS(REFUSED, bitacora_reader_open(NULL, "orders", &reader));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_open(client, NULL, &reader));
    REQUIRE_LAST_ERROR("stream_name is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_open(client, "orders", NULL));
    REQUIRE_LAST_ERROR("reader_out is null");
    /* Two bytes that are not UTF-8. */
    REQUIRE_STATUS(REFUSED, bitacora_reader_open(client, "\xff\xfe", &reader));
    REQUIRE(reader == NULL);

    REQUIRE_STATUS(REFUSED,
                   bitacora_reader_read_range(NULL, 1, 10, &records, &low_water_mark));
    REQUIRE_LAST_ERROR("reader is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_low_water_mark(NULL, &low_water_mark));
    REQUIRE_LAST_ERROR("reader is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_follow(NULL, 1, &follower));
    REQUIRE_LAST_ERROR("reader is null");
    REQUIRE_STATUS(REFUSED, bitacora_follower_next(NULL, 10, &records, &low_water_mark));
    REQUIRE_LAST_ERROR("follower is null");
    REQUIRE(follower == NULL);

    /* Opening a reader contacts nobody, and a range that is not valid is refused before
     * anything is sent. */
    REQUIRE_STATUS(BITACORA_STATUS_OK, bitacora_reader_open(client, "orders", &reader));
    REQUIRE(reader != NULL);
    REQUIRE_STATUS(REFUSED, bitacora_reader_read_range(reader, 0, 10, &records, &low_water_mark));
    REQUIRE_STATUS(REFUSED, bitacora_reader_read_range(reader, 10, 9, &records, &low_water_mark));
    REQUIRE(records == NULL);
    REQUIRE_STATUS(REFUSED, bitacora_reader_read_range(reader, 1, 10, NULL, NULL));
    REQUIRE_LAST_ERROR("records_out is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_low_water_mark(reader, NULL));
    REQUIRE_LAST_ERROR("low_water_mark_out is null");
    REQUIRE_STATUS(REFUSED, bitacora_reader_follow(reader, 1, NULL));
    REQUIRE_LAST_ERROR("follower_out is null");
    bitacora_reader_free(reader);

    /* The low-water mark is written for a truncation and for nothing else. */
    REQUIRE(low_water_mark == 77);
}

static void record_accessors_refuse_what_they_cannot_use(void)
{
    const uint8_t *payload = NULL;
    size_t payload_length = 77;

    REQUIRE(bitacora_records_count(NULL) == 0);
    REQUIRE_STATUS(REFUSED, bitacora_records_get(NULL, 0, NULL, &payload, &payload_length));
    REQUIRE_LAST_ERROR("records is null");
    REQUIRE(payload == NULL);
    REQUIRE(payload_length == 77);
}

static void stream_functions_refuse_what_they_cannot_use(const bitacora_client *client)
{
    bitacora_stream_options options;
    bitacora_stream_description description;

    REQUIRE_STATUS(REFUSED, bitacora_stream_create(NULL, "orders", NULL));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_create(client, NULL, NULL));
    REQUIRE_LAST_ERROR("stream_name is null");
    memset(&options, 0, sizeof(options));
    REQUIRE_STATUS(REFUSED, bitacora_stream_create(client, "orders", &options));
    REQUIRE_LAST_ERROR("options.struct_size is 0: set it to the size of the structure");
    /* Four replicas cannot confirm a record of a stream that has three. */
    options.struct_size = (uint32_t)sizeof(options);
    options.replica_count = 3;
    options.write_quorum = 4;
    REQUIRE_STATUS(REFUSED, bitacora_stream_create(client, "orders", &options));

    memset(&description, 0, sizeof(description));
    REQUIRE_STATUS(REFUSED, bitacora_stream_describe(NULL, "orders", &description));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_describe(client, NULL, &description));
    REQUIRE_LAST_ERROR("stream_name is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_describe(client, "orders", NULL));
    REQUIRE_LAST_ERROR("description_out is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_describe(client, "orders", &description));
    REQUIRE_LAST_ERROR("description_out.struct_size is 0: set it to the size of the structure");

    REQUIRE_STATUS(REFUSED, bitacora_stream_truncate_before(NULL, "orders", 10));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_truncate_before(client, NULL, 10));
    REQUIRE_LAST_ERROR("stream_name is null");

    REQUIRE_STATUS(REFUSED, bitacora_stream_delete(NULL, "orders"));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_delete(client, NULL));
    REQUIRE_LAST_ERROR("stream_name is null");

    REQUIRE_STATUS(REFUSED, bitacora_stream_register_materializer(NULL, "orders", "pages"));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_register_materializer(client, NULL, "pages"));
    REQUIRE_LAST_ERROR("stream_name is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_register_materializer(client, "orders", NULL));
    REQUIRE_LAST_ERROR("materializer is null");

    REQUIRE_STATUS(REFUSED, bitacora_stream_unregister_materializer(NULL, "orders", "pages"));
    REQUIRE_LAST_ERROR("client is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_unregister_materializer(client, NULL, "pages"));
    REQUIRE_LAST_ERROR("stream_name is null");
    REQUIRE_STATUS(REFUSED, bitacora_stream_unregister_materializer(client, "orders", NULL));
    REQUIRE_LAST_ERROR("materializer is null");
}

static void freeing_null_does_nothing(void)
{
    bitacora_client_free(NULL);
    bitacora_writer_free(NULL);
    bitacora_reader_free(NULL);
    bitacora_follower_free(NULL);
    bitacora_records_free(NULL);
}

int main(void)
{
    bitacora_client *client;

    thread_that_made_no_call_has_an_empty_message();
    library_is_the_one_the_header_describes();
    connecting_refuses_what_can_never_work();

    client = connect_to_the_absent_cluster();
    writer_functions_refuse_what_they_cannot_use(client);
    reader_functions_refuse_what_they_cannot_use(client);
    record_accessors_refuse_what_they_cannot_use();
    stream_functions_refuse_what_they_cannot_use(client);
    bitacora_client_free(client);

    freeing_null_does_nothing();

    printf("errors_test: every check passed\n");
    return 0;
}
