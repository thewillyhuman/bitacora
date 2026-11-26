/*
 * checks.h - the few lines of test framework the C tests of bitacora share.
 *
 * Why this file exists
 *
 * The C tests are plain programs. Each one has to stop at the first thing
 * that is not as expected and say where it was and what the library last
 * reported. That is all a test of a blocking C interface needs, and it is
 * too little to justify a dependency.
 *
 * What it contains
 *
 * REQUIRE, REQUIRE_STATUS and REQUIRE_LAST_ERROR.
 *
 * How it works
 *
 * A requirement that does not hold prints one line to standard error, with
 * the file, the line and the library's last error message, and ends the
 * program with exit code 1. Nothing is cleaned up: the operating system
 * reclaims what the program still held, and a test that failed has nothing
 * left to prove.
 */

#ifndef BITACORA_TESTS_CHECKS_H
#define BITACORA_TESTS_CHECKS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bitacora.h"

/*
 * Ends the program because the requirement at `file`:`line` does not hold.
 * `what` says which requirement it was.
 */
static inline void fail_requirement(const char *file, int line, const char *what)
{
    fprintf(stderr, "%s:%d: %s (last error: \"%s\")\n", file, line, what,
            bitacora_last_error_message());
    exit(EXIT_FAILURE);
}

/*
 * Ends the program unless `actual_status` is `expected_status`. `call` is the
 * text of the call that returned the status.
 */
static inline void require_status(const char *file, int line, const char *call,
                                  bitacora_status expected_status, bitacora_status actual_status)
{
    if (actual_status != expected_status) {
        fprintf(stderr, "%s:%d: %s returned status %d, expected %d (last error: \"%s\")\n", file,
                line, call, (int)actual_status, (int)expected_status,
                bitacora_last_error_message());
        exit(EXIT_FAILURE);
    }
}

/* Requires that `condition` holds. */
#define REQUIRE(condition)                                                 \
    do {                                                                   \
        if (!(condition)) {                                                \
            fail_requirement(__FILE__, __LINE__, "required: " #condition); \
        }                                                                  \
    } while (0)

/* Requires that `call` returns `expected_status`. The call is made once. */
#define REQUIRE_STATUS(expected_status, call) \
    require_status(__FILE__, __LINE__, #call, (expected_status), (call))

/* Requires that this thread's last error message is exactly `expected_message`. */
#define REQUIRE_LAST_ERROR(expected_message) \
    REQUIRE(strcmp(bitacora_last_error_message(), (expected_message)) == 0)

#endif /* BITACORA_TESTS_CHECKS_H */
