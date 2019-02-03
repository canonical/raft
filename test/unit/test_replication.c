#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/replication.h"
#include "../../src/state.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/raft.h"

/**
 * Helpers
 */

struct fixture
{
    TEST_RAFT_FIXTURE_FIELDS;
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    TEST_RAFT_FIXTURE_SETUP(f);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    TEST_RAFT_FIXTURE_TEAR_DOWN(f);

    free(f);
}

/**
 * Transition the state of the raft instance to RAFT_LEADER.
 */
#define __convert_to_leader(F)                                  \
    {                                                           \
        int rv;                                                 \
                                                                \
        rv = raft_state__convert_to_candidate(&F->raft);        \
        munit_assert_int(rv, ==, 0);                            \
                                                                \
        rv = raft_state__convert_to_leader(&F->raft);           \
        munit_assert_int(rv, ==, 0);                            \
                                                                \
        munit_assert_int(F->raft.state, ==, RAFT_STATE_LEADER); \
                                                                \
        raft_io_stub_flush(&F->io);                             \
    }

/**
 * Append an entry to the log.
 */
#define __append_entry(F)                                                     \
    {                                                                         \
        struct raft_buffer buf;                                               \
        int rv;                                                               \
                                                                              \
        buf.len = 8;                                                          \
        buf.base = raft_malloc(buf.len);                                      \
        munit_assert_ptr_not_null(buf.base);                                  \
                                                                              \
        rv = raft_log__append(&F->raft.log, 1, RAFT_LOG_COMMAND, &buf, NULL); \
        munit_assert_int(rv, ==, 0);                                          \
    }

/**
 * raft_replication__send_append_entries
 */

static char *send_ae_oom_heap_fault_delay[] = {"0", NULL};
static char *send_ae_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_ae_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_ae_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_ae_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
static MunitResult test_send_ae_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    test_heap_fault_enable(&f->heap);

    i = raft_configuration__index(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
static MunitResult test_send_ae_io_err(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    raft_io_stub_fault(&f->io, 0, 1);

    i = raft_configuration__index(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* Send the second log entry. */
static MunitResult test_send_ae_second_entry(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    i = raft_configuration__index(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, 0);

    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}

static MunitTest send_append_entries_tests[] = {
    {"/oom", test_send_ae_oom, setup, tear_down, 0, send_ae_oom_params},
    {"/io-err", test_send_ae_io_err, setup, tear_down, 0, NULL},
    {"/second-entry", test_send_ae_second_entry, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_replication__trigger
 */

/* A failure occurs upon submitting the I/O request for a particular server, the
 * I/O requests for other servers are still submitted. */
static MunitResult test_trigger_io_err(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

    __convert_to_leader(f);
    __append_entry(f);

    raft_io_stub_fault(&f->io, 0, 1);

    raft_replication__trigger(&f->raft, 0);

    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}

static MunitTest trigger_tests[] = {
    {"/io-err", test_trigger_io_err, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_replication_suites[] = {
    {"/send-append-entries", send_append_entries_tests, NULL, 1, 0},
    {"/trigger", trigger_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
