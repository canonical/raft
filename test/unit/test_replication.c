#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/replication.h"
#include "../../src/state.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(replication);

/**
 * Helpers
 */

struct fixture
{
    RAFT_FIXTURE;
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    RAFT_SETUP(f);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    RAFT_TEAR_DOWN(f);

    free(f);
}

/**
 * Transition the state of the raft instance to RAFT_LEADER.
 */
#define __convert_to_leader(F)                            \
    {                                                     \
        int rv;                                           \
                                                          \
        rv = raft_state__convert_to_candidate(&F->raft);  \
        munit_assert_int(rv, ==, 0);                      \
                                                          \
        rv = raft_state__convert_to_leader(&F->raft);     \
        munit_assert_int(rv, ==, 0);                      \
                                                          \
        munit_assert_int(F->raft.state, ==, RAFT_LEADER); \
                                                          \
        raft_io_stub_flush(&F->io);                       \
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
        rv = log__append(&F->raft.log, 1, RAFT_LOG_COMMAND, &buf, NULL); \
        munit_assert_int(rv, ==, 0);                                          \
    }

/**
 * raft_replication__send_append_entries
 */

TEST_SUITE(send_append_entries);

TEST_SETUP(send_append_entries, setup);
TEST_TEAR_DOWN(send_append_entries, tear_down);

TEST_GROUP(send_append_entries, error);
TEST_GROUP(send_append_entries, success);

static char *send_ae_oom_heap_fault_delay[] = {"0", NULL};
static char *send_ae_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_ae_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_ae_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_ae_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory failures. */
TEST_CASE(send_append_entries, error, oom, send_ae_oom_params)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    test_heap_fault_enable(&f->heap);

    i = configuration__index_of(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST_CASE(send_append_entries, error, io, NULL)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    raft_io_stub_fault(&f->io, 0, 1);

    i = configuration__index_of(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* Send the second log entry. */
TEST_CASE(send_append_entries, success, second_entry, NULL)
{
    struct fixture *f = data;
    size_t i;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __convert_to_leader(f);
    __append_entry(f);

    i = configuration__index_of(&f->raft.configuration, 2);

    rv = raft_replication__send_append_entries(&f->raft, i);
    munit_assert_int(rv, ==, 0);

    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}

/**
 * raft_replication__trigger
 */

TEST_SUITE(trigger);

TEST_SETUP(trigger, setup);
TEST_TEAR_DOWN(trigger, tear_down);

TEST_GROUP(trigger, error);

/* A failure occurs upon submitting the I/O request for a particular server, the
 * I/O requests for other servers are still submitted. */
TEST_CASE(trigger, error, _io, NULL)
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
