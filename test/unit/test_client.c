#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"

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
    struct raft_heap heap;
    struct raft_logger logger;
    struct raft_io io;
    struct raft raft;
};

static int __rand()
{
    return munit_rand_uint32();
}

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_io_setup(params, &f->io);

    raft_init(&f->raft, &f->io, f, id);

    raft_set_logger(&f->raft, &f->logger);
    raft_set_rand(&f->raft, __rand);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

    test_io_tear_down(&f->io);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 *
 * raft_accept
 *
 */

/* If the raft instance is not in leader state, an error is returned. */
static MunitResult test_accept_not_leader(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    buf.base = NULL;
    buf.len = 0;

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);
    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "can't accept entries: server is not the leader");

    return MUNIT_OK;
}

static char *accept_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *accept_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum accept_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, accept_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, accept_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
static MunitResult test_accept_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    /* Reset the request queue, to trigger a failure when attempting to grow
     * it. */
    raft_free(f->raft.io_queue.requests);
    f->raft.io_queue.requests = NULL;
    f->raft.io_queue.size = 0;

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* I/O error. */
static MunitResult test_accept_io_err(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    buf.base = NULL;
    buf.len = 0;

    test_io_fault(&f->io, 0, 1);

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_SHUTDOWN);

    return MUNIT_OK;
}

/* The new entries are sent to all other servers. */
static MunitResult test_accept_send_entries(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    struct test_io_request request1;
    struct test_io_request request2;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    buf.base = NULL;
    buf.len = 0;

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, 0);

    /* A write log request has been submitted. */
    test_io_get_one_request(&f->io, RAFT_IO_WRITE_LOG, &request1);

    /* An append entries request has been submitted. */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES, &request2);

    test_io_flush(&f->io);

    raft_handle_io(&f->raft, request1.id, 0);
    raft_handle_io(&f->raft, request2.id, 0);

    return MUNIT_OK;
}

static MunitTest accept_tests[] = {
    {"/not-leader", test_accept_not_leader, setup, tear_down, 0, NULL},
    {"/oom", test_accept_oom, setup, tear_down, 0, accept_oom_params},
    {"/io-err", test_accept_io_err, setup, tear_down, 0, NULL},
    {"/send-entries", test_accept_send_entries, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_client_suites[] = {
    {"/accept", accept_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
