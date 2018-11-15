#include "../../include/raft.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
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
    struct raft_fsm fsm;
    struct raft raft;
};

static int __rand()
{
    return munit_rand_uint32();
}

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    uint64_t id = 1;
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);

    test_logger_setup(params, &f->logger, id);

    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);
    rv = raft_io_sim_init(&f->raft);
    munit_assert_int(rv, ==, 0);

    raft_set_logger(&f->raft, &f->logger);
    raft_set_rand(&f->raft, __rand);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);

    test_logger_tear_down(&f->logger);

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * raft_init
 */

/* The raft state is properly initialized. */
static MunitResult test_init_state(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    (void)params;

    munit_assert_ptr_not_null(f->raft.io);

    munit_assert_int(f->raft.id, ==, 1);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_UNAVAILABLE);

    munit_assert_int(f->raft.current_term, ==, 0);
    munit_assert_int(f->raft.voted_for, ==, 0);
    munit_assert_ptr_null(f->raft.log.entries);
    munit_assert_int(f->raft.log.offset, ==, 0);

    munit_assert_int(f->raft.commit_index, ==, 0);
    munit_assert_int(f->raft.last_applied, ==, 0);

    munit_assert_ptr_null(f->raft.configuration.servers);
    munit_assert_int(f->raft.configuration.n, ==, 0);

    munit_assert_int(f->raft.configuration_index, ==, 0);
    munit_assert_int(f->raft.configuration_uncommitted_index, ==, 0);

    munit_assert_int(f->raft.election_timeout, ==, 1000);
    munit_assert_int(f->raft.election_timeout_rand, >=,
                     f->raft.election_timeout);
    munit_assert_int(f->raft.election_timeout_rand, <,
                     2 * f->raft.election_timeout);
    munit_assert_int(f->raft.heartbeat_timeout, ==, 100);

    munit_assert_int(f->raft.timer, ==, 0);

    munit_assert(f->raft.watchers[RAFT_EVENT_STATE_CHANGE] == NULL);

    munit_assert_ptr_equal(f->raft.ctx.state, &f->raft.state);
    munit_assert_ptr_equal(f->raft.ctx.current_term, &f->raft.current_term);

    munit_assert_ptr_null(f->raft.io_queue.requests);
    munit_assert_int(f->raft.io_queue.size, ==, 0);

    return MUNIT_OK;
}

static MunitTest init_tests[] = {
    {"/state", test_init_state, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_start
 */

/* The initial state is RAFT_STATE_UNAVAILABLE. */
static MunitResult test_start_state(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_start(&f->raft);
    munit_assert_int(rv, ==, 0);

    __assert_state(f, RAFT_STATE_UNAVAILABLE);

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/state", test_start_state, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};


/**
 * Test suite
 */

MunitSuite raft_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/start", start_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
