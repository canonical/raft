#include "../../include/raft.h"

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
    struct raft_heap heap;
    struct raft_logger logger;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
    struct
    {
        bool invoked;
    } stop_cb;
};

static int __rand()
{
    return munit_rand_uint32();
}

static void __stop_cb(void *data)
{
    struct fixture *f = data;

    f->stop_cb.invoked = true;
}

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    uint64_t id = 1;
    const char *address = "1";
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_io_setup(params, &f->io, &f->logger);
    test_fsm_setup(params, &f->fsm);

    rv = raft_init(&f->raft, &f->logger, &f->io, &f->fsm, f, id, address);
    munit_assert_int(rv, ==, 0);

    raft_set_rand(&f->raft, __rand);

    f->stop_cb.invoked = false;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);
    test_io_tear_down(&f->io);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Start the fixture's instance and check that no error occurs.
 */
#define __start(F)                   \
    {                                \
        int rv;                      \
                                     \
        rv = raft_start(&F->raft);   \
        munit_assert_int(rv, ==, 0); \
    }

/**
 * Start the fixture's instance and that the given error occurs.
 */
#define __assert_start_error(F, RV)   \
    {                                 \
        int rv;                       \
                                      \
        rv = raft_start(&F->raft);    \
        munit_assert_int(rv, ==, RV); \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * raft_init
 */

static char *init_oom_heap_fault_delay[] = {"0", NULL};
static char *init_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum init_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, init_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, init_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
static MunitResult test_init_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft raft;
    int rv;

    (void)params;

    test_heap_fault_enable(&f->heap);

    rv = raft_init(&raft, &f->logger, &f->io, &f->fsm, f, 1, "1");
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

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

    return MUNIT_OK;
}

static MunitTest init_tests[] = {
    {"/oom", test_init_oom, setup, tear_down, 0, init_oom_params},
    {"/state", test_init_state, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_start
 */

/* An error occurs when starting the I/O backend. */
static MunitResult test_start_io_err(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_io_stub_fault(&f->io, 0, 1);

    __assert_start_error(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The state after a successful start of a pristine server is
 * RAFT_STATE_FOLLOWER. */
static MunitResult test_start_pristine(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __start(f);

    __assert_state(f, RAFT_STATE_FOLLOWER);

    rv = raft_stop(&f->raft, f, __stop_cb);
    munit_assert_int(rv, ==, 0);

    munit_assert_true(f->stop_cb.invoked);

    return MUNIT_OK;
}

/* Start an instance that has been bootstrapped. */
static MunitResult test_start_bootstrapped(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    __start(f);

    __assert_state(f, RAFT_STATE_FOLLOWER);

    rv = raft_stop(&f->raft, NULL, NULL);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static char *start_oom_heap_fault_delay[] = {"0", "1,", "2", "3", "4",
                                             "5", "6",  "7", NULL};
static char *start_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum start_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, start_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, start_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory condditions. */
static MunitResult test_start_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    test_heap_fault_enable(&f->heap);

    __assert_start_error(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/io-err", test_start_io_err, setup, tear_down, 0, NULL},
    {"/pristine", test_start_pristine, setup, tear_down, 0, NULL},
    {"/bootstrapped", test_start_bootstrapped, setup, tear_down, 0, NULL},
    {"/oom", test_start_oom, setup, tear_down, 0, start_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_bootstrap
 */

/* An error occurs when bootstrapping the state on disk. */
static MunitResult test_bootstrap_io_err(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_bootstrap(&f->raft, &configuration);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/* Starting an instance after it's bootstrapped initializes the
 * configuration. */
static MunitResult test_bootstrap_state(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_bootstrap(&f->raft, &configuration);
    munit_assert_int(rv, ==, 0);

    __start(f);

    munit_assert_int(f->raft.configuration.n, ==, 1);
    munit_assert_int(f->raft.configuration.servers[0].id, ==, 1);
    munit_assert_string_equal(f->raft.configuration.servers[0].address, "1");

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

static MunitTest bootstrap_tests[] = {
    {"/io-err", test_bootstrap_io_err, setup, tear_down, 0, NULL},
    {"/state", test_bootstrap_state, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/start", start_tests, NULL, 1, 0},
    {"/bootstrap", bootstrap_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
