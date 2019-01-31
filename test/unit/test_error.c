#include "../../src/error.h"

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
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    unsigned id = 1;
    const char *address = "1";

    (void)user_data;

    test_heap_setup(params, &f->heap);

    test_logger_setup(params, &f->logger, id);

    test_io_setup(params, &f->io);

    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id, address);

    raft_set_logger(&f->raft, &f->logger);

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
 * raft_errorf
 */

/* Render an error message without parameters. */
static MunitResult test_errorf_plain(const MunitParameter params[], void *data)
{
    char errmsg[RAFT_ERRMSG_SIZE];

    (void)data;
    (void)params;

    raft_errorf(errmsg, "boom");
    munit_assert_string_equal("boom", errmsg);

    return MUNIT_OK;
}

/* Render an error message with parameters. */
static MunitResult test_errorf_params(const MunitParameter params[], void *data)
{
    char errmsg[RAFT_ERRMSG_SIZE];

    (void)data;
    (void)params;

    raft_errorf(errmsg, "boom %d", 123);
    munit_assert_string_equal("boom 123", errmsg);

    return MUNIT_OK;
}

static MunitTest errorf_tests[] = {
    {"plain", test_errorf_plain, NULL, NULL, 0, NULL},
    {"params", test_errorf_params, NULL, NULL, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};


/**
 * raft_errorf
 */

/* Wrap an error message without parameters. */
static MunitResult test_wrapf_plain(const MunitParameter params[], void *data)
{
    char errmsg[RAFT_ERRMSG_SIZE];

    (void)data;
    (void)params;

    raft_errorf(errmsg, "boom");

    raft_wrapf(errmsg, "do stuff");

    munit_assert_string_equal("do stuff: boom", errmsg);

    return MUNIT_OK;
}

/* Wrap an error message with parameters. */
static MunitResult test_wrapf_params(const MunitParameter params[], void *data)
{
    char errmsg[RAFT_ERRMSG_SIZE];

    (void)data;
    (void)params;

    raft_errorf(errmsg, "boom");

    raft_wrapf(errmsg, "do stuff %d", 123);

    munit_assert_string_equal("do stuff 123: boom", errmsg);

    return MUNIT_OK;
}

static MunitTest wrapf_tests[] = {
    {"plain", test_wrapf_plain, NULL, NULL, 0, NULL},
    {"params", test_wrapf_params, NULL, NULL, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_error__printf
 */

/* Wrap an error message. */
static MunitResult test_printf(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_error__printf(&f->raft, RAFT_ERR_NOMEM, "boom");
    raft_error__wrapf(&f->raft, "major failure %s", "now");

    munit_assert_string_equal("major failure now: boom: out of memory",
                              f->raft.errmsg);

    return MUNIT_OK;
}

static MunitTest printf_tests[] = {
    {"", test_printf, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_error_suites[] = {
    {"/errorf", errorf_tests, NULL, 1, 0},
    {"/wrapf", wrapf_tests, NULL, 1, 0},
    {"/printf", printf_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
