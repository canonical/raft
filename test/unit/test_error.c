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

    (void)user_data;

    test_heap_setup(params, &f->heap);

    test_logger_setup(params, &f->logger, id);

    test_io_setup(params, &f->io);

    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);

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
 * raft_context__wrapf
 */

/* Wrap an error message. */
static MunitResult test_wrapf(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    raft_error__printf(&f->raft, RAFT_ERR_NOMEM, "boom");
    raft_error__wrapf(&f->raft, "major failure %s", "now");

    munit_assert_string_equal("major failure now: boom: out of memory",
                              f->raft.errmsg);

    return MUNIT_OK;
}

static MunitTest wrapf_tests[] = {
    {"", test_wrapf, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_error_suites[] = {
    {"/wrapf", wrapf_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
