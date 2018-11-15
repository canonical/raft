#include "../../include/raft.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct uv_loop_s loop;
    struct raft_heap heap;
    struct raft_logger logger;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    int rv;

    (void)user_data;

    rv = uv_loop_init(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);

    rv = raft_uv(&f->raft, &f->loop);
    munit_assert_int(rv, ==, 0);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    rv = uv_loop_close(&f->loop);
    munit_assert_int(rv, ==, 0);

    free(f);
}

/**
 * raft_uv
 */

static MunitResult test_start_and_stop(const MunitParameter params[], void *data)
{
    (void)params;
    (void)data;

    return MUNIT_OK;
}

static MunitTest lifecycle_tests[] = {
    {"/start-and-stop", test_start_and_stop, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_uv_suites[] = {
    {"/lifecycle", lifecycle_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
