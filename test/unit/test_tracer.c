#include "../lib/heap.h"
#include "../lib/runner.h"

#include "../../include/raft.h"

TEST_MODULE(tracer);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    struct raft_tracer tracer;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    int rv;
    SETUP_HEAP;
    rv = raft_tracer_init(&f->tracer, 128);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    raft_tracer_close(&f->tracer);
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * tracerEmit
 *
 *****************************************************************************/

TEST_SUITE(emit);
TEST_SETUP(emit, setup);
TEST_TEAR_DOWN(emit, tear_down);

/* If the directory exists, nothing is needed. */
TEST_CASE(emit, first, NULL)
{
    struct fixture *f = data;
    (void)params;
    return MUNIT_OK;
}

