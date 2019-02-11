#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;
    (void)params;

    test_heap_setup(params, &f->heap);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Test suite
 */

MunitSuite raft_io_uv_store_preparer_suites[] = {
    {NULL, NULL, NULL, 0, 0},
};
