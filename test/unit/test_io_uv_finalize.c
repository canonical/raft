#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/uv.h"

TEST_MODULE(io_uv__finalize);

/**
 * Helpers.
 */

struct fixture
{
    IO_UV_FIXTURE;
    uvCounter counter;
    size_t used;
    raft_index first_index;
    raft_index last_index;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->counter = 1;
    f->used = 256;
    f->first_index = 1;
    f->last_index = 2;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    IO_UV_TEAR_DOWN;
}

#define write_open_segment(COUNTER) \
    test_io_uv_write_open_segment_file(f->dir, COUNTER, 1, 0);

/* Invoke io_uv__finalize withe arguments in the fixture and assert that the
 * given result value is returned. */
#define invoke(RV)                                                       \
    {                                                                    \
        int rv;                                                          \
        rv = io_uv__finalize(f->uv, f->counter, f->used, f->first_index, \
                             f->last_index);                             \
        munit_assert_int(rv, ==, RV);                                    \
    }

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* Submit a request to finalize the first segment that was created. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;

    (void)params;

    write_open_segment(1);

    invoke(0);

    test_uv_run(&f->loop, 1);

    munit_assert_true(test_dir_has_file(f->dir, "1-2"));

    return MUNIT_OK;
}

/* Submet a request to finalize an open segment that was never written. */
TEST_CASE(success, unused, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, "open-1", 256);

    f->used = 0;

    invoke(0);

    test_uv_run(&f->loop, 1);

    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* If a request is submitted after the previous one has completed, it will wait
 * until the previous one completes before starting to be executed. */
TEST_CASE(success, wait, NULL)
{
    struct fixture *f = data;

    (void)params;

    write_open_segment(1);
    invoke(0);

    f->counter = 2;
    f->first_index = 3;
    f->last_index = 3;

    write_open_segment(2);
    invoke(0);

    test_uv_run(&f->loop, 1);
    test_uv_run(&f->loop, 1);

    munit_assert_true(test_dir_has_file(f->dir, "1-2"));
    munit_assert_true(test_dir_has_file(f->dir, "3-3"));

    return MUNIT_OK;
}

/**
 * Failure scenarios.
 */

TEST_SUITE(error);
TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);


static char *error_oom_heap_fault_delay[] = {"0", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(error, oom, error_oom_params)
{
    struct fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    invoke(RAFT_NOMEM);

    return MUNIT_OK;
}
