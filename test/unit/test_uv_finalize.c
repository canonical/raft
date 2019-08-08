#include "../lib/runner.h"
#include "../lib/uv.h"

#include "../../src/uv.h"

TEST_MODULE(uv_finalize);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    uvCounter counter;
    size_t used;
    raft_index first_index;
    raft_index last_index;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_UV;
    f->counter = 1;
    f->used = 256;
    f->first_index = 1;
    f->last_index = 2;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define WRITE_OPEN_SEGMENT(COUNTER) UV_WRITE_OPEN_SEGMENT(COUNTER, 1, 0);

/* Invoke io_uv__finalize withe arguments in the fixture and assert that the
 * given result value is returned. */
#define FINALIZE(RV)                                                \
    {                                                               \
        int rv;                                                     \
        rv = uvFinalize(f->uv, f->counter, f->used, f->first_index, \
                        f->last_index);                             \
        munit_assert_int(rv, ==, RV);                               \
    }

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* Submit a request to finalize the first segment that was created. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;
    (void)params;

    WRITE_OPEN_SEGMENT(1);

    FINALIZE(0);

    LOOP_RUN(1);

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

    FINALIZE(0);

    LOOP_RUN(1);

    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* If a request is submitted after the previous one has completed, it will wait
 * until the previous one completes before starting to be executed. */
TEST_CASE(success, wait, NULL)
{
    struct fixture *f = data;
    (void)params;

    WRITE_OPEN_SEGMENT(1);
    FINALIZE(0);

    f->counter = 2;
    f->first_index = 3;
    f->last_index = 3;

    WRITE_OPEN_SEGMENT(2);
    FINALIZE(0);

    LOOP_RUN(1);
    LOOP_RUN(1);

    munit_assert_true(test_dir_has_file(f->dir, "1-2"));
    munit_assert_true(test_dir_has_file(f->dir, "3-3"));

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Failure scenarios.
 *
 *****************************************************************************/

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
    FINALIZE(RAFT_NOMEM);
    return MUNIT_OK;
}
