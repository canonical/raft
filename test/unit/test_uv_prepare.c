#include "../../src/uv.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a Uv instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * Helper macros.
 *
 *****************************************************************************/

struct result
{
    int status;
    unsigned long long counter;
    bool done;
};

static void prepareCbAssertResult(struct uvPrepare *req, int status)
{
    struct result *data = req->data;
    munit_assert_int(status, ==, data->status);
    if (status == 0) {
        munit_assert_int(req->counter, ==, data->counter);
        munit_assert_int(req->fd, >=, 0);
        munit_assert_int(UvOsClose(req->fd), ==, 0);
    }
    data->done = true;
}

#define PREPARE_REQ(COUNTER, STATUS)                  \
    struct result _result = {STATUS, COUNTER, false}; \
    struct uvPrepare _req;                            \
    _req.data = &_result;                             \
    uvPrepare(f->uv, &_req, prepareCbAssertResult);

/* Submit a prepare request expecting the open segment with the given counter to
 * be prepared, and wait for the operation to successfully complete. */
#define PREPARE(COUNTER)                      \
    do {                                      \
        PREPARE_REQ(COUNTER, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);        \
    } while (0)

/* Submit a prepare request with the given parameters and wait for the operation
 * to fail with the given status. */
#define PREPARE_FAILURE(STATUS)        \
    {                                  \
        PREPARE_REQ(0, STATUS);        \
        LOOP_RUN_UNTIL(&_result.done); \
    }

/******************************************************************************
 *
 * UvPrepare
 *
 *****************************************************************************/

SUITE(UvPrepare)

/* Issue the very first get request. */
TEST(UvPrepare, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PREPARE(1);
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    return MUNIT_OK;
}

/* Issue the very first get request and the a second one. */
TEST(UvPrepare, second, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PREPARE(1);
    PREPARE(2);
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}

/* The creation of the first segment fails because there's no space. */
TEST(UvPrepare, noSpace, setUp, tearDown, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    struct uv *uv;
    SKIP_IF_NO_FIXTURE;
#if !HAVE_DECL_UV_FS_O_CREAT
    /* This test appears to leak memory on older libuv versions. */
    return MUNIT_SKIP;
#endif
    uv = f->io.impl;
    uv->segment_size = 32768 * uv->block_size;
    PREPARE_FAILURE(RAFT_IOERR);
    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(UvPrepare, oom, setUp, tearDown, 0, error_oom_params)
{
    struct fixture *f = data;
    test_heap_fault_enable(&f->heap);
    PREPARE_FAILURE(RAFT_NOMEM);
    return MUNIT_OK;
}

/* It's possible to close the raft_io instance immediately after
 * initialization. */
TEST(UvPrepare, closeNoop, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    UV_CLOSE;
    return MUNIT_OK;
}

/* When the preparer is closed, all pending get requests get canceled. */
TEST(UvPrepare, closeCancelRequests, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct result data_ = {RAFT_CANCELED, 0, false};
    struct uvPrepare req;
    req.data = &data_;
    uvPrepare(f->uv, &req, prepareCbAssertResult);
    UV_CLOSE;
    LOOP_RUN_UNTIL(&data_.done);
    return MUNIT_OK;
}

/* When the preparer is closed, all unused files ready get removed. */
TEST(UvPrepare, closeRemovePool, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PREPARE(1);
    LOOP_RUN(1);
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    UV_CLOSE;
    LOOP_RUN(2);
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}
