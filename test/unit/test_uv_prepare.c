#include "../../src/uv.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

TEST_MODULE(uv_prepare)

/******************************************************************************
 *
 * Fixture with an Uv instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    return f;
}

static void tear_down(void *data)
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
 * UvPrepare
 *
 *****************************************************************************/

/* Object linked to uvPrepare->data. */
struct prepareData
{
    int status;
    unsigned long long counter;
    bool done;
};

static void prepareCb(struct uvPrepare *req, int status)
{
    struct prepareData *data = req->data;
    munit_assert_int(status, ==, data->status);
    if (status == 0) {
        munit_assert_int(req->counter, ==, data->counter);
        munit_assert_int(req->fd, >=, 0);
        munit_assert_int(UvOsClose(req->fd), ==, 0);
    }
    data->done = true;
}

/* Wait for the prepare callback to fire. */
#define PREPARE_WAIT(DATA)               \
    {                                    \
        int i_;                          \
        for (i_ = 0; i_ < 2; i_++) {     \
            LOOP_RUN(1);                 \
            if ((DATA)->done) {          \
                break;                   \
            }                            \
        }                                \
        munit_assert_true((DATA)->done); \
    }

/* Submit a prepare request expecting the open segment with the given counter to
 * be prepared, and wait for the operation to successfully complete. */
#define PREPARE(COUNTER)                                \
    {                                                   \
        struct prepareData data_ = {0, COUNTER, false}; \
        struct uvPrepare req_;                          \
        req_.data = &data_;                             \
        uvPrepare(f->uv, &req_, prepareCb);             \
        PREPARE_WAIT(&data_);                           \
    }

/* Submit a prepare request with the given parameters and wait for the operation
 * to fail with the given. */
#define PREPARE_FAILURE(RV)                        \
    {                                              \
        struct prepareData data_ = {RV, 0, false}; \
        struct uvPrepare req_;                     \
        req_.data = &data_;                        \
        uvPrepare(f->uv, &req_, prepareCb);        \
        PREPARE_WAIT(&data_);                      \
    }

SUITE(UvPrepare)

/* Issue the very first get request. */
TEST(UvPrepare, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    PREPARE(1);
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    return MUNIT_OK;
}

/* Issue the very first get request and the a second one. */
TEST(UvPrepare, second, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    PREPARE(1);
    PREPARE(2);
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}

/* The creation of the first segment fails because there's no space. */
TEST(UvPrepare, noSpace, setup, tear_down, 0, dir_tmpfs_params)
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

static char *error_oom_heap_fault_delay[] = {"2", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(UvPrepare, oom, setup, tear_down, 0, error_oom_params)
{
    struct fixture *f = data;
    test_heap_fault_enable(&f->heap);
    PREPARE_FAILURE(RAFT_NOMEM);
    return MUNIT_OK;
}

/* It's possible to close the raft_io instance immediately after
 * initialization. */
TEST(UvPrepare, closeNoop, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    UV_CLOSE;
    return MUNIT_OK;
}

/* When the preparer is closed, all pending get requests get canceled. */
TEST(UvPrepare, closeCancelRequests, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct prepareData data_ = {RAFT_CANCELED, 0, false};
    struct uvPrepare req;
    req.data = &data_;
    uvPrepare(f->uv, &req, prepareCb);
    UV_CLOSE;
    PREPARE_WAIT(&data_);
    return MUNIT_OK;
}

/* When the preparer is closed, all unused files ready get removed. */
TEST(UvPrepare, closeRemovePool, setup, tear_down, 0, NULL)
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
