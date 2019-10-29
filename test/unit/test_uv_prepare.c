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
    struct uvPrepare req;
    int invoked;                /* Number of times __get_cb was invoked */
    struct uvFile *file;        /* Last open segment passed to __get_cb */
    unsigned long long counter; /* Last counter passed to __get_cb */
    int status;                 /* Last status passed to __get_cb */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_UV;
    f->req.data = f;
    f->invoked = 0;
    f->file = NULL;
    f->counter = 0;
    f->status = -1;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    if (f->file != NULL) {
        uvFileClose(f->file, (uvFileCloseCb)raft_free);
    }
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void prepareCbAssertOk(struct uvPrepare *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

/* Submit a prepare request with the given parameters and wait for the operation
 * to successfully complete. */
#define PREPARE                                     \
    {                                               \
        struct uvPrepare req_;                      \
        bool done_ = false;                         \
        int i_;                                     \
        req_.data = &done_;                         \
        uvPrepare(f->uv, &req_, prepareCbAssertOk); \
        for (i_ = 0; i_ < 2; i_++) {                \
            LOOP_RUN(1);                            \
            if (done_) {                            \
                break;                              \
            }                                       \
        }                                           \
        munit_assert_true(done_);                   \
    }

static void prepareCb(struct uvPrepare *req, int status)
{
    struct fixture *f = req->data;

    f->invoked++;
    f->status = status;
}

/* Invoke uvPrepare. */
#define PREPARE_ uvPrepare(f->uv, &f->req, prepareCb);

/* Wait for the get callback to fire and check its status. */
#define WAIT_CB(STATUS)                          \
    {                                            \
        int i;                                   \
        for (i = 0; i < 5; i++) {                \
            LOOP_RUN(1);                         \
            if (f->invoked == 1) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, 1);     \
        munit_assert_int(f->status, ==, STATUS); \
        f->invoked = 0;                          \
    }

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

SUITE(UvPrepare)

/* Issue the very first get request. */
TEST(UvPrepare, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    PREPARE;
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    return MUNIT_OK;
}

/* Issue the very first get request and the a second one. */
TEST(UvPrepare, second, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    PREPARE;
    PREPARE;
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Failure scenarios.
 *
 *****************************************************************************/

TEST_SUITE(error)

TEST_SETUP(error, setup)
TEST_TEAR_DOWN(error, tear_down)

/* The creation of the first segment fails because uvIoSetup() returns EAGAIN.
 */
TEST_CASE(error, no_resources, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    int rv;
    (void)params;
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    PREPARE_;
    WAIT_CB(RAFT_IOERR);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* The creation of the first segment fails because there's no space. */
TEST(error, no_space, setup, tear_down, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    struct uv *uv;
    SKIP_IF_NO_FIXTURE;
    uv = f->io.impl;
    uv->n_blocks = 32768;
    PREPARE_;
    WAIT_CB(RAFT_IOERR);
    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", "1", NULL};
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
    PREPARE_;
    WAIT_CB(RAFT_NOMEM);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Close raft_io instance scenarios.
 *
 *****************************************************************************/

TEST_SUITE(close)

TEST_SETUP(close, setup)
TEST_TEAR_DOWN(close, tear_down)

/* It's possible to close the raft_io instance immediately after
 * initialization. */
TEST_CASE(close, noop, NULL)
{
    struct fixture *f = data;
    (void)params;
    UV_CLOSE;
    return MUNIT_OK;
}

/* When the preparer is closed, all pending get requests get canceled. */
TEST_CASE(close, cancel_requests, NULL)
{
    struct fixture *f = data;
    (void)params;
    PREPARE_;
    UV_CLOSE;
    WAIT_CB(RAFT_CANCELED);
    return MUNIT_OK;
}

/* When the preparer is closed, all unused files ready get removed. */
TEST_CASE(close, remove_pool, NULL)
{
    struct fixture *f = data;
    (void)params;
    PREPARE_;
    WAIT_CB(0);
    LOOP_RUN(1);
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    UV_CLOSE;
    LOOP_RUN(2);
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}
