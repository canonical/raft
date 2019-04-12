#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/uv.h"

TEST_MODULE(io_uv__prepare);

/**
 * Helpers
 */

struct fixture
{
    IO_UV_FIXTURE;
    struct uv__prepare req;
    int invoked;                /* Number of times __get_cb was invoked */
    struct uv__file *file;      /* Last open segment passed to __get_cb */
    unsigned long long counter; /* Last counter passed to __get_cb */
    int status;                 /* Last status passed to __get_cb */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
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
    if (f->file != NULL) {
        uv__file_close(f->file, (uv__file_close_cb)raft_free);
    }
    IO_UV_TEAR_DOWN;
}

static void prepare_cb(struct uv__prepare *req,
                       struct uv__file *file,
                       unsigned long long counter,
                       int status)
{
    struct fixture *f = req->data;

    f->invoked++;
    f->file = file;
    f->counter = counter;
    f->status = status;
}

/* Invoke io_uv__prepare. */
#define prepare__invoke                             \
    {                                               \
        io_uv__prepare(f->uv, &f->req, prepare_cb); \
    }

/* Wait for the get callback to fire and check its status. */
#define prepare__wait_cb(STATUS)                 \
    {                                            \
        int i;                                   \
        for (i = 0; i < 5; i++) {                \
            test_uv_run(&f->loop, 1);            \
            if (f->invoked == 1) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, 1);     \
        munit_assert_int(f->status, ==, STATUS); \
        f->invoked = 0;                          \
    }

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* Issue the very first get request. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;

    (void)params;

    prepare__invoke;
    prepare__wait_cb(0);

    munit_assert_true(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* Issue the very first get request and the a second one. */
TEST_CASE(success, second, NULL)
{
    struct fixture *f = data;

    (void)params;

    prepare__invoke;
    prepare__wait_cb(0);

    uv__file_close(f->file, (uv__file_close_cb)raft_free);

    prepare__invoke;
    prepare__wait_cb(0);

    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}

/**
 * Failure scenarios.
 */

TEST_SUITE(error);

TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* The creation of the first segment fails because io_setup() returns EAGAIN. */
TEST_CASE(error, no_resources, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    prepare__invoke;
    prepare__wait_cb(RAFT_IOERR);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* The creation of the first segment fails because there's no space. */
TEST_CASE(error, no_space, NULL)
{
    struct fixture *f = data;
    struct uv *uv = f->io.impl;

    (void)params;

    uv->n_blocks = 32768;

    prepare__invoke;
    prepare__wait_cb(RAFT_IOERR);

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

    prepare__invoke;
    prepare__wait_cb(RAFT_NOMEM);

    return MUNIT_OK;
}

/**
 * Close raft_io instance scenarios.
 */

TEST_SUITE(close);

TEST_SETUP(close, setup);
TEST_TEAR_DOWN(close, tear_down);

/* It's possible to close the raft_io instance immediately after
 * initialization. */
TEST_CASE(close, noop, NULL)
{
    struct fixture *f = data;

    (void)params;

    io_uv__close;

    return MUNIT_OK;
}

/* When the preparer is closed, all pending get requests get canceled. */
TEST_CASE(close, cancel_requests, NULL)
{
    struct fixture *f = data;

    (void)params;

    prepare__invoke;
    io_uv__close;
    prepare__wait_cb(RAFT_CANCELED);

    return MUNIT_OK;
}

/* When the preparer is closed, all unused files ready get removed. */
TEST_CASE(close, remove_pool, NULL)
{
    struct fixture *f = data;

    (void)params;

    prepare__invoke;
    prepare__wait_cb(0);

    test_uv_run(&f->loop, 1);
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));

    io_uv__close;

    test_uv_run(&f->loop, 2);
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}
