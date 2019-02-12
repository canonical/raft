#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/uv.h"

#include "../../src/io_uv_preparer.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct uv_loop_s loop;
    struct raft_logger logger;
    char *dir;
    size_t block_size;
    struct raft__io_uv_preparer preparer;
    struct raft__io_uv_preparer_get get;
    struct
    {
        int invoked;                /* Number of times __get_cb was invoked */
        struct raft__uv_file *file; /* Last open segment passed to __get_cb */
        unsigned long long counter; /* Last counter passed to __get_cb */
        int status;                 /* Last status passed to __get_cb */
    } get_cb;
    struct
    {
        unsigned long long counter;
        size_t used;
        raft_index first_index;
        raft_index last_index;
    } put_args;
    struct
    {
        int invoked; /* Number of times the close callback was invoked */
    } close_cb;
};

static void __get_cb(struct raft__io_uv_preparer_get *req,
                     struct raft__uv_file *file,
                     unsigned long long counter,
                     int status)
{
    struct fixture *f = req->data;

    f->get_cb.invoked++;
    f->get_cb.file = file;
    f->get_cb.counter = counter;
    f->get_cb.status = status;
}

static void __close_cb(struct raft__io_uv_preparer *preparer)
{
    struct fixture *f = preparer->data;

    f->close_cb.invoked++;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;
    (void)params;

    test_heap_setup(params, &f->heap);
    test_uv_setup(params, &f->loop);
    test_logger_setup(params, &f->logger, 1);

    f->dir = test_dir_setup(params);

    rv = raft__uv_file_block_size(f->dir, &f->block_size);
    munit_assert_int(rv, ==, 0);

    rv = raft__io_uv_preparer_init(&f->preparer, &f->loop, &f->logger, f->dir,
                                   f->block_size, 3);
    munit_assert_int(rv, ==, 0);

    f->preparer.data = f;
    f->get.data = f;

    f->get_cb.invoked = 0;
    f->get_cb.file = NULL;
    f->get_cb.counter = 0;
    f->get_cb.status = -1;

    f->put_args.counter = 1;
    f->put_args.used = 256;
    f->put_args.first_index = 1;
    f->put_args.last_index = 2;

    f->close_cb.invoked = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    if (!raft__io_uv_preparer_is_closing(&f->preparer)) {
        raft__io_uv_preparer_close(&f->preparer, __close_cb);
    }

    if (f->get_cb.file != NULL) {
        raft__uv_file_close(f->get_cb.file, (raft__uv_file_close_cb)raft_free);
    }

    test_uv_stop(&f->loop);

    munit_assert_int(f->close_cb.invoked, ==, 1);

    test_dir_tear_down(f->dir);

    test_logger_tear_down(&f->logger);
    test_uv_tear_down(&f->loop);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Assert that @raft__io_uv_preparer_get returns the given code.
 */
#define __get_assert_result(F, RV)                                      \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft__io_uv_preparer_get(&F->preparer, &F->get, __get_cb); \
        munit_assert_int(rv, ==, RV);                                   \
    }

/**
 * Wait for the get callback to fire and check its status.
 */
#define __get_assert_cb(F, STATUS)                            \
    {                                                         \
        int i;                                                \
                                                              \
        /* Run the loop until the get request is completed */ \
        for (i = 0; i < 5; i++) {                             \
            test_uv_run(&F->loop, 1);                         \
                                                              \
            if (F->get_cb.invoked == 1) {                     \
                break;                                        \
            }                                                 \
        }                                                     \
                                                              \
        munit_assert_int(F->get_cb.invoked, ==, 1);           \
        munit_assert_int(F->get_cb.status, ==, STATUS);       \
        F->get_cb.invoked = 0;                                \
    }

/**
 * Submit a get open segment request, wait for its completion and assert that no
 * error occurs.
 */
#define __get(F)                   \
    {                              \
        __get_assert_result(F, 0); \
        __get_assert_cb(F, 0);     \
    }

/**
 * Close the fixture preparer.
 */
#define __close(F)                                            \
    {                                                         \
        raft__io_uv_preparer_close(&F->preparer, __close_cb); \
    }

/**
 * raft__uv_preparer_get
 */

/* Issue the very first get request. */
static MunitResult test_get_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __get(f);

    munit_assert_true(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* Issue the very first get request and the a second one. */
static MunitResult test_get_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __get(f);

    raft__uv_file_close(f->get_cb.file, (raft__uv_file_close_cb)raft_free);

    __get(f);

    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}

/* The creation of the first segment fails because io_setup() returns EAGAIN. */
static MunitResult test_get_no_resources(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    __get_assert_result(f, 0);
    __get_assert_cb(f, RAFT_ERR_IO_ABORTED);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* The creation of the first segment fails because there's no space. */
static MunitResult test_get_no_space(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    f->preparer.n_blocks = 32768;

    __get_assert_result(f, 0);
    __get_assert_cb(f, RAFT_ERR_IO_ABORTED);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_get_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    __get_assert_result(f, 0);
    __get_assert_cb(f, RAFT_ERR_IO_ABORTED);

    return MUNIT_OK;
}

static char *get_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *get_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum get_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, get_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, get_oom_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_get(NAME, FUNC, PARAMS)                         \
    {                                                          \
        "/" NAME, test_get_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest get_tests[] = {
    __test_get("first", first, NULL),
    __test_get("second", second, NULL),
    __test_get("no-resources", no_resources, NULL),
    __test_get("no-space", no_space, NULL),
    __test_get("oom", oom, get_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__uv_preparer_close
 */

/* When the preparer is closed, all pending get requests get canceled. */
static MunitResult test_close_cancel_get(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    __get_assert_result(f, 0);
    __close(f);
    __get_assert_cb(f, RAFT_ERR_IO_CANCELED);

    return MUNIT_OK;
}

/* When the preparer is closed, all unused ready get removed. */
static MunitResult test_close_remove_ready(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;

    (void)params;

    __get_assert_result(f, 0);
    __get_assert_cb(f, 0);

    test_uv_run(&f->loop, 1);
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));

    __close(f);

    test_uv_run(&f->loop, 1);
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}

#define __test_close(NAME, FUNC, PARAMS)                         \
    {                                                            \
        "/" NAME, test_close_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest close_tests[] = {
    __test_close("cancel-get", cancel_get, NULL),
    __test_close("remove-ready", remove_ready, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_preparer_suites[] = {
    {"/get", get_tests, NULL, 1, 0},
    {"/close", close_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
