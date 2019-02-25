#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/uv.h"

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_uv_closer.h"
#include "../../src/io_uv_encoding.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct uv_loop_s loop;
    struct raft_logger logger;
    char *dir;
    struct raft__io_uv_loader loader;
    struct raft__io_uv_closer closer;
    struct
    {
        unsigned long long counter;
        size_t used;
        raft_index first_index;
        raft_index last_index;
    } put_args;
    struct
    {
        raft_index index;
    } truncate_args;
    struct
    {
        int invoked; /* Number of times the truncate callback was invoked */
        int status;
    } truncate_cb;
    struct
    {
        int invoked; /* Number of times the close callback was invoked */
    } close_cb;
    int count;
};

static void __truncate_cb(struct raft__io_uv_closer_truncate *req, int status)
{
    struct fixture *f = req->data;

    f->truncate_cb.invoked++;
    f->truncate_cb.status = status;

    free(req);
}

static void __close_cb(struct raft__io_uv_closer *closer)
{
    struct fixture *f = closer->data;

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

    raft__io_uv_loader_init(&f->loader, &f->logger, f->dir);

    rv = raft__io_uv_closer_init(&f->closer, &f->loop, &f->logger, &f->loader,
                                 f->dir);
    munit_assert_int(rv, ==, 0);

    f->closer.data = f;

    f->put_args.counter = 1;
    f->put_args.used = 256;
    f->put_args.first_index = 1;
    f->put_args.last_index = 2;

    f->truncate_args.index = 2;

    f->truncate_cb.invoked = 0;
    f->truncate_cb.status = -1;

    f->close_cb.invoked = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    if (!raft__io_uv_closer_is_closing(&f->closer)) {
        raft__io_uv_closer_close(&f->closer, __close_cb);
    }

    test_uv_stop(&f->loop);

    munit_assert_int(f->close_cb.invoked, ==, 1);

    test_dir_tear_down(f->dir);

    test_logger_tear_down(&f->logger);
    test_uv_tear_down(&f->loop);
    test_heap_tear_down(&f->heap);

    free(f);
}

#define __WORD_SIZE sizeof(uint64_t)

/**
 * Write a valid segment with #M batches.
 */
#define __make_segment(F, FILENAME, M)                                        \
    {                                                                         \
        size_t size = __WORD_SIZE /* Format version */;                       \
        int i;                                                                \
        uint8_t *buf;                                                         \
        void *cursor;                                                         \
        unsigned crc1;                                                        \
        unsigned crc2;                                                        \
        uint8_t *batch; /* Start of the batch */                              \
        size_t header_size = raft_io_uv_sizeof__batch_header(1);              \
        size_t data_size = __WORD_SIZE;                                       \
                                                                              \
        size += (__WORD_SIZE /* Checksums */ + header_size + data_size) *     \
                (size_t)(M);                                                  \
        buf = munit_malloc(size);                                             \
        cursor = buf;                                                         \
        raft__put64(&cursor, 1); /* Format version */                         \
        batch = cursor;                                                       \
                                                                              \
        for (i = 0; i < (int)(M); i++) {                                      \
            F->count++;                                                       \
                                                                              \
            raft__put64(&cursor, 0);               /* CRC sums placeholder */ \
            raft__put64(&cursor, 1);               /* Number of entries */    \
            raft__put64(&cursor, 1);               /* Entry term */           \
            raft__put8(&cursor, RAFT_LOG_COMMAND); /* Entry type */           \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put32(&cursor, 8);               /* Size of entry data */   \
            raft__put64(&cursor, F->count);        /* Entry data */           \
                                                                              \
            cursor = batch + __WORD_SIZE;                                     \
            crc1 = raft__crc32(cursor, header_size, 0);                       \
            crc2 = raft__crc32(cursor + header_size, data_size, 0);           \
            cursor = batch;                                                   \
            raft__put32(&cursor, crc1); /* Header checksum */                 \
            raft__put32(&cursor, crc2); /* Data checksum */                   \
            batch += __WORD_SIZE + header_size + data_size;                   \
            cursor = batch;                                                   \
        }                                                                     \
                                                                              \
        test_dir_write_file(F->dir, FILENAME, buf, size);                     \
        free(buf);                                                            \
        F->put_args.used = size;                                              \
    }

/**
 * Create an open segment with a counter and number of entries matching the
 * values of F->put_args.
 */
#define __make_open_segment(F)                                                \
    {                                                                         \
        char filename[64];                                                    \
                                                                              \
        sprintf(filename, "open-%lld", F->put_args.counter);                  \
                                                                              \
        __make_segment(F, filename,                                           \
                       F->put_args.last_index - F->put_args.first_index + 1); \
    }

/**
 * Give back to the closer the open segment described in @put_args.
 */
#define __put_assert_result(F, RV)                                             \
    {                                                                          \
        int rv;                                                                \
        rv = raft__io_uv_closer_put(&F->closer, F->put_args.counter,           \
                                    F->put_args.used, F->put_args.first_index, \
                                    F->put_args.last_index);                   \
        munit_assert_int(rv, ==, RV);                                          \
    }

/**
 * Call @raft__io_uv_close_put checking that no error occurs, then spin a few
 * times to let the request finish.
 */
#define __put(F)                   \
    {                              \
        __put_assert_result(f, 0); \
        test_uv_run(&f->loop, 2);  \
    }

/**
 * Call @raft__io_uv_close_truncate and check that it returns the given value.
 */
#define __truncate_assert_result(F, RV)                                      \
    {                                                                        \
        struct raft__io_uv_closer_truncate *req;                             \
        int rv;                                                              \
                                                                             \
        req = munit_malloc(sizeof *req);                                     \
        req->data = F;                                                       \
                                                                             \
        rv = raft__io_uv_closer_truncate(&F->closer, F->truncate_args.index, \
                                         req, __truncate_cb);                \
        munit_assert_int(rv, ==, RV);                                        \
                                                                             \
        if (rv != 0) {                                                       \
            free(req);                                                       \
        }                                                                    \
    }

/**
 * Wait for the truncate callback to fire and check its status.
 */
#define __truncate_assert_cb(F, STATUS)                       \
    {                                                         \
        int i;                                                \
                                                              \
        /* Run the loop until the get request is completed */ \
        for (i = 0; i < 5; i++) {                             \
            test_uv_run(&F->loop, 1);                         \
                                                              \
            if (F->truncate_cb.invoked == 1) {                \
                break;                                        \
            }                                                 \
        }                                                     \
                                                              \
        munit_assert_int(F->truncate_cb.invoked, ==, 1);      \
        munit_assert_int(F->truncate_cb.status, ==, STATUS);  \
        F->truncate_cb.invoked = 0;                           \
    }

/**
 * Submit a truncate log request, wait for its completion and assert that no
 * error occurs.
 */
#define __truncate(F)                   \
    {                                   \
        __truncate_assert_result(F, 0); \
        __truncate_assert_cb(F, 0);     \
    }

/**
 * Close the fixture closer.
 */
#define __close(F)                                              \
    {                                                           \
        int i;                                                  \
                                                                \
        raft__io_uv_closer_close(&F->closer, __close_cb);       \
                                                                \
        /* Run the loop until the close request is completed */ \
        for (i = 0; i < 5; i++) {                               \
            test_uv_run(&F->loop, 1);                           \
                                                                \
            if (F->close_cb.invoked == 1) {                     \
                break;                                          \
            }                                                   \
        }                                                       \
                                                                \
        munit_assert_int(F->close_cb.invoked, ==, 1);           \
    }

/**
 * raft__uv_closer_put
 */

/* Ask the closer to close the first segment that it created. */
static MunitResult test_put_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __make_open_segment(f);

    __put(f);

    munit_assert_true(test_dir_has_file(f->dir, "1-2"));

    return MUNIT_OK;
}

/* Ask the closer to close an open segment that was never written. */
static MunitResult test_put_unused(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __make_open_segment(f);

    f->put_args.used = 0;

    __put(f);

    munit_assert_false(test_dir_has_file(f->dir, "1-2"));
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* If the closer has aborted, any put request is ignored. */
static MunitResult test_put_abort(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    __make_open_segment(f);

    test_aio_fill(&ctx, 0);

    test_uv_run(&f->loop, 1);

    __put_assert_result(f, 0);

    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_false(test_dir_has_file(f->dir, "1-2"));

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_put_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __make_open_segment(f);

    test_heap_fault_enable(&f->heap);

    __put_assert_result(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *put_oom_heap_fault_delay[] = {"0", NULL};
static char *put_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum put_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, put_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, put_oom_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_put(NAME, FUNC, PARAMS)                         \
    {                                                          \
        "/" NAME, test_put_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest put_tests[] = {
    __test_put("first", first, NULL),  __test_put("unused", unused, NULL),
    __test_put("abort", abort, NULL),  __test_put("oom", oom, put_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__io_uv_closer_truncate
 */

/* If the index to truncate is at the start of a segment, that segment and all
 * subsequent ones are removed. */
static MunitResult test_truncate_whole_segment(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;

    (void)params;

    f->put_args.first_index = 1;
    f->put_args.last_index = 3;

    __make_open_segment(f);

    __put(f);

    f->put_args.first_index = 4;
    f->put_args.last_index = 4;

    __make_open_segment(f);

    __put(f);

    __truncate(f);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    return MUNIT_OK;
}

/* If the index to truncate is not at the start of a segment, that segment gets
 * truncated. */
static MunitResult test_truncate_partial_segment(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    struct raft_snapshot *snapshot;
    struct raft_entry *entries;
    size_t n;
    int rv;

    (void)params;

    f->put_args.first_index = 1;
    f->put_args.last_index = 3;

    __make_open_segment(f);

    __put(f);

    f->put_args.first_index = 4;
    f->put_args.last_index = 4;

    __make_open_segment(f);

    __put(f);

    f->truncate_args.index = 2;

    __truncate(f);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    rv = raft__io_uv_loader_load_all(&f->loader, &snapshot, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n, ==, 1);
    munit_assert_int(raft__flip64(*(uint64_t *)entries[0].buf.base), ==, 1);

    raft_free(entries[0].batch);
    raft_free(entries);

    return MUNIT_OK;
}

/* A truncate request waits for any pending close request to finish. */
static MunitResult test_truncate_wait_closing(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    f->put_args.first_index = 1;
    f->put_args.last_index = 3;

    __make_open_segment(f);

    __put_assert_result(f, 0);

    __truncate(f);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));

    return MUNIT_OK;
}

#define __test_truncate(NAME, FUNC, PARAMS)                         \
    {                                                               \
        "/" NAME, test_truncate_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest truncate_tests[] = {
    __test_truncate("whole-segment", whole_segment, NULL),
    __test_truncate("partial-segment", partial_segment, NULL),
    __test_truncate("wait-closing", wait_closing, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__io_uv_closer_close
 */

/* It's possible to close the closer right after initialization. */
static MunitResult test_close_noop(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __close(f);

    return MUNIT_OK;
}

/* When the closer is closed, all pending get requests get canceled. */
static MunitResult test_close_wait(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __make_open_segment(f);
    __put_assert_result(f, 0);

    f->put_args.counter = 2;
    f->put_args.first_index = 3;
    f->put_args.last_index = 4;

    __put_assert_result(f, 0);

    __close(f);

    munit_assert_true(test_dir_has_file(f->dir, "1-2"));
    munit_assert_false(test_dir_has_file(f->dir, "3-4"));

    return MUNIT_OK;
}

#define __test_close(NAME, FUNC, PARAMS)                         \
    {                                                            \
        "/" NAME, test_close_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest close_tests[] = {
    __test_close("noop", noop, NULL),
    __test_close("wait", wait, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_closer_suites[] = {
    {"/put", put_tests, NULL, 1, 0},
    {"/truncate", truncate_tests, NULL, 1, 0},
    {"/close", close_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
