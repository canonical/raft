#include "../../src/uv_error.h"
#include "../../src/uv_fs.h"
#include "../../src/uv_writer.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with an uninitialized UvWriter and an open file ready for writing.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    int fd;
    size_t block_size;
    size_t direct_io;
    bool async_io;
    char errmsg[256];
    struct UvWriter writer;
};

#define N_BLOCKS 5

/* Initialize a the fixture's UvFs object, leaving UvWriter unitialized. */
static void *setUp(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    char path[UV__PATH_SZ];
    char errmsg[256];
    int rv;
    SETUP_DIR_OR_SKIP;
    SETUP_LOOP;
    rv = UvFsProbeCapabilities(f->dir, &f->direct_io, &f->async_io, errmsg);
    munit_assert_int(rv, ==, 0);
    f->block_size = f->direct_io != 0 ? f->direct_io : 4096;
    UvOsJoin(f->dir, "foo", path);
    rv = UvOsOpen(path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR, &f->fd);
    munit_assert_int(rv, ==, 0);
    rv = UvOsFallocate(f->fd, 0, f->block_size * N_BLOCKS);
    munit_assert_int(rv, ==, 0);
    return f;
}

/* Cleanup the fixture's UvFs object. */
static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return; /* Was skipped. */
    }
    UvOsClose(f->fd);
    TEAR_DOWN_LOOP;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * UvWriterInit
 *
 *****************************************************************************/

static void closeCbMarkDone(struct UvWriter *w)
{
    bool *done = w->data;
    *done = true;
}

/* Initialize the fixture's writer. */
#define INIT(MAX_WRITES)                                                   \
    {                                                                      \
        int _rv;                                                           \
        _rv = UvWriterInit(&f->writer, &f->loop, f->fd, f->direct_io != 0, \
                           f->async_io, MAX_WRITES, f->errmsg);            \
        munit_assert_int(_rv, ==, 0);                                      \
    }

/* Trye to initialize the fixture's writer and check that the given error is
 * returned. */
#define INIT_ERROR(RV, ERRMSG)                                             \
    {                                                                      \
        int _rv;                                                           \
        _rv = UvWriterInit(&f->writer, &f->loop, f->fd, f->direct_io != 0, \
                           f->async_io, 1, f->errmsg);                     \
        munit_assert_int(_rv, ==, RV);                                     \
        munit_assert_string_equal(f->errmsg, ERRMSG);                      \
    }

/* Start closing the writer and wait for it shutdown. */
#define CLOSE                                       \
    {                                               \
        bool _done = false;                         \
        f->writer.data = &_done;                    \
        UvWriterClose(&f->writer, closeCbMarkDone); \
        LOOP_RUN_UNTIL(&_done);                     \
    }

SUITE(UvWriterInit)

/* The kernel has ran out of available AIO events. */
TEST(UvWriterInit, noResources, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    int rv;
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    INIT_ERROR(UV__ERROR, "io_setup: resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* Initialize and then close the fixture writer. */
TEST(UvWriterInit, success, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    INIT(1);
    CLOSE;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvWriterSubmit
 *
 *****************************************************************************/

struct result
{
    int status;
    const char *errmsg;
    bool done;
};

static void cbAssertResult(struct UvWriterReq *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

#define MAKE_BUFS(BUFS, N_BUFS, CONTENT)                               \
    {                                                                  \
        int __i;                                                       \
        BUFS = munit_malloc(sizeof *BUFS * N_BUFS);                    \
        for (__i = 0; __i < N_BUFS; __i++) {                           \
            uv_buf_t *__buf = &BUFS[__i];                              \
            __buf->len = f->block_size;                                \
            __buf->base = aligned_alloc(f->block_size, f->block_size); \
            munit_assert_ptr_not_null(__buf->base);                    \
            memset(__buf->base, CONTENT + __i, __buf->len);            \
        }                                                              \
    }

#define DESTROY_BUFS(BUFS, N_BUFS)           \
    {                                        \
        int __i;                             \
        for (__i = 0; __i < N_BUFS; __i++) { \
            free(BUFS[__i].base);            \
        }                                    \
        free(BUFS);                          \
    }

#define WRITE_REQ(N_BUFS, CONTENT, OFFSET, RV, STATUS)             \
    struct uv_buf_t *_bufs;                                        \
    struct UvWriterReq _req;                                       \
    struct result _result = {STATUS, NULL, false};                 \
    int _rv;                                                       \
    MAKE_BUFS(_bufs, N_BUFS, CONTENT);                             \
    _req.data = &_result;                                          \
    _rv = UvWriterSubmit(&f->writer, &_req, _bufs, N_BUFS, OFFSET, \
                         cbAssertResult);                          \
    munit_assert_int(_rv, ==, RV);

/* Submit a write request with the given parameters and wait for the operation
 * to successfully complete. Deallocate BUFS when done.
 *
 * N_BUFS is the number of buffers to allocate and write, each of them will have
 * f->block_size bytes.
 *
 * CONTENT must be an unsigned byte value: all bytes of the first buffer will be
 * filled with that value, all bytes of the second buffer will be filled will
 * that value plus one, etc.
 *
 * OFFSET is the offset at which to write the buffers. */
#define WRITE(N_BUFS, CONTENT, OFFSET)                                  \
    {                                                                   \
        WRITE_REQ(N_BUFS, CONTENT, OFFSET, 0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);                                  \
        DESTROY_BUFS(_bufs, N_BUFS);                                    \
    }

/* Submit a write request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define WRITE_FAILURE(N_BUFS, CONTENT, OFFSET, STATUS, ERRMSG)  \
    {                                                           \
        WRITE_REQ(N_BUFS, CONTENT, OFFSET, 0 /* rv */, STATUS); \
        LOOP_RUN_UNTIL(&_result.done);                          \
        munit_assert_string_equal(f->writer.errmsg, ERRMSG);    \
        DESTROY_BUFS(_bufs, N_BUFS);                            \
    }

/* Submit a write request with the given parameters, close the writer after
 * N loop iterations and assert that the request got canceled. */
#define WRITE_CANCEL(N_BUFS, CONTENT, OFFSET, N)                      \
    {                                                                 \
        WRITE_REQ(N_BUFS, CONTENT, OFFSET, 0 /* rv */, UV__CANCELED); \
        LOOP_RUN(N);                                                  \
        munit_assert_false(_result.done);                             \
        UvWriterCancel(&_req);                                        \
        LOOP_RUN_UNTIL(&_result.done);                                \
        DESTROY_BUFS(_bufs, N_BUFS);                                  \
    }

/* Assert that the content of the test file has the given number of blocks, each
 * filled with progressive numbers. */
#define ASSERT_CONTENT(N)                                    \
    {                                                        \
        size_t size = N * f->block_size;                     \
        void *buf = munit_malloc(size);                      \
        unsigned i_;                                         \
        unsigned j_;                                         \
                                                             \
        test_dir_read_file(f->dir, "foo", buf, size);        \
                                                             \
        for (i_ = 0; i_ < N; i_++) {                         \
            char *cursor = (char *)buf + i_ * f->block_size; \
            for (j_ = 0; j_ < f->block_size; j_++) {         \
                munit_assert_int(cursor[j_], ==, i_ + 1);    \
            }                                                \
        }                                                    \
                                                             \
        free(buf);                                           \
    }

SUITE(UvWriterSubmit)

TEST(UvWriterSubmit, one, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
TEST(UvWriterSubmit, two, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE(1 /* n bufs */, 2 /* content */, f->block_size /* offset */);
    ASSERT_CONTENT(2);
    CLOSE;
    return MUNIT_OK;
}

/* Write the same block twice. */
TEST(UvWriterSubmit, twice, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 0 /* content */, 0 /* offset */);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write a vector of buffers. */
TEST(UvWriterSubmit, vec, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
TEST(UvWriterSubmit, vec_twice, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(2);
    CLOSE;
    return MUNIT_OK;
}

/* Write past the allocated space. */
TEST(UvWriterSubmit, beyondEOF, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    int i;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    for (i = 0; i < N_BLOCKS + 1; i++) {
        WRITE(1 /* n bufs */, i + 1 /* content */,
              i * f->block_size /* offset */);
    }
    ASSERT_CONTENT((N_BLOCKS + 1));
    CLOSE;
    return MUNIT_OK;
}

/* Write two different blocks concurrently. */
TEST(UvWriterSubmit, concurrent, setUp, tearDown, 0, dir_all_params)
{
    return MUNIT_SKIP; /* TODO: tests hang */
}

/* Write the same block concurrently. */
TEST(UvWriterSubmit, concurrentSame, setUp, tearDown, 0, dir_all_params)
{
    return MUNIT_SKIP; /* TODO: tests hang */
}

/* There are not enough resources to create an AIO context to perform the
 * write. */
TEST(UvWriterSubmit, noResources, setUp, tearDown, 0, dir_no_aio_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    int rv;
    SKIP_IF_NO_FIXTURE;
    INIT(2);
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        CLOSE;
        return MUNIT_SKIP;
    }
    WRITE_FAILURE(1, 0, 0, UV__ERROR,
                  "io_setup: resource temporarily unavailable");
    test_aio_destroy(ctx);
    CLOSE;
    return MUNIT_OK;
}

/* Cancel an inflight write. */
TEST(UvWriterSubmit, cancel, setUp, tearDown, 0, dir_all_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE_CANCEL(1, 0, 0, 0);
    CLOSE;
    return MUNIT_OK;
}
