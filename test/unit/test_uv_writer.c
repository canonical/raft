#include "../../src/uv_error.h"
#include "../../src/uv_writer.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with an UvWriter.
 *
 *****************************************************************************/

struct writer
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    struct UvFs fs;
    int fd;
    size_t block_size;
    size_t direct_io;
    bool async_io;
    struct UvWriter writer;
};

#define N_BLOCKS 5

/* Initialize a the fixture's UvFs object, leaving UvWriter unitialized. */
static void *setupWriter(MUNIT_UNUSED const MunitParameter params[],
                         MUNIT_UNUSED void *user_data)
{
    struct writer *f = munit_malloc(sizeof *f);
    char path[UV__PATH_SZ];
    char *errmsg;
    int rv;
    SETUP_DIR_OR_SKIP;
    SETUP_LOOP;
    UvFsInit(&f->fs, &f->loop);
    rv = uvProbeIoCapabilities(f->dir, &f->direct_io, &f->async_io, &errmsg);
    munit_assert_int(rv, ==, 0);
    f->block_size = f->direct_io != 0 ? f->direct_io : 4096;
    UvOsJoin(f->dir, "foo", path);
    f->fd = UvOsOpen(path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    munit_assert_int(f->fd, >=, 0);
    rv = UvOsFallocate(f->fd, 0, f->block_size * N_BLOCKS);
    munit_assert_int(rv, ==, 0);
    return f;
}

/* Cleanup the fixture's UvFs object. */
static void tearDownWriter(void *data)
{
    struct writer *f = data;
    if (f == NULL) {
        return; /* Was skipped. */
    }
    UvOsClose(f->fd);
    UvFsClose(&f->fs);
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

#define INIT(MAX_WRITES)                                                 \
    {                                                                    \
        int rv_;                                                         \
        rv_ = UvWriterInit(&f->fs, &f->writer, f->fd, f->direct_io != 0, \
                           f->async_io, MAX_WRITES);                     \
        munit_assert_int(rv_, ==, 0);                                    \
    }

#define INIT_ERROR(RV, ERRMSG)                                           \
    {                                                                    \
        int rv_;                                                         \
        rv_ = UvWriterInit(&f->fs, &f->writer, f->fd, f->direct_io != 0, \
                           f->async_io, 1);                              \
        munit_assert_int(rv_, ==, RV);                                   \
        munit_assert_string_equal(UvFsErrMsg(&f->fs), ERRMSG);           \
    }

/* Start closing the writer wait for it shutdown. */
#define CLOSE                                       \
    {                                               \
        bool done_ = false;                         \
        int i_;                                     \
        f->writer.data = &done_;                    \
        UvWriterClose(&f->writer, closeCbMarkDone); \
        for (i_ = 0; i_ < 2; i_++) {                \
            LOOP_RUN(1);                            \
            if (done_) {                            \
                break;                              \
            }                                       \
        }                                           \
        munit_assert_true(done_);                   \
    }

SUITE(UvWriterInit)

/* The kernel has ran out of available AIO events. */
TEST(UvWriterInit, noResources, setupWriter, tearDownWriter, 0, NULL)
{
    struct writer *f = data;
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
TEST(UvWriterInit, success, setupWriter, tearDownWriter, 0, NULL)
{
    struct writer *f = data;
    INIT(1);
    CLOSE;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvWriterSubmit
 *
 *****************************************************************************/

struct submitResult
{
    int status;
    const char *errmsg;
    bool done;
};

static void submitCbAssertOk(struct UvWriterReq *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void submitCbAssertFail(struct UvWriterReq *req, int status)
{
    struct submitResult *result = req->data;
    munit_assert_int(status, !=, 0);
    munit_assert_int(status, ==, result->status);
    munit_assert_string_equal(UvWriterErrMsg(req->writer), result->errmsg);
    result->done = true;
}

#define MAKE_BUFS(BUFS, N_BUFS, CONTENT)                             \
    {                                                                \
        int i__;                                                     \
        BUFS = munit_malloc(sizeof *BUFS * N_BUFS);                  \
        for (i__ = 0; i__ < N_BUFS; i__++) {                         \
            uv_buf_t *buf = &BUFS[i__];                              \
            buf->len = f->block_size;                                \
            buf->base = aligned_alloc(f->block_size, f->block_size); \
            munit_assert_ptr_not_null(buf->base);                    \
            memset(buf->base, CONTENT + i__, buf->len);              \
        }                                                            \
    }

#define DESTROY_BUFS(BUFS, N_BUFS)           \
    {                                        \
        int i__;                             \
        for (i__ = 0; i__ < N_BUFS; i__++) { \
            free(BUFS[i__].base);            \
        }                                    \
        free(BUFS);                          \
    }

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
#define WRITE(N_BUFS, CONTENT, OFFSET)                                 \
    {                                                                  \
        struct uv_buf_t *bufs_;                                        \
        struct UvWriterReq req_;                                       \
        bool done_ = false;                                            \
        int rv_;                                                       \
        int i_;                                                        \
        MAKE_BUFS(bufs_, N_BUFS, CONTENT);                             \
        req_.data = &done_;                                            \
        rv_ = UvWriterSubmit(&f->writer, &req_, bufs_, N_BUFS, OFFSET, \
                             submitCbAssertOk);                        \
        munit_assert_int(rv_, ==, 0);                                  \
        for (i_ = 0; i_ < 2; i_++) {                                   \
            LOOP_RUN(1);                                               \
            if (done_) {                                               \
                break;                                                 \
            }                                                          \
        }                                                              \
        munit_assert_true(done_);                                      \
        DESTROY_BUFS(bufs_, N_BUFS);                                   \
    }

/* Submit a write request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define WRITE_FAILURE(N_BUFS, CONTENT, OFFSET, STATUS, ERRMSG)         \
    {                                                                  \
        struct uv_buf_t *bufs_;                                        \
        struct UvWriterReq req_;                                       \
        struct submitResult result_ = {STATUS, ERRMSG, false};         \
        int rv_;                                                       \
        int i_;                                                        \
        MAKE_BUFS(bufs_, N_BUFS, CONTENT);                             \
        req_.data = &result_;                                          \
        rv_ = UvWriterSubmit(&f->writer, &req_, bufs_, N_BUFS, OFFSET, \
                             submitCbAssertFail);                      \
        munit_assert_int(rv_, ==, 0);                                  \
        for (i_ = 0; i_ < 2; i_++) {                                   \
            LOOP_RUN(1);                                               \
            if (result_.done) {                                        \
                break;                                                 \
            }                                                          \
        }                                                              \
        munit_assert_true(result_.done);                               \
        DESTROY_BUFS(bufs_, N_BUFS);                                   \
    }

/* Assert that the content of the test file has the given number of blocks, each
 * filled with progressive numbers. */
#define ASSERT_CONTENT(N)                                   \
    {                                                       \
        size_t size = N * f->block_size;                    \
        void *buf = munit_malloc(size);                     \
        unsigned i;                                         \
        unsigned j;                                         \
                                                            \
        test_dir_read_file(f->dir, "foo", buf, size);       \
                                                            \
        for (i = 0; i < N; i++) {                           \
            char *cursor = (char *)buf + i * f->block_size; \
            for (j = 0; j < f->block_size; j++) {           \
                munit_assert_int(cursor[j], ==, i + 1);     \
            }                                               \
        }                                                   \
                                                            \
        free(buf);                                          \
    }

SUITE(UvWriterSubmit)

TEST(UvWriterSubmit, one, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
TEST(UvWriterSubmit, two, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE(1 /* n bufs */, 2 /* content */, f->block_size /* offset */);
    ASSERT_CONTENT(2);
    CLOSE;
    return MUNIT_OK;
}

/* Write the same block twice. */
TEST(UvWriterSubmit, twice, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(1 /* n bufs */, 0 /* content */, 0 /* offset */);
    WRITE(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write a vector of buffers. */
TEST(UvWriterSubmit, vec, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    CLOSE;
    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
TEST(UvWriterSubmit, vec_twice, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(2);
    CLOSE;
    return MUNIT_OK;
}

/* Write past the allocated space. */
TEST(UvWriterSubmit, beyondEOF, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
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
TEST(UvWriterSubmit, concurrent, setupWriter, tearDownWriter, 0, dir_all_params)
{
    return MUNIT_SKIP; /* TODO: tests hang */
}

/* Write the same block concurrently. */
TEST(UvWriterSubmit,
     concurrentSame,
     setupWriter,
     tearDownWriter,
     0,
     dir_all_params)
{
    return MUNIT_SKIP; /* TODO: tests hang */
}

/* There are not enough resources to create an AIO context to perform the
 * write. */
TEST(UvWriterSubmit,
     noResources,
     setupWriter,
     tearDownWriter,
     0,
     dir_no_aio_params)
{
    struct writer *f = data;
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
TEST(UvWriterSubmit, cancel, setupWriter, tearDownWriter, 0, dir_all_params)
{
    struct writer *f = data;
    struct uv_buf_t *bufs;
    struct UvWriterReq req;
    struct submitResult result = {UV__CANCELED, "canceled", false};
    int rv;
    SKIP_IF_NO_FIXTURE;
    INIT(1);
    MAKE_BUFS(bufs, 1, 0);
    req.data = &result;
    rv = UvWriterSubmit(&f->writer, &req, bufs, 1, 0, submitCbAssertFail);
    munit_assert_int(rv, ==, 0);
    UvWriterCancel(&req);
    LOOP_RUN(1);
    munit_assert_true(result.done);
    DESTROY_BUFS(bufs, 1);
    CLOSE;
    return MUNIT_OK;
}
