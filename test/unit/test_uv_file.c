#include "../../src/uv_file.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct file
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    size_t block_size;
    size_t direct_io;
    bool async_io;
    struct uvFile file;
};

struct result
{
    int status;
    const char *errmsg;
    bool done;
};

static void *setupFile(const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct file *f = munit_malloc(sizeof *f);
    uvErrMsg errmsg;
    int rv;
    SETUP_DIR;
    if (f->dir == NULL) { /* Desired fs not available, skip test. */
        free(f);
        return NULL;
    }
    SETUP_LOOP;
    rv = uvProbeIoCapabilities(f->dir, &f->direct_io, &f->async_io, errmsg);
    munit_assert_int(rv, ==, 0);
    rv = uvFileInit(&f->file, &f->loop, f->direct_io != 0, f->async_io, errmsg);
    munit_assert_int(rv, ==, 0);
    f->block_size = f->direct_io != 0 ? f->direct_io : 4096;
    return f;
}

static void tearDownFile(void *data)
{
    struct file *f = data;
    if (f == NULL) {
        return;
    }
    if (uvFileIsOpen(&f->file)) {
        uvFileClose(&f->file, NULL);
    }
    TEAR_DOWN_LOOP;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

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

/******************************************************************************
 *
 * uvFileCreate
 *
 *****************************************************************************/

static void createCbAssertOk(struct uvFileCreate *req,
                             int status,
                             MUNIT_UNUSED const char *errmsg)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void createCbAssertFail(struct uvFileCreate *req,
                               int status,
                               MUNIT_UNUSED const char *errmsg)
{
    struct result *result = req->data;
    munit_assert_int(status, !=, 0);
    munit_assert_int(status, ==, result->status);
    munit_assert_string_equal(errmsg, result->errmsg);
    result->done = true;
}

/* Start creating a file with the given parameters and wait for the operation
 * to successfully complete. */
#define CREATE_(FILENAME, SIZE, MAX_CONCURRENT_WRITES)                        \
    {                                                                         \
        struct uvFileCreate req_;                                             \
        bool done_ = false;                                                   \
        uvErrMsg errmsg_;                                                     \
        int rv_;                                                              \
        int i_;                                                               \
        req_.data = &done_;                                                   \
        rv_ = uvFileCreate(&f->file, &req_, f->dir, FILENAME, SIZE,           \
                           MAX_CONCURRENT_WRITES, createCbAssertOk, errmsg_); \
        munit_assert_int(rv_, ==, 0);                                         \
        for (i_ = 0; i_ < 2; i_++) {                                          \
            LOOP_RUN(1);                                                      \
            if (done_) {                                                      \
                break;                                                        \
            }                                                                 \
        }                                                                     \
        munit_assert_true(done_);                                             \
    }

/* Try to create a file with the given parameters and check that the given error
 * is immediately returned. */
#define CREATE_ERROR_(FILENAME, SIZE, MAX_CONCURRENT_WRITES, RV, ERRMSG) \
    {                                                                    \
        struct uvFileCreate req_;                                        \
        uvErrMsg errmsg_;                                                \
        int rv_;                                                         \
        rv_ = uvFileCreate(&f->file, &req_, f->dir, FILENAME, SIZE,      \
                           MAX_CONCURRENT_WRITES, NULL, errmsg_);        \
        munit_assert_int(rv_, ==, RV);                                   \
        munit_assert_string_equal(errmsg_, ERRMSG);                      \
    }

/* Start creating a file with the given parameters and wait for the operation to
 * fail with the given code and message. */
#define CREATE_FAILURE(FILENAME, SIZE, MAX_CONCURRENT_WRITES, STATUS, ERRMSG) \
    {                                                                         \
        struct uvFileCreate req_;                                             \
        struct result result_ = {                                             \
            .status = STATUS, .errmsg = ERRMSG, .done = false};               \
        uvErrMsg errmsg_;                                                     \
        int rv_;                                                              \
        int i_;                                                               \
        req_.data = &result_;                                                 \
        rv_ =                                                                 \
            uvFileCreate(&f->file, &req_, f->dir, FILENAME, SIZE,             \
                         MAX_CONCURRENT_WRITES, createCbAssertFail, errmsg_); \
        munit_assert_int(rv_, ==, 0);                                         \
        for (i_ = 0; i_ < 2; i_++) {                                          \
            LOOP_RUN(1);                                                      \
            if (result_.done) {                                               \
                break;                                                        \
            }                                                                 \
        }                                                                     \
        munit_assert_true(result_.done);                                      \
    }

SUITE(uvFileCreate)

/* If the given path is valid, the file gets created. */
TEST(uvFileCreate, success, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", /* file name */
            4096,  /* file size */
            1 /* max concurrent writes */);
    munit_assert_true(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
TEST(uvFileCreate, dirNoExists, setupFile, tearDownFile, 0, NULL)
{
    struct file *f = data;
    CREATE_ERROR_("foo/bar", /* file name */
                  4096,      /* file size */
                  1,         /* max concurrent writes */
                  UV__NOENT, /* error code */
                  "open: no such file or directory" /* error message */);
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST(uvFileCreate, fileAlreadyExists, setupFile, tearDownFile, 0, NULL)
{
    struct file *f = data;
    char buf[8];
    test_dir_write_file(f->dir, "foo", buf, sizeof buf);
    CREATE_ERROR_("foo",     /* file name */
                  4096,      /* file size */
                  1,         /* max concurrent writes */
                  UV__ERROR, /* error code */
                  "open: file already exists" /* error message */);
    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
TEST(uvFileCreate, noResources, setupFile, tearDownFile, 0, NULL)
{
    struct file *f = data;
    aio_context_t ctx = 0;
    int rv;
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    CREATE_ERROR_("foo",     /* file name */
                  4096,      /* file size */
                  1,         /* max concurrent writes */
                  UV__ERROR, /* error code */
                  "io_setup: Resource temporarily unavailable" /* message */);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST(uvFileCreate, noSpace, setupFile, tearDownFile, 0, dir_tmpfs_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_FAILURE(
        "foo",        /* file name */
        4096 * 32768, /* file size */
        1,            /* max concurrent writes */
        UV__ERROR,    /* error code */
        "posix_fallocate: No space left on device" /* error message */);
    return MUNIT_OK;
}

/* Close a file just after having issued a create request. */
TEST(uvFileCreate, cancel, setupFile, tearDownFile, 0, NULL)
{
    struct file *f = data;
    struct uvFileCreate req;
    struct result result = {
        .status = UV__CANCELED, .errmsg = "canceled", .done = false};
    uvErrMsg errmsg;
    int rv;
    req.data = &result;
    rv = uvFileCreate(&f->file, &req, f->dir, "foo", 4096, 1,
                      createCbAssertFail, errmsg);
    munit_assert_int(rv, ==, 0);
    uvFileClose(&f->file, NULL);
    LOOP_RUN(1);
    munit_assert_true(result.done);
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvFileWrite
 *
 *****************************************************************************/

static void writeCbAssertOk(struct uvFileWrite *req,
                            int status,
                            MUNIT_UNUSED const char *errmsg)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void writeCbAssertFail(struct uvFileWrite *req,
                              int status,
                              MUNIT_UNUSED const char *errmsg)
{
    struct
    {
        int status;
        const char *errmsg;
        bool done;
    } *data = req->data;
    munit_assert_int(status, !=, 0);
    munit_assert_int(status, ==, data->status);
    munit_assert_string_equal(errmsg, data->errmsg);
    data->done = true;
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

/* Start writing a file with the given parameters and wait for the operation
 * to successfully complete. Deallocate BUFS when done.
 *
 * N_BUFS is the number of buffers to allocate and write, each of them will have
 * f->block_size bytes.
 *
 * CONTENT must be unsigned byte value: all bytes of the first buffer will be
 * filled with that value, all bytes of the second buffer will be filled will
 * that value plus one, etc.
 *
 * OFFSET is the offset at which to write the buffers. */
#define WRITE_(N_BUFS, CONTENT, OFFSET)                           \
    {                                                             \
        struct uv_buf_t *bufs_;                                   \
        struct uvFileWrite req_;                                  \
        bool done_ = false;                                       \
        uvErrMsg errmsg_;                                         \
        int rv_;                                                  \
        int i_;                                                   \
        MAKE_BUFS(bufs_, N_BUFS, CONTENT);                        \
        req_.data = &done_;                                       \
        rv_ = uvFileWrite(&f->file, &req_, bufs_, N_BUFS, OFFSET, \
                          writeCbAssertOk, errmsg_);              \
        for (i_ = 0; i_ < 2; i_++) {                              \
            LOOP_RUN(1);                                          \
            if (done_) {                                          \
                break;                                            \
            }                                                     \
        }                                                         \
        munit_assert_true(done_);                                 \
        DESTROY_BUFS(bufs_, N_BUFS);                              \
    }

/* Start writing a file with the given parameters and wait for the operation to
 * fail with the given code and message. */
#define WRITE_FAILURE(N_BUFS, CONTENT, OFFSET, STATUS, ERRMSG)    \
    {                                                             \
        struct uv_buf_t *bufs_;                                   \
        struct uvFileWrite req_;                                  \
        struct result result_ = {                                 \
            .status = STATUS, .errmsg = ERRMSG, .done = false};   \
        uvErrMsg errmsg_;                                         \
        int rv_;                                                  \
        int i_;                                                   \
        MAKE_BUFS(bufs_, N_BUFS, CONTENT);                        \
        req_.data = &result_;                                     \
        rv_ = uvFileWrite(&f->file, &req_, bufs_, N_BUFS, OFFSET, \
                          writeCbAssertFail, errmsg_);            \
        munit_assert_int(rv_, ==, 0);                             \
        for (i_ = 0; i_ < 2; i_++) {                              \
            LOOP_RUN(1);                                          \
            if (result_.done) {                                   \
                break;                                            \
            }                                                     \
        }                                                         \
        munit_assert_true(result_.done);                          \
        DESTROY_BUFS(bufs_, N_BUFS);                              \
    }

SUITE(uvFileWrite)

/* Write a single buffer. */
TEST(uvFileWrite, one, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    WRITE_(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
TEST(uvFileWrite, two, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    WRITE_(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE_(1 /* n bufs */, 2 /* content */, f->block_size /* offset */);
    ASSERT_CONTENT(2);
    return MUNIT_OK;
}

/* Write the same block twice. */
TEST(uvFileWrite, twice, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    WRITE_(1 /* n bufs */, 0 /* content */, 0 /* offset */);
    WRITE_(1 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    return MUNIT_OK;
}

/* Write a vector of buffers. */
TEST(uvFileWrite, vec, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    WRITE_(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(1);
    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
TEST(uvFileWrite, vec_twice, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    WRITE_(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    WRITE_(2 /* n bufs */, 1 /* content */, 0 /* offset */);
    ASSERT_CONTENT(2);
    return MUNIT_OK;
}

/* Write two different blocks concurrently. */
TEST(uvFileWrite, concurrent, setupFile, tearDownFile, 0, dir_all_params)
{
    /*struct fixture *f = data;
      struct uvFileWrite req;
      char errmsg[2048];
      int rv;*/
    return MUNIT_SKIP; /* TODO: tests hang */

    /*req.data = f;
    WRITE(1, 0);
    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, f->block_size, writeCb,
                     errmsg);
    munit_assert_int(rv, ==, 0);
    WRITE_WAIT_STATUS_N(2, 0);
    ASSERT_CONTENT(2);*/
    return MUNIT_OK;
}

/* Write the same block concurrently. */
TEST(uvFileWrite, concurrent_twice, setupFile, tearDownFile, 0, dir_all_params)
{
    /*struct file *f = data;
      struct uvFileWrite req;
      int rv;*/
    return MUNIT_SKIP; /* TODO: tests hang */

    /*req.data = f;
    memset(f->bufs[1].base, 1, f->bufs[1].len);
    WRITE(1, 0);
    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, 0, writeCb, f->errmsg);
    munit_assert_int(rv, ==, 0);
    WRITE_WAIT_STATUS_N(2, 0);
    ASSERT_CONTENT(1);*/
    return MUNIT_OK;
}

/* There are not enough resources to create an AIO context to perform the
 * write. */
TEST(uvFileWrite, noResources, setupFile, tearDownFile, 0, dir_no_aio_params)
{
    struct file *f = data;
    aio_context_t ctx = 0;
    int rv;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 2);
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    WRITE_FAILURE(1, 0, 0, UV__ERROR,
                  "io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* Cancel an inflight write. */
TEST(uvFileWrite, cancel, setupFile, tearDownFile, 0, dir_all_params)
{
    struct file *f = data;
    struct uv_buf_t *bufs;
    struct uvFileWrite req;
    struct result result = {
        .status = UV__CANCELED, .errmsg = "canceled", .done = false};
    uvErrMsg errmsg;
    int rv;
    SKIP_IF_NO_FIXTURE;
    CREATE_("foo", f->block_size, 1);
    MAKE_BUFS(bufs, 1, 0);
    req.data = &result;
    rv = uvFileWrite(&f->file, &req, bufs, 1, 0, writeCbAssertFail, errmsg);
    munit_assert_int(rv, ==, 0);
    uvFileClose(&f->file, NULL);
    LOOP_RUN(1);
    munit_assert_true(result.done);
    DESTROY_BUFS(bufs, 1);
    return MUNIT_OK;
}
