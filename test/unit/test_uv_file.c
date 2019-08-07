#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

#include "../../src/uv_file.h"
#include "../../src/uv_os.h"

TEST_MODULE(uv_file);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    size_t block_size;
    size_t direct_io;
    bool async_io;
    struct uvFile file;
    struct uvFileCreate create_req;
    struct uvFileWrite write_req;
    uv_buf_t bufs[2];
    bool closed;
    int completed;
    int status;
    uvErrMsg errmsg;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;
    (void)user_data;
    SETUP_DIR;
    SETUP_LOOP;
    rv = uvProbeIoCapabilities(f->dir, &f->direct_io, &f->async_io, f->errmsg);
    munit_assert_int(rv, ==, 0);
    f->block_size = f->direct_io != 0 ? f->direct_io : 4096;
    rv = uvFileInit(&f->file, &f->loop, f->direct_io != 0, f->async_io,
                    f->errmsg);
    munit_assert_int(rv, ==, 0);
    f->file.data = f;
    f->create_req.data = f;
    f->write_req.data = f;
    f->completed = 0;
    f->closed = false;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (!f->closed) {
        uvFileClose(&f->file, NULL);
    }
    TEAR_DOWN_LOOP;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void createCb(struct uvFileCreate *req, int status, const char *errmsg)
{
    struct fixture *f = req->data;
    f->completed++;
    f->status = status;
    strcpy(f->errmsg, errmsg);
}

static void writeCb(struct uvFileWrite *req, int status, const char *errmsg)
{
    struct fixture *f = req->data;
    f->completed++;
    f->status = status;
    strcpy(f->errmsg, errmsg);
}

/* Invoke uvFileCreate passing it the fixture's file object and dir. */
#define CREATE_RV(...)                                                    \
    uvFileCreate(&f->file, &f->create_req, f->dir, __VA_ARGS__, createCb, \
                 f->errmsg)
#define CREATE(FILENAME, SIZE, MAX_CONCURRENT_WRITES) \
    munit_assert_int(CREATE_RV(FILENAME, SIZE, MAX_CONCURRENT_WRITES), ==, 0)
#define CREATE_ERROR(RV, FILENAME, SIZE, MAX_CONCURRENT_WRITES) \
    munit_assert_int(CREATE_RV(FILENAME, SIZE, MAX_CONCURRENT_WRITES), ==, RV)

/* Wait for the create callback to fire and check its status. */
#define CREATE_WAIT_STATUS(STATUS)               \
    {                                            \
        int i_;                                  \
        for (i_ = 0; i_ < 2; i_++) {             \
            LOOP_RUN(1);                         \
            if (f->completed == 1) {             \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->status, ==, STATUS); \
        f->completed = 0;                        \
    }

#define CREATE_WAIT CREATE_WAIT_STATUS(0)

/* Fill the I'th fixture buffer with the given integer. */
#define FILL_BUF(I, N) memset(f->bufs[I].base, N, f->bufs[I].len);

/* Invoke uvFileWrite passing it the fixture's buffers. */
#define WRITE_RV(...)                                                   \
    uvFileWrite(&f->file, &f->write_req, f->bufs, __VA_ARGS__, writeCb, \
                f->errmsg)
#define WRITE(N_BUFS, OFFSET) munit_assert_int(WRITE_RV(N_BUFS, OFFSET), ==, 0)

/* Wait for a write callback to fire N times and check its last status. */
#define WRITE_WAIT_STATUS_N(N, STATUS)           \
    {                                            \
        int i_;                                  \
        for (i_ = 0; i_ < 5; i_++) {             \
            LOOP_RUN(1);                         \
            if (f->completed == N) {             \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->completed, ==, N);   \
        munit_assert_int(f->status, ==, STATUS); \
                                                 \
        f->completed = 0;                        \
        f->status = -1;                          \
    }
#define WRITE_WAIT_STATUS(STATUS) WRITE_WAIT_STATUS_N(1, STATUS)
#define WRITE_WAIT WRITE_WAIT_STATUS(0)

#define WRITE_AND_WAIT(N_BUFS, OFFSET) \
    WRITE(N_BUFS, OFFSET);             \
    WRITE_WAIT

/* CLose the fixture's file object. */
#define CLOSE                        \
    {                                \
        uvFileClose(&f->file, NULL); \
        f->closed = true;            \
    }

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the fixture's errmsg string matches the given value. */
#define ASSERT_ERRMSG(MSG) munit_assert_string_equal(f->errmsg, MSG)

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

TEST_SUITE(create);
TEST_SETUP(create, setup);
TEST_TEAR_DOWN(create, tear_down);

/* If the given path is valid, the file gets created. */
TEST_CASE(create, success, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    CREATE("foo", 4096, 1 /* max concurrent writes */);
    CREATE_WAIT;
    munit_assert_true(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

TEST_GROUP(create, error)

/* The directory of given path does not exist, an error is returned. */
TEST_CASE(create, error, no_entry, NULL)
{
    struct fixture *f = data;
    (void)params;
    CREATE_ERROR(UV__NOENT, "foo/bar", 4096, 1 /* max concurrent writes */);
    ASSERT_ERRMSG("open: No such file or directory");
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST_CASE(create, error, already_exists, NULL)
{
    struct fixture *f = data;
    char buf[8];
    (void)params;
    test_dir_write_file(f->dir, "foo", buf, sizeof buf);
    CREATE_ERROR(UV__ERROR, "foo", 4096, 1 /* max concurrent writes */);
    ASSERT_ERRMSG("open: File exists");
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST_CASE(create, error, no_space, NULL)
{
    struct fixture *f = data;
    (void)params;
    CREATE("foo", 4096 * 32768, 1 /* max concurrent writes */);
    CREATE_WAIT_STATUS(UV__ERROR);
    ASSERT_ERRMSG("posix_fallocate: No space left on device");
    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
TEST_CASE(create, error, no_resources, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    (void)params;
    test_aio_fill(&ctx, 0);
    CREATE_ERROR(UV__ERROR, "foo", 4096, 1);
    ASSERT_ERRMSG("io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* Close a file just after having issued a create request. */
TEST_CASE(create, error, cancel, NULL)
{
    struct fixture *f = data;
    (void)params;
    CREATE("foo", 4096, 1);
    CLOSE;
    CREATE_WAIT_STATUS(UV__CANCELED);
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvFileWrite
 *
 *****************************************************************************/

TEST_SUITE(write);

TEST_SETUP(write)
{
    struct fixture *f = setup(params, user_data);
    int i;
    CREATE("foo", 4096, 2 /* max concurrent writes */);
    CREATE_WAIT;
    for (i = 0; i < 2; i++) {
        uv_buf_t *buf = &f->bufs[i];
        buf->len = f->block_size;
        buf->base = aligned_alloc(f->block_size, f->block_size);
        munit_assert_ptr_not_null(buf->base);
        FILL_BUF(i, i +1);
    }
    return f;
}

TEST_TEAR_DOWN(write)
{
    struct fixture *f = data;
    int i;
    for (i = 0; i < 2; i++) {
        free(f->bufs[i].base);
    }
    tear_down(f);
}

/* Write a single buffer. */
TEST_CASE(write, one, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1 /* n_bufs */, 0 /* offset */);
    WRITE_WAIT;
    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
TEST_CASE(write, two, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    WRITE_AND_WAIT(1 /* n_bufs */, 0 /* offset */);
    FILL_BUF(0, 2);
    WRITE_AND_WAIT(1 /* n_bufs */, f->block_size /* offset */);
    ASSERT_CONTENT(2);
    return MUNIT_OK;
}

/* Write the same block twice. */
TEST_CASE(write, twice, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    FILL_BUF(0, 0);
    WRITE_AND_WAIT(1 /* n_bufs */, 0 /* offset */);
    FILL_BUF(0, 1);
    WRITE_AND_WAIT(1 /* n_bufs */, 0 /* offset */);
    ASSERT_CONTENT(1);
    return MUNIT_OK;
}

/* Write a vector of buffers. */
TEST_CASE(write, vec, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    WRITE_AND_WAIT(2 /* n_bufs */, 0 /* offset */);
    ASSERT_CONTENT(1);
    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
TEST_CASE(write, vec_twice, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    WRITE_AND_WAIT(2 /* n_bufs */, 0 /* offset */);
    WRITE_AND_WAIT(2 /* n_bufs */, 0 /* offset */);
    ASSERT_CONTENT(2);
    return MUNIT_OK;
}

/* Write two different blocks concurrently. */
TEST_CASE(write, concurrent, dir_all_params)
{
    struct fixture *f = data;
    struct uvFileWrite req;
    char errmsg[2048];
    int rv;
    (void)params;
    return MUNIT_SKIP; /* TODO: tests hang */

    req.data = f;

    WRITE(1, 0);

    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, f->block_size, writeCb,
                     errmsg);
    munit_assert_int(rv, ==, 0);

    WRITE_WAIT_STATUS_N(2, 0);
    ASSERT_CONTENT(2);

    return MUNIT_OK;
}

/* Write the same block concurrently. */
TEST_CASE(write, concurrent_twice, dir_all_params)
{
    struct fixture *f = data;
    struct uvFileWrite req;
    int rv;
    (void)params;
    return MUNIT_SKIP; /* TODO: tests hang */

    req.data = f;

    memset(f->bufs[1].base, 1, f->bufs[1].len);

    WRITE(1, 0);

    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, 0, writeCb, f->errmsg);
    munit_assert_int(rv, ==, 0);

    WRITE_WAIT_STATUS_N(2, 0);

    ASSERT_CONTENT(1);

    return MUNIT_OK;
}

TEST_GROUP(write, error)

/* There are not enough resources to create an AIO context to perform the
 * write. */
TEST_CASE(write, error, no_resources, dir_no_aio_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    (void)params;
    test_aio_fill(&ctx, 0);
    WRITE(1, 0);
    WRITE_WAIT_STATUS(UV__ERROR);
    ASSERT_ERRMSG("io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

/* Cancel an inflight write. */
TEST_CASE(write, error, cancel, dir_all_params)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, 0);
    CLOSE;
    WRITE_WAIT_STATUS(UV__CANCELED);
    return MUNIT_OK;
}
