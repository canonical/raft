#include "../lib/fs.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

#include "../../src/aio.h"
#include "../../src/os.h"
#include "../../src/uv_file.h"

TEST_MODULE(uv_file);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

#define FIXTURE_FILE    \
    FIXTURE_DIR;        \
    FIXTURE_LOOP;       \
    size_t block_size;  \
    struct uvFile file; \
    bool closed;

#define SETUP_FILE                            \
    int rv;                                   \
    (void)user_data;                          \
    SETUP_DIR;                                \
    SETUP_LOOP;                               \
    rv = osBlockSize(f->dir, &f->block_size); \
    munit_assert_int(rv, ==, 0);              \
    rv = uvFileInit(&f->file, &f->loop);      \
    munit_assert_int(rv, ==, 0);              \
    f->file.data = f;                         \
    f->closed = false;

#define TEAR_DOWN_FILE               \
    if (!f->closed) {                \
        uvFileClose(&f->file, NULL); \
    }                                \
    TEAR_DOWN_LOOP;                  \
    TEAR_DOWN_DIR;

/**
 * Invoke @uvFileCreate and assert that it returns the given code.
 */
#define CREATE__INVOKE(RV)                                     \
    {                                                          \
        int rv;                                                \
        rv = uvFileCreate(&f->file, &f->req, f->path, f->size, \
                          f->max_n_writes, create__cb);        \
        munit_assert_int(rv, ==, RV);                          \
    }

/******************************************************************************
 *
 * uvFileCreate
 *
 *****************************************************************************/

TEST_SUITE(create);

struct create_fixture
{
    FIXTURE_FILE;
    struct uvFileCreate req;
    char path[64];         /* Path of the file to create */
    size_t size;           /* Size of the file to create */
    unsigned max_n_writes; /* Max n of writes of the file to create */
    bool invoked;
    int status;
};

TEST_SETUP(create)
{
    struct create_fixture *f = munit_malloc(sizeof *f);
    SETUP_FILE;
    f->req.data = f;
    sprintf(f->path, "%s/foo", f->dir);
    f->size = 4096;
    f->max_n_writes = 1;
    f->invoked = 0;
    return f;
}

TEST_TEAR_DOWN(create)
{
    struct create_fixture *f = data;
    TEAR_DOWN_FILE;
    free(f);
}

static void create__cb(struct uvFileCreate *req, int status)
{
    struct create_fixture *f = req->data;
    f->invoked = true;
    f->status = status;
}

/**
 * Wait for the create callback to fire and check its status.
 */
#define CREATE__WAIT_CB(STATUS)                  \
    {                                            \
        int i;                                   \
        for (i = 0; i < 2; i++) {                \
            LOOP_RUN(1);            \
            if (f->invoked == 1) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, 1);     \
        munit_assert_int(f->status, ==, STATUS); \
    }

#define CREATE__CLOSE                \
    {                                \
        uvFileClose(&f->file, NULL); \
        f->closed = true;            \
    }

/* If the given path is valid, the file gets opened. */
TEST_CASE(create, success, dir_fs_supported_params)
{
    struct create_fixture *f = data;

    (void)params;

    CREATE__INVOKE(0);
    CREATE__WAIT_CB(0);

    return MUNIT_OK;
}

TEST_GROUP(create, error)

/* The directory of given path does not exist, an error is returned. */
TEST_CASE(create, error, no_entry, NULL)
{
    struct create_fixture *f = data;

    sprintf(f->path, "/non/existing/dir/foo");

    (void)params;

    CREATE__INVOKE(UV_ENOENT);

    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST_CASE(create, error, already_exists, NULL)
{
    struct create_fixture *f = data;
    char buf[8];

    (void)params;

    test_dir_write_file(f->dir, "foo", buf, sizeof buf);

    CREATE__INVOKE(UV_EEXIST);

    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST_CASE(create, error, no_space, NULL)
{
    struct create_fixture *f = data;

    (void)params;

    f->size = 4096 * 32768;

    CREATE__INVOKE(0);
    CREATE__WAIT_CB(UV_ENOSPC);

    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
TEST_CASE(create, error, no_resources, NULL)
{
    struct create_fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    CREATE__INVOKE(UV_EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Close a file just after having issued a create request. */
TEST_CASE(create, error, cancel, NULL)
{
    struct create_fixture *f = data;

    (void)params;

    CREATE__INVOKE(0);
    CREATE__CLOSE;

    CREATE__WAIT_CB(UV_ECANCELED);

    munit_assert_false(test_dir_has_file(f->dir, "foo"));

    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvFileWrite
 *
 *****************************************************************************/

TEST_SUITE(write);

TEST_GROUP(write, success)
TEST_GROUP(write, error)

struct write_fixture
{
    FIXTURE_FILE;
    struct uvFileWrite req;
    uv_buf_t bufs[2];
    unsigned n_bufs;
    size_t offset;
    int invoked; /* Number of times the write callback was invoked */
    int status;  /* Result passed to the last callback invokation */
};

TEST_SETUP(write)
{
    struct write_fixture *f = munit_malloc(sizeof *f);
    struct uvFileCreate req;
    int i;
    char path[64];
    size_t size = 4096;
    SETUP_FILE;
    sprintf(path, "%s/foo", f->dir);
    rv = uvFileCreate(&f->file, &req, path, size, 2, NULL);
    munit_assert_int(rv, ==, 0);
    LOOP_RUN(1);
    for (i = 0; i < 2; i++) {
        uv_buf_t *buf = &f->bufs[i];
        buf->len = f->block_size;
        buf->base = aligned_alloc(f->block_size, f->block_size);
        munit_assert_ptr_not_null(buf->base);
        memset(buf->base, i + 1, buf->len);
    }
    f->n_bufs = 1; /* By default write only one of the two buffers */
    f->offset = 0;
    f->invoked = 0;
    f->req.data = f;
    return f;
}

TEST_TEAR_DOWN(write)
{
    struct write_fixture *f = data;
    int i;
    for (i = 0; i < 2; i++) {
        free(f->bufs[i].base);
    }
    TEAR_DOWN_FILE;
    free(f);
}

static void write_cb(struct uvFileWrite *req, int status)
{
    struct write_fixture *f = req->data;

    f->invoked++;
    f->status = status;
}

/* Invoke @uvFileWrite and assert it returns the given code. */
#define write__invoke(RV)                                                   \
    {                                                                       \
        int rv2;                                                            \
        rv2 = uvFileWrite(&f->file, &f->req, f->bufs, f->n_bufs, f->offset, \
                          write_cb);                                        \
        munit_assert_int(rv2, ==, RV);                                      \
    }

/* Wait for a write callback to fire N times and check its last status. */
#define write__wait_cb(N, STATUS)                \
    {                                            \
        int i;                                   \
        for (i = 0; i < 5; i++) {                \
            LOOP_RUN(1);            \
            if (f->invoked == N) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, N);     \
        munit_assert_int(f->status, ==, STATUS); \
                                                 \
        f->invoked = 0;                          \
        f->status = -1;                          \
    }

/* Submit a write file request, wait for its completion and assert that no error
 * occurs. */
#define write__complete                               \
    {                                                 \
        write__invoke(0);                             \
        write__wait_cb(1, f->n_bufs * f->block_size); \
    }

/* Assert that the content of the test file has the given number of blocks, each
 * filled with progressive numbers. */
#define write__assert_content(N)                            \
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

#define write__close                 \
    {                                \
        uvFileClose(&f->file, NULL); \
        f->closed = true;            \
    }

/* Write a single buffer. */
TEST_CASE(write, success, one, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    write__invoke(0);
    write__wait_cb(1, f->block_size);

    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
TEST_CASE(write, success, two, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    write__complete;

    f->offset = f->block_size;

    memset(f->bufs[0].base, 2, f->bufs[0].len);

    write__complete;

    write__assert_content(2);

    return MUNIT_OK;
}

/* Write the same block twice. */
TEST_CASE(write, success, twice, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    memset(f->bufs[0].base, 0, f->bufs[0].len);

    write__complete;

    memset(f->bufs[0].base, 1, f->bufs[0].len);

    write__complete;

    write__assert_content(1);

    return MUNIT_OK;
}

/* Write a vector of buffers. */
TEST_CASE(write, success, vec, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    f->n_bufs = 2;

    write__complete;
    write__assert_content(2);

    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
TEST_CASE(write, success, vec_twice, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    f->n_bufs = 2;

    write__complete;
    write__complete;
    write__assert_content(2);

    return MUNIT_OK;
}

/* Write two different blocks concurrently. */
TEST_CASE(write, success, concurrent, dir_fs_supported_params)
{
    struct write_fixture *f = data;
    struct uvFileWrite req;
    int rv;

    (void)params;

    req.data = f;

    write__invoke(0);

    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, f->block_size, write_cb);
    munit_assert_int(rv, ==, 0);

    write__wait_cb(2, f->block_size);

    write__assert_content(2);

    return MUNIT_OK;
}

/* Write the same block concurrently. */
TEST_CASE(write, success, concurrent_twice, dir_fs_supported_params)
{
    struct write_fixture *f = data;
    struct uvFileWrite req;
    int rv;

    (void)params;

    req.data = f;

    memset(f->bufs[1].base, 1, f->bufs[1].len);

    write__invoke(0);

    rv = uvFileWrite(&f->file, &req, &f->bufs[1], 1, 0, write_cb);
    munit_assert_int(rv, ==, 0);

    write__wait_cb(2, f->block_size);

    write__assert_content(1);

    return MUNIT_OK;
}

/* There are not enough resources to create an AIO context to perform the
 * write. */
TEST_CASE(write, error, no_resources, dir_fs_no_aio_params)
{
    struct write_fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    write__invoke(0);
    write__wait_cb(1, UV_EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Cancel an inflight write. */
TEST_CASE(write, error, cancel, dir_fs_supported_params)
{
    struct write_fixture *f = data;

    (void)params;

    write__invoke(0);

    write__close;

    write__wait_cb(1, UV_ECANCELED);

    return MUNIT_OK;
}
