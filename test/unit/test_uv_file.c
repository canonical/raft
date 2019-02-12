#include "../../src/aio.h"
#include "../../src/uv_file.h"

#include "../lib/fs.h"
#include "../lib/munit.h"
#include "../lib/uv.h"

/**
 * Helpers
 */

struct fixture
{
    char *dir;
    size_t block_size; /* File system block size */
    struct uv_loop_s loop;
    struct raft__uv_file file;
    struct
    {
        char path[64];         /* Path of the file to create */
        size_t size;           /* Size of the file to create */
        unsigned max_n_writes; /* Max n of writes of the file to create */
    } create_args;
    struct
    {
        uv_buf_t bufs[2];
        unsigned n_bufs;
        size_t offset;
    } write_args;
    struct
    {
        int invoked; /* Number of times the write callback was invoked */
        int status;  /* Result passed to the last callback invokation */
    } create_cb;
    struct
    {
        int invoked; /* Number of times the write callback was invoked */
        int status;  /* Result passed to the last callback invokation */
    } write_cb;
    struct
    {
        int invoked; /* Number of times the close callback was invoked */
    } close_cb;
};

static void __create_cb(struct raft__uv_file_create *req, int status)
{
    struct fixture *f = req->data;

    f->create_cb.invoked++;
    f->create_cb.status = status;

    free(req);
}

static void __write_cb(struct raft__uv_file_write *req, int status)
{
    struct fixture *f = req->data;

    f->write_cb.invoked++;
    f->write_cb.status = status;

    free(req);
}

static void __close_cb(struct raft__uv_file *file)
{
    struct fixture *f = file->data;

    f->close_cb.invoked++;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int i;
    int rv;

    (void)user_data;

    f->dir = test_dir_setup(params);

    rv = raft__uv_file_block_size(f->dir, &f->block_size);
    munit_assert_int(rv, ==, 0);

    test_uv_setup(params, &f->loop);

    rv = raft__uv_file_init(&f->file, &f->loop);
    munit_assert_int(rv, ==, 0);

    f->file.data = f;

    sprintf(f->create_args.path, "%s/foo", f->dir);
    f->create_args.size = 4096;
    f->create_args.max_n_writes = 1;

    for (i = 0; i < 2; i++) {
        uv_buf_t *buf = &f->write_args.bufs[i];
        buf->len = f->block_size;
        buf->base = aligned_alloc(f->block_size, f->block_size);
        munit_assert_ptr_not_null(buf->base);
        memset(buf->base, i + 1, buf->len);
    }

    f->write_args.n_bufs = 1; /* By default write only one of the two buffers */
    f->write_args.offset = 0;

    f->create_cb.invoked = 0;
    f->create_cb.status = -1;

    f->write_cb.invoked = 0;
    f->write_cb.status = -1;

    f->close_cb.invoked = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int i;

    if (!raft__uv_file_is_closing(&f->file)) {
        raft__uv_file_close(&f->file, __close_cb);
    }

    test_uv_stop(&f->loop);

    munit_assert_int(f->close_cb.invoked, ==, 1);

    for (i = 0; i < 2; i++) {
        free(f->write_args.bufs[i].base);
    }

    test_uv_tear_down(&f->loop);

    test_dir_tear_down(f->dir);

    free(f);
}

/**
 * Assert that @raft__uv_file_block_size returns the given code.
 */
#define __block_size_assert_result(F, RV)             \
    {                                                 \
        size_t size;                                  \
        int rv;                                       \
                                                      \
        rv = raft__uv_file_block_size(F->dir, &size); \
        munit_assert_int(rv, ==, RV);                 \
    }

/**
 * Assert that @raft__uv_file_create returns the given code.
 */
#define __create_assert_result(F, RV)                                        \
    {                                                                        \
        struct raft__uv_file_create *req;                                    \
        int rv;                                                              \
                                                                             \
        req = munit_malloc(sizeof *req);                                     \
        req->data = F;                                                       \
                                                                             \
        rv = raft__uv_file_create(&F->file, req, F->create_args.path,        \
                                  F->create_args.size,                       \
                                  F->create_args.max_n_writes, __create_cb); \
        munit_assert_int(rv, ==, RV);                                        \
                                                                             \
        if (rv != 0) {                                                       \
            free(req);                                                       \
        }                                                                    \
    }

/**
 * Wait for the create callback to fire and check its status.
 */
#define __create_assert_cb(F, STATUS)                            \
    {                                                            \
        int i;                                                   \
                                                                 \
        /* Run the loop until the create request is completed */ \
        for (i = 0; i < 2; i++) {                                \
            test_uv_run(&F->loop, 1);                            \
                                                                 \
            if (F->create_cb.invoked == 1) {                     \
                break;                                           \
            }                                                    \
        }                                                        \
                                                                 \
        munit_assert_int(F->create_cb.invoked, ==, 1);           \
        munit_assert_int(F->create_cb.status, ==, STATUS);       \
    }

/**
 * Submit a create file request, wait for its completion and assert that no
 * error occurs.
 */
#define __create(F)                   \
    {                                 \
        __create_assert_result(F, 0); \
        __create_assert_cb(F, 0);     \
    }

/**
 * Assert that @raft__uv_file_write returns the given code.
 */
#define __write_assert_result(F, RV)                                         \
    {                                                                        \
        struct raft__uv_file_write *req;                                     \
        int rv;                                                              \
                                                                             \
        req = munit_malloc(sizeof *req);                                     \
        req->data = F;                                                       \
                                                                             \
        rv = raft__uv_file_write(&F->file, req, F->write_args.bufs,          \
                                 F->write_args.n_bufs, F->write_args.offset, \
                                 __write_cb);                                \
        munit_assert_int(rv, ==, RV);                                        \
                                                                             \
        if (rv != 0) {                                                       \
            free(req);                                                       \
        }                                                                    \
    }

/**
 * Wait for a write callback to fire N times and check its last status.
 */
#define __write_assert_cb(F, N, STATUS)                         \
    {                                                           \
        int i;                                                  \
                                                                \
        /* Run the loop until the write request is completed */ \
        for (i = 0; i < 5; i++) {                               \
            test_uv_run(&F->loop, 1);                           \
                                                                \
            if (F->write_cb.invoked == N) {                     \
                break;                                          \
            }                                                   \
        }                                                       \
        munit_assert_int(F->write_cb.invoked, ==, N);           \
        munit_assert_int(F->write_cb.status, ==, STATUS);       \
                                                                \
        F->write_cb.invoked = 0;                                \
        F->write_cb.status = -1;                                \
    }

/**
 * Submit a write file request, wait for its completion and assert that no
 * error occurs.
 */
#define __write(F)                                                     \
    {                                                                  \
        __write_assert_result(F, 0);                                   \
        __write_assert_cb(F, 1, F->write_args.n_bufs * F->block_size); \
    }

/**
 * Assert that the content of the test file has the given number of blocks, each
 * filled with progressive numbers.
 */
#define __write_assert_content(F, N)                        \
    {                                                       \
        size_t size = N * F->block_size;                    \
        void *buf = munit_malloc(size);                     \
        unsigned i;                                         \
        unsigned j;                                         \
                                                            \
        test_dir_read_file(F->dir, "foo", buf, size);       \
                                                            \
        for (i = 0; i < N; i++) {                           \
            char *cursor = (char *)buf + i * F->block_size; \
            for (j = 0; j < F->block_size; j++) {           \
                munit_assert_int(cursor[j], ==, i + 1);     \
            }                                               \
        }                                                   \
                                                            \
        free(buf);                                          \
    }

#define __close(F)                                 \
    {                                              \
        raft__uv_file_close(&F->file, __close_cb); \
    }

/**
 * raft__uv_file_block_size
 */

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
static MunitResult test_block_size_no_access(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_unexecutable(f->dir);

    __block_size_assert_result(f, UV_EACCES);

    return MUNIT_OK;
}

/* No space is left on the target device. */
static MunitResult test_block_size_no_space(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_fill(f->dir, 0);

    __block_size_assert_result(f, UV_ENOSPC);

    return MUNIT_OK;
}

/* The io_setup() call fails with EAGAIN. */
static MunitResult test_block_size_no_resources(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    __block_size_assert_result(f, UV_EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

#define __test_block_size(NAME, FUNC, PARAMS)                         \
    {                                                                 \
        "/" NAME, test_block_size_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest block_size_tests[] = {
#if defined(RWF_NOWAIT)
    __test_block_size("no-access", no_access, NULL),
    __test_block_size("no-space", no_space, dir_fs_btrfs_params),
    __test_block_size("no-resources", no_resources, dir_fs_btrfs_params),
#endif /* RWF_NOWAIT */
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__uv_file_create
 */

/* If the given path is valid, the file gets opened. */
static MunitResult test_create_valid_path(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
static MunitResult test_create_no_entry(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    sprintf(f->create_args.path, "/non/existing/dir/foo");

    (void)params;

    __create_assert_result(f, UV_ENOENT);

    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
static MunitResult test_create_already_exists(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    char buf[8];

    (void)params;

    test_dir_write_file(f->dir, "foo", buf, sizeof buf);

    __create_assert_result(f, UV_EEXIST);

    return MUNIT_OK;
}

/* The file system has run out of space. */
static MunitResult test_create_no_space(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    f->create_args.size = 4096 * 32768;

    __create_assert_result(f, 0);
    __create_assert_cb(f, UV_ENOSPC);

    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
static MunitResult test_create_no_resources(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    __create_assert_result(f, UV_EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Close a file just after having issued a create request. */
static MunitResult test_create_cancel(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create_assert_result(f, 0);
    __close(f);

    __create_assert_cb(f, UV_ECANCELED);

    return MUNIT_OK;
}

#define __test_create(NAME, FUNC, PARAMS)                         \
    {                                                             \
        "/" NAME, test_create_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest create_tests[] = {
    __test_create("valid-path", valid_path, dir_fs_supported_params),
    __test_create("no-entry", no_entry, NULL),
    __test_create("already-exists", already_exists, NULL),
    __test_create("no-space", no_space, NULL),
    __test_create("no-resources", no_resources, NULL),
    __test_create("cancel", cancel, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__uv_file_write
 */

/* Write a single buffer. */
static MunitResult test_write_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    __write(f);
    __write_assert_content(f, 1);

    return MUNIT_OK;
}

/* Write two buffers, one after the other. */
static MunitResult test_write_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    __write(f);

    f->write_args.offset = f->block_size;

    memset(f->write_args.bufs[0].base, 2, f->write_args.bufs[0].len);

    __write(f);

    __write_assert_content(f, 2);

    return MUNIT_OK;
}

/* Write the same block twice. */
static MunitResult test_write_twice(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    memset(f->write_args.bufs[0].base, 0, f->write_args.bufs[0].len);

    __write(f);

    memset(f->write_args.bufs[0].base, 1, f->write_args.bufs[0].len);

    __write(f);

    __write_assert_content(f, 1);

    return MUNIT_OK;
}

/* Write a vector of buffers. */
static MunitResult test_write_vec(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    f->write_args.n_bufs = 2;

    __write(f);
    __write_assert_content(f, 2);

    return MUNIT_OK;
}

/* Write a vector of buffers twice. */
static MunitResult test_write_vec_twice(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    f->write_args.n_bufs = 2;

    __write(f);
    __write(f);
    __write_assert_content(f, 2);

    return MUNIT_OK;
}

/* Write two different blocks concurrently. */
static MunitResult test_write_concurrent(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft__uv_file_write *req;
    int rv;

    (void)params;

    req = munit_malloc(sizeof *req);
    req->data = f;

    f->create_args.max_n_writes = 2;

    __create(f);

    __write_assert_result(f, 0);

    rv = raft__uv_file_write(&f->file, req, &f->write_args.bufs[1], 1,
                             f->block_size, __write_cb);
    munit_assert_int(rv, ==, 0);

    __write_assert_cb(f, 2, f->block_size);

    __write_assert_content(f, 2);

    return MUNIT_OK;
}

/* Write the same block concurrently. */
static MunitResult test_write_concurrent_twice(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    struct raft__uv_file_write *req;
    int rv;

    (void)params;

    req = munit_malloc(sizeof *req);
    req->data = f;

    f->create_args.max_n_writes = 2;

    memset(f->write_args.bufs[1].base, 1, f->write_args.bufs[1].len);

    __create(f);

    __write_assert_result(f, 0);

    rv = raft__uv_file_write(&f->file, req, &f->write_args.bufs[1], 1, 0,
                             __write_cb);
    munit_assert_int(rv, ==, 0);

    __write_assert_cb(f, 2, f->block_size);

    __write_assert_content(f, 1);

    return MUNIT_OK;
}

/* There are not enough resources to create an AIO context to perform the
 * write. */
static MunitResult test_write_no_resources(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    __create(f);

    test_aio_fill(&ctx, 0);

    __write_assert_result(f, 0);
    __write_assert_cb(f, 1, UV_EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Cancel an inflight write. */
static MunitResult test_write_cancel(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __create(f);

    __write_assert_result(f, 0);

    __close(f);

    __write_assert_cb(f, 1, UV_ECANCELED);

    return MUNIT_OK;
}

#define __test_write(NAME, FUNC, PARAMS)                         \
    {                                                            \
        "/" NAME, test_write_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest write_tests[] = {
    __test_write("one", one, dir_fs_supported_params),
    __test_write("two", two, dir_fs_supported_params),
    __test_write("twice", twice, dir_fs_supported_params),
    __test_write("vec", vec, dir_fs_supported_params),
    __test_write("vec-twice", vec_twice, dir_fs_supported_params),
    __test_write("concurrent", concurrent, dir_fs_supported_params),
    __test_write("concurrent-twice", concurrent_twice, dir_fs_supported_params),
    __test_write("no-resources", no_resources, dir_fs_no_aio_params),
    __test_write("cancel", cancel, dir_fs_supported_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_uv_file_suites[] = {
    {"/block-size", block_size_tests, NULL, 1, 0},
    {"/create", create_tests, NULL, 1, 0},
    {"/write", write_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
