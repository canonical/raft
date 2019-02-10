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
    struct raft__uv_file_create create;
    struct raft__uv_file_write write;
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
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    f->dir = test_dir_setup(params);

    rv = raft__uv_file_block_size(f->dir, &f->block_size);
    munit_assert_int(rv, ==, 0);

    test_uv_setup(params, &f->loop);

    f->create.data = f;
    f->write.data = f;

    f->create_cb.invoked = 0;
    f->create_cb.status = -1;

    f->write_cb.invoked = 0;
    f->write_cb.status = -1;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_uv_tear_down(&f->loop);

    test_dir_tear_down(f->dir);

    free(f);
}

/**
 * Save the result of the request on the write_cb.status attribute of the
 * fixture and set write_cb.invoked to true.
 */
static void __write_cb(struct raft__uv_file_write *req, int status)
{
    struct fixture *f = req->data;

    f->write_cb.invoked++;
    f->write_cb.status = status;
}

static void __create_cb(struct raft__uv_file_create *req, int status)
{
    struct fixture *f = req->data;

    f->create_cb.invoked++;
    f->create_cb.status = status;
}

/**
 * Create a file and assert that no error occurs.
 */
#define __create(F)                                                           \
    {                                                                         \
        int rv;                                                               \
        char path[64];                                                        \
                                                                              \
        sprintf(path, "%s/foo", F->dir);                                      \
                                                                              \
        rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, 8192, \
                                  3, __create_cb);                            \
        munit_assert_int(rv, ==, 0);                                          \
                                                                              \
        rv = uv_run(&f->loop, UV_RUN_ONCE);                                   \
        munit_assert_int(rv, ==, 1);                                          \
                                                                              \
        munit_assert_int(f->create_cb.status, ==, 0);                         \
    }

/**
 * Perform a write
 */
#define __write(F, BUFS, N, OFFSET)                                    \
    {                                                                  \
        int i;                                                         \
        int rv;                                                        \
                                                                       \
        rv = raft__uv_file_write(&F->file, &F->write, BUFS, N, OFFSET, \
                                 __write_cb);                          \
        munit_assert_int(rv, ==, 0);                                   \
                                                                       \
        /* Run the loop until the write request is completed */        \
        for (i = 0; i < 5; i++) {                                      \
            rv = uv_run(&F->loop, UV_RUN_ONCE);                        \
            munit_assert_int(rv, ==, 1);                               \
                                                                       \
            if (F->write_cb.invoked > 0) {                             \
                break;                                                 \
            }                                                          \
        }                                                              \
        munit_assert_int(F->write_cb.invoked, ==, 1);                  \
        F->write_cb.invoked = 0;                                       \
    }

/**
 * Close the file handle of the given fixture and assert that no error occurs.
 */
#define __close(F)                                                      \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft__uv_file_close(&f->file);                             \
        munit_assert_int(rv, ==, 0);                                    \
                                                                        \
        /* Run a single loop iteration to allow for the close callbacks \
         * to fire and for the poller handle to be inactive. */         \
        rv = uv_run(&f->loop, UV_RUN_ONCE);                             \
        munit_assert_int(rv, ==, 0);                                    \
    }

/**
 * Allocate an aligned buffer of the size of one block and all its bytes with
 * the given character.
 */
#define __fill_buf(F, BUF, CHAR)                                \
    {                                                           \
        BUF.len = F->block_size;                                \
        BUF.base = aligned_alloc(F->block_size, F->block_size); \
        munit_assert_ptr_not_null(BUF.base);                    \
        memset(BUF.base, CHAR, BUF.len);                        \
    }

/**
 * Assert that the write callback was passed the given value as status
 * parameter.
 */
#define __assert_status(F, STATUS)                        \
    {                                                     \
        munit_assert_int(F->write_cb.status, ==, STATUS); \
    }

/**
 * Assert that the content of the file created with the __create() macro has the
 * given number of blocks, each filled with progressive numbers.
 */
#define __assert_content(F, N)                              \
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

/**
 * raft_uv__block_size
 */

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
static MunitResult test_block_size_unexecutable(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    size_t size;
    int rv;

    (void)params;

    test_dir_unexecutable(f->dir);

    rv = raft__uv_file_block_size(f->dir, &size);

    munit_assert_int(rv, ==, UV_EACCES);

    return MUNIT_OK;
}

/* Test against file systems that require a probe write. */
static char *block_size_no_space_dir_fs_type[] = {"btrfs", NULL};

static MunitParameterEnum block_size_no_space_params[] = {
    {TEST_DIR_FS_TYPE, block_size_no_space_dir_fs_type},
    {NULL, NULL},
};

/* No space is left on the target device. */
static MunitResult test_block_size_no_space(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t size;
    int rv;

    (void)params;

    test_dir_fill(f->dir, 0);

    rv = raft__uv_file_block_size(f->dir, &size);

    munit_assert_int(rv, ==, UV_ENOSPC);

    return MUNIT_OK;
}

/* Test against file systems that require a probe write. */
static char *block_size_no_resources_dir_fs_type[] = {"btrfs", NULL};

static MunitParameterEnum block_size_no_resources_params[] = {
    {TEST_DIR_FS_TYPE, block_size_no_resources_dir_fs_type},
    {NULL, NULL},
};

/* The io_setup() call fails with EAGAIN. */
static MunitResult test_block_size_no_resources(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    size_t size;
    int rv;

    (void)params;

    test_aio_fill(&ctx, 0);

    rv = raft__uv_file_block_size(f->dir, &size);
    munit_assert_int(rv, ==, UV_EAGAIN);

    rv = io_destroy(ctx);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest block_size_tests[] = {
#if defined(RWF_NOWAIT)
    {"/unexecutable", test_block_size_unexecutable, setup, tear_down, 0, NULL},
    {"/no-space", test_block_size_no_space, setup, tear_down, 0,
     block_size_no_space_params},
    {"/no-resources", test_block_size_no_resources, setup, tear_down, 0,
     block_size_no_resources_params},
#endif /* RWF_NOWAIT */
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_file__create
 */

char *foo[] = {"tmpfs", NULL};
/* Test against all file system types */
static MunitParameterEnum create_valid_path_params[] = {
    {TEST_DIR_FS_TYPE, foo},
    //{TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* If the given path is valid, the file gets opened. */
static MunitResult test_create_valid_path(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    char path[64];
    int rv;

    (void)params;

    sprintf(path, "%s/foo", f->dir);

    rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, 4096, 1,
                              __create_cb);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 1);

    munit_assert_int(f->create_cb.status, ==, 0);
    munit_assert_int(f->file.fd, >=, 0);
    munit_assert_int(f->file.event_fd, >=, 0);
    munit_assert_int(f->file.ctx, !=, 0);

    __close(f);

    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
static MunitResult test_create_no_entry(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    const char *path = "/non/existing/dir/foo";
    int rv;

    (void)params;

    rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, 4096, 1,
                              __create_cb);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->create_cb.status, ==, UV_ENOENT);
    munit_assert_int(f->file.fd, ==, -1);

    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
static MunitResult test_create_already_exists(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    char buf[8];
    char path[64];
    int rv;

    (void)params;

    test_dir_write_file(f->dir, "foo", buf, sizeof buf);

    sprintf(path, "%s/foo", f->dir);

    rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, 4096, 1,
                              __create_cb);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->create_cb.status, ==, UV_EEXIST);
    munit_assert_int(f->file.fd, ==, -1);

    return MUNIT_OK;
}

/* Test against all file system types except tmpfs and zfs. */
static char *create_no_space_dir_fs_type[] = {"btrfs", "ext4", "xfs", NULL};

static MunitParameterEnum create_no_space_params[] = {
    {TEST_DIR_FS_TYPE, create_no_space_dir_fs_type},
    {NULL, NULL},
};

/* The file system has run out of space. */
static MunitResult test_create_no_space(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    char path[64];
    size_t size = 4096 * 32768;
    int rv;

    (void)params;

    sprintf(path, "%s/foo", f->dir);

    rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, size, 1,
                              __create_cb);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->create_cb.status, ==, UV_ENOSPC);
    munit_assert_int(f->file.fd, ==, -1);

    return MUNIT_OK;
}

/* Test against all file system types except tmpfs and zfs. */
static char *create_no_resources_dir_fs_type[] = {"btrfs", "ext4", "xfs", NULL};

static MunitParameterEnum create_no_resources_params[] = {
    {TEST_DIR_FS_TYPE, create_no_resources_dir_fs_type},
    {NULL, NULL},
};

/* The kernel has ran out of available AIO events. */
static MunitResult test_create_no_resources(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    char path[64];
    size_t size = 4096;
    int rv;

    (void)params;

    test_aio_fill(&ctx, 0);
    sprintf(path, "%s/foo", f->dir);

    rv = raft__uv_file_create(&f->file, &f->create, &f->loop, path, size, 1,
                              __create_cb);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->create_cb.status, ==, UV_EAGAIN);
    munit_assert_int(f->file.fd, ==, -1);

    rv = io_destroy(ctx);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest create_tests[] = {
    {"/valid-path", test_create_valid_path, setup, tear_down, 0,
     create_valid_path_params},
    {"/no-entry", test_create_no_entry, setup, tear_down, 0, NULL},
    {"/already-exists", test_create_already_exists, setup, tear_down, 0, NULL},
    {"/no-space", test_create_no_space, setup, tear_down, 0,
     create_no_space_params},
    {"/no-resources", test_create_no_resources, setup, tear_down, 0,
     create_no_resources_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_fs__write
 */

/* Test against all file system types */
static MunitParameterEnum write_one_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write a single buffer. */
static MunitResult test_write_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uv_buf_t buf;

    (void)params;

    __create(f);

    __fill_buf(f, buf, 1);

    __write(f, &buf, 1, 0);

    __assert_status(f, f->block_size);
    __assert_content(f, 1);

    __close(f);

    free(buf.base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_two_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write two buffers, one after the other. */
static MunitResult test_write_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uv_buf_t buf1;
    uv_buf_t buf2;

    (void)params;

    __create(f);

    __fill_buf(f, buf1, 1);
    __fill_buf(f, buf2, 2);

    __write(f, &buf1, 1, 0);

    __assert_status(f, f->block_size);

    __write(f, &buf2, 1, f->block_size);

    __assert_status(f, f->block_size);

    __assert_content(f, 2);

    __close(f);

    free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_twice_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write the same block twice. */
static MunitResult test_write_twice(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uv_buf_t buf1;
    uv_buf_t buf2;

    (void)params;

    __create(f);

    __fill_buf(f, buf1, 0);
    __fill_buf(f, buf2, 1);

    __write(f, &buf1, 1, 0);
    __write(f, &buf2, 1, 0);

    __assert_status(f, f->block_size);
    __assert_content(f, 1);

    __close(f);

    free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_vec_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write a vector of buffers. */
static MunitResult test_write_vec(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uv_buf_t bufs[2];

    (void)params;

    __create(f);

    __fill_buf(f, bufs[0], 1);
    __fill_buf(f, bufs[1], 2);

    __write(f, bufs, 2, 0);

    __assert_status(f, f->block_size * 2);

    __close(f);

    free(bufs[0].base);
    free(bufs[1].base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_vec_twice_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write a vector of buffers twice. */
static MunitResult test_write_vec_twice(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    uv_buf_t bufs[2];

    (void)params;

    __create(f);

    __fill_buf(f, bufs[0], 1);
    __fill_buf(f, bufs[1], 2);

    __write(f, bufs, 1, 0);

    __assert_status(f, f->block_size);

    __write(f, bufs, 2, 0);

    __assert_status(f, f->block_size * 2);

    __close(f);

    free(bufs[0].base);
    free(bufs[1].base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_concurrent_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write two different blocks concurrently. */
static MunitResult test_write_concurrent(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft__uv_file_write req;
    uv_buf_t buf1;
    uv_buf_t buf2;
    int rv;
    unsigned i;

    (void)params;

    __create(f);

    req.data = f;

    __fill_buf(f, buf1, 1);
    __fill_buf(f, buf2, 2);

    rv = raft__uv_file_write(&f->file, &f->write, &buf1, 1, 0, __write_cb);
    munit_assert_int(rv, ==, 0);

    rv = raft__uv_file_write(&f->file, &req, &buf2, 1, f->block_size,
                             __write_cb);
    munit_assert_int(rv, ==, 0);

    /* Run the loop until the write request is completed */
    for (i = 0; i < 10; i++) {
        rv = uv_run(&f->loop, UV_RUN_ONCE);
        munit_assert_int(rv, ==, 1);

        if (f->write_cb.invoked == 2) {
            break;
        }
    }

    munit_assert_int(f->write_cb.invoked, ==, 2);

    __assert_content(f, 2);

    __close(f);

    free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum write_concurrent_twice_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write the same block concurrently. */
static MunitResult test_write_concurrent_twice(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    struct raft__uv_file_write req;
    uv_buf_t buf1;
    uv_buf_t buf2;
    int rv;
    unsigned i;

    (void)params;

    __create(f);

    req.data = f;

    __fill_buf(f, buf1, 1);
    __fill_buf(f, buf2, 1);

    rv = raft__uv_file_write(&f->file, &f->write, &buf1, 1, 0, __write_cb);
    munit_assert_int(rv, ==, 0);

    rv = raft__uv_file_write(&f->file, &req, &buf2, 1, 0, __write_cb);
    munit_assert_int(rv, ==, 0);

    /* Run the loop until the write request is completed */
    for (i = 0; i < 10; i++) {
        rv = uv_run(&f->loop, UV_RUN_ONCE);
        munit_assert_int(rv, ==, 1);

        if (f->write_cb.invoked == 2) {
            break;
        }
    }

    munit_assert_int(f->write_cb.invoked, ==, 2);

    __assert_content(f, 1);

    __close(f);

    free(buf1.base);
    free(buf2.base);

    return MUNIT_OK;
}

/* Test against all file system types that do not support fully async AIO. */
static char *write_no_resources_dir_fs_type[] = {"tmpfs", "zfs", NULL};

static MunitParameterEnum write_no_resources_params[] = {
    {TEST_DIR_FS_TYPE, write_no_resources_dir_fs_type},
    {NULL, NULL},
};

/* There are not enough resources to create an AIO context to perform the
 * write. */
static MunitResult test_write_no_resources(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    uv_buf_t buf;
    int rv;

    (void)params;

    __create(f);

    test_aio_fill(&ctx, 0);

    __fill_buf(f, buf, 0);

    __write(f, &buf, 1, 0);

    __assert_status(f, UV_EAGAIN);

    __close(f);

    free(buf.base);

    rv = io_destroy(ctx);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest write_tests[] = {
    {"/one", test_write_one, setup, tear_down, 0, write_one_params},
    {"/two", test_write_two, setup, tear_down, 0, write_two_params},
    {"/twice", test_write_twice, setup, tear_down, 0, write_twice_params},
    {"/vec", test_write_vec, setup, tear_down, 0, write_vec_params},
    {"/vec-twice", test_write_vec_twice, setup, tear_down, 0,
     write_vec_twice_params},
    {"/concurrent", test_write_concurrent, setup, tear_down, 0,
     write_concurrent_params},
    {"/concurrent-twice", test_write_concurrent_twice, setup, tear_down, 0,
     write_concurrent_twice_params},
    {"/no-resources", test_write_no_resources, setup, tear_down, 0,
     write_no_resources_params},
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
