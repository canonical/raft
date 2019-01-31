#include "../../src/uv_fs.h"

#include "../lib/fs.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    char *dir;
    size_t block_size; /* File system block size */
    struct uv_loop_s loop;
    struct raft_uv_file file;
    struct raft_uv_fs req;
    bool completed; /* Whether the last write was completed */
    int status;     /* Result of the last write */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    f->dir = test_dir_setup(params);

    rv = raft_uv_fs__block_size(f->dir, &f->block_size);
    munit_assert_int(rv, ==, 0);

    rv = uv_loop_init(&f->loop);
    munit_assert_int(rv, ==, 0);

    f->req.data = f;

    f->completed = false;
    f->status = -1;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    rv = uv_loop_close(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_dir_tear_down(f->dir);

    free(f);
}

/**
 * Save the result of the request on the @status attribute of the fixture.
 */
static void __write_cb(struct raft_uv_fs *req)
{
    struct fixture *f = req->data;

    f->completed = true;
    f->status = req->status;
}

/**
 * Create a file and assert that no error occurs.
 */
#define __create(F)                                                            \
    {                                                                          \
        int rv;                                                                \
        char path[64];                                                         \
                                                                               \
        sprintf(path, "%s/foo", F->dir);                                       \
                                                                               \
        rv =                                                                   \
            raft_uv_fs__create(&f->file, &f->req, &f->loop, path, 8192, NULL); \
        munit_assert_int(rv, ==, 0);                                           \
                                                                               \
        rv = uv_run(&f->loop, UV_RUN_ONCE);                                    \
        munit_assert_int(rv, ==, 1);                                           \
                                                                               \
        munit_assert_int(f->req.status, ==, 0);                                \
    }

/**
 * Perform a write
 */
#define __write(F, BUFS, N, OFFSET)                                            \
    {                                                                          \
        int i;                                                                 \
        int rv;                                                                \
                                                                               \
        rv =                                                                   \
            raft_uv_fs__write(&F->file, &F->req, BUFS, N, OFFSET, __write_cb); \
        munit_assert_int(rv, ==, 0);                                           \
                                                                               \
        /* Run the loop until the write request is completed */                \
        for (i = 0; i < 5; i++) {                                              \
            rv = uv_run(&F->loop, UV_RUN_ONCE);                                \
            munit_assert_int(rv, ==, 1);                                       \
                                                                               \
            if (F->completed) {                                                \
                break;                                                         \
            }                                                                  \
        }                                                                      \
        munit_assert_true(F->completed);                                       \
        F->completed = false;                                                  \
    }

/**
 * Close the file handle of the given fixture and assert that no error occurs.
 */
#define __close(F)                                                      \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft_uv_fs__close(&f->file);                               \
        munit_assert_int(rv, ==, 0);                                    \
                                                                        \
        /* Run a single loop iteration to allow for the close callbacks \
         * to fire and for the poller handle to be inactive. */         \
        rv = uv_run(&f->loop, UV_RUN_ONCE);                             \
        munit_assert_int(rv, ==, 0);                                    \
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

    rv = raft_uv_fs__block_size(f->dir, &size);

    munit_assert_int(rv, ==, UV_EACCES);

    return MUNIT_OK;
}

/* Test against file systems that require a probe write. */
static char *test_block_size_no_space_dir_fs_type[] = {"btrfs", NULL};

static MunitParameterEnum test_block_size_no_space_params[] = {
    {TEST_DIR_FS_TYPE, test_block_size_no_space_dir_fs_type},
    {NULL, NULL},
};

/* No space is left on the target device.. */
static MunitResult test_block_size_no_space(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t size;
    int rv;

    (void)params;

    test_dir_fill(f->dir, 0);

    rv = raft_uv_fs__block_size(f->dir, &size);

    munit_assert_int(rv, ==, UV_ENOSPC);

    return MUNIT_OK;
}

static MunitTest block_size_tests[] = {
#if defined(RWF_NOWAIT)
    {"/unexecutable", test_block_size_unexecutable, setup, tear_down, 0, NULL},
    {"/no-space", test_block_size_no_space, setup, tear_down, 0,
     test_block_size_no_space_params},
#endif /* RWF_NOWAIT */
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_file__create
 */

/* Test against all file system types */
static MunitParameterEnum test_create_valid_path_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
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

    rv = raft_uv_fs__create(&f->file, &f->req, &f->loop, path, 4096, NULL);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 1);

    munit_assert_int(f->req.status, ==, 0);
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

    rv = raft_uv_fs__create(&f->file, &f->req, &f->loop, path, 4096, NULL);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->req.status, ==, UV_ENOENT);
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

    rv = raft_uv_fs__create(&f->file, &f->req, &f->loop, path, 4096, NULL);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->req.status, ==, UV_EEXIST);
    munit_assert_int(f->file.fd, ==, -1);

    return MUNIT_OK;
}

/* Test against all file system types except tmpfs and zfs. */
static char *test_create_no_space_dir_fs_type[] = {"btrfs", "ext4", "xfs",
                                                   NULL};

static MunitParameterEnum test_create_no_space_params[] = {
    {TEST_DIR_FS_TYPE, test_create_no_space_dir_fs_type},
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

    rv = raft_uv_fs__create(&f->file, &f->req, &f->loop, path, size, NULL);
    munit_assert_int(rv, ==, 0);

    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->req.status, ==, UV_ENOSPC);
    munit_assert_int(f->file.fd, ==, -1);

    return MUNIT_OK;
}

static MunitTest create_tests[] = {
    {"/valid-path", test_create_valid_path, setup, tear_down, 0,
     test_create_valid_path_params},
    {"/no-entry", test_create_no_entry, setup, tear_down, 0, NULL},
    {"/already-exists", test_create_already_exists, setup, tear_down, 0, NULL},
    {"/no-space", test_create_no_space, setup, tear_down, 0,
     test_create_no_space_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_fs__write
 */

/* Test against all file system types */
static MunitParameterEnum test_write_one_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write a single buffer. */
static MunitResult test_write_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    char *text;
    uv_buf_t buf;

    (void)params;

    __create(f);

    text = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(text);

    sprintf(text, "hello");

    buf.base = (void *)text;
    buf.len = f->block_size;

    __write(f, &buf, 1, 0);

    munit_assert_int(f->status, ==, f->block_size);

    test_dir_read_file(f->dir, "foo", text, f->block_size);
    munit_assert_string_equal(text, "hello");

    __close(f);

    free(text);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum test_write_two_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write two buffers, one after the other. */
static MunitResult test_write_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    char *text;
    uv_buf_t buf;

    (void)params;

    __create(f);

    text = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(text);

    sprintf(text, "hello");

    buf.base = (void *)text;
    buf.len = f->block_size;

    __write(f, &buf, 1, 0);

    munit_assert_int(f->status, ==, f->block_size);

    sprintf(text, "world");

    __write(f, &buf, 1, f->block_size);

    munit_assert_int(f->status, ==, f->block_size);

    test_dir_read_file(f->dir, "foo", text, f->block_size);
    munit_assert_string_equal(text, "hello");

    __close(f);

    free(text);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum test_write_twice_params[] = {
    {TEST_DIR_FS_TYPE, test_dir_fs_type_supported},
    {NULL, NULL},
};

/* Write the same block twice. */
static MunitResult test_write_twice(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    char *text;
    uv_buf_t buf;

    (void)params;

    __create(f);

    text = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(text);

    sprintf(text, "hello");

    buf.base = (void *)text;
    buf.len = f->block_size;

    __write(f, &buf, 1, 0);

    munit_assert_int(f->status, ==, f->block_size);

    sprintf(text, "hello world");

    __write(f, &buf, 1, 0);

    munit_assert_int(f->status, ==, f->block_size);

    test_dir_read_file(f->dir, "foo", text, f->block_size);
    munit_assert_string_equal(text, "hello world");

    __close(f);

    free(text);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum test_write_vec_params[] = {
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

    bufs[0].base = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(bufs[0].base);
    bufs[0].len = f->block_size;

    bufs[1].base = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(bufs[1].base);
    bufs[1].len = f->block_size;

    __write(f, bufs, 2, 0);

    munit_assert_int(f->status, ==, f->block_size * 2);

    __close(f);

    free(bufs[0].base);
    free(bufs[1].base);

    return MUNIT_OK;
}

/* Test against all file system types */
static MunitParameterEnum test_write_vec_twice_params[] = {
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

    bufs[0].base = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(bufs[0].base);
    bufs[0].len = f->block_size;

    bufs[1].base = aligned_alloc(f->block_size, f->block_size);
    munit_assert_ptr_not_null(bufs[1].base);
    bufs[1].len = f->block_size;

    __write(f, bufs, 1, 0);

    munit_assert_int(f->status, ==, f->block_size);

    __write(f, bufs, 2, 0);

    munit_assert_int(f->status, ==, f->block_size * 2);

    __close(f);

    free(bufs[0].base);
    free(bufs[1].base);

    return MUNIT_OK;
}

static MunitTest write_tests[] = {
    {"/one", test_write_one, setup, tear_down, 0, test_write_one_params},
    {"/two", test_write_two, setup, tear_down, 0, test_write_two_params},
    {"/twice", test_write_twice, setup, tear_down, 0, test_write_twice_params},
    {"/vec", test_write_vec, setup, tear_down, 0, test_write_vec_params},
    {"/vec-twice", test_write_vec_twice, setup, tear_down, 0,
     test_write_vec_twice_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_uv_fs_suites[] = {
    {"/block-size", block_size_tests, NULL, 1, 0},
    {"/create", create_tests, NULL, 1, 0},
    {"/write", write_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
