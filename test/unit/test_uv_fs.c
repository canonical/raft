#include "../../src/uv_error.h"
#include "../../src/uv_fs.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * UvFsEnsureDir
 *
 *****************************************************************************/

/* Invoke UvFsEnsureDir passing it the given dir. */
#define ENSURE_DIR(DIR)                                       \
    {                                                         \
        struct ErrMsg errmsg;                                 \
        munit_assert_int(UvFsEnsureDir(DIR, &errmsg), ==, 0); \
    }

/* Invoke UvFsEnsureDir passing it the given dir and check that the given error
 * occurs. */
#define ENSURE_DIR_ERROR(DIR, RV, ERRMSG)                         \
    {                                                             \
        struct ErrMsg errmsg;                                     \
        munit_assert_int(UvFsEnsureDir(DIR, &errmsg), ==, RV);    \
        munit_assert_string_equal(ErrMsgString(&errmsg), ERRMSG); \
    }

SUITE(UvFsEnsureDir)

/* If the directory doesn't exist, it is created. */
TEST(UvFsEnsureDir, doesNotExist, setupDir, tearDownDir, 0, NULL)
{
    const char *parent = data;
    char dir[1024];
    sprintf(dir, "%s/sub", parent);
    ENSURE_DIR(dir);
    munit_assert_true(test_dir_exists(dir));
    return MUNIT_OK;
}

/* If the directory exists, nothing is needed. */
TEST(UvFsEnsureDir, exists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    ENSURE_DIR(dir);
    return MUNIT_OK;
}

/* If the directory can't be created, an error is returned. */
TEST(UvFsEnsureDir, mkdirError, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/foobarbazegg", UV__ERROR, "mkdir: permission denied");
    return MUNIT_OK;
}

/* If the directory can't be probed for existence, an error is returned. */
TEST(UvFsEnsureDir, statError, NULL, NULL, 0, NULL)
{
    bool has_access = test_dir_has_file("/proc/1", "root");
    /* Skip the test is the process actually has access to /proc/1/root. */
    if (has_access) {
        return MUNIT_SKIP;
    }
    ENSURE_DIR_ERROR("/proc/1/root", UV__ERROR, "stat: permission denied");
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST(UvFsEnsureDir, notDir, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/dev/null", UV__ERROR, "not a directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvFsSyncDir
 *
 *****************************************************************************/

/* Invoke UvFsSyncDir passing it the given dir. */
#define SYNC_DIR_ERROR(DIR, RV, ERRMSG)                           \
    {                                                             \
        struct ErrMsg errmsg;                                     \
        munit_assert_int(UvFsSyncDir(DIR, &errmsg), ==, RV);      \
        munit_assert_string_equal(ErrMsgString(&errmsg), ERRMSG); \
    }

SUITE(UvFsSyncDir)

/* If the directory doesn't exist, an error is returned. */
TEST(UvFsSyncDir, noExists, NULL, NULL, 0, NULL)
{
    SYNC_DIR_ERROR("/abcdef", UV__ERROR, "open directory: no such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvFsAllocateFile
 *
 *****************************************************************************/

/* Allocate a file with the given parameters and assert that no error occured.
 */
#define ALLOCATE_FILE(DIR, FILENAME, SIZE)                           \
    {                                                                \
        uv_file fd_;                                                 \
        struct ErrMsg errmsg_;                                       \
        int rv_;                                                     \
        rv_ = UvFsAllocateFile(DIR, FILENAME, SIZE, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, 0);                                \
        munit_assert_int(UvOsClose(fd_), ==, 0);                     \
    }

/* Assert that creating a file with the given parameters fails with the given
 * code and error message. */
#define ALLOCATE_FILE_ERROR(DIR, FILENAME, SIZE, RV, ERRMSG)         \
    {                                                                \
        uv_file fd_;                                                 \
        struct ErrMsg errmsg_;                                       \
        int rv_;                                                     \
        rv_ = UvFsAllocateFile(DIR, FILENAME, SIZE, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, RV);                               \
        munit_assert_string_equal(ErrMsgString(&errmsg_), ERRMSG);   \
    }

SUITE(UvFsAllocateFile)

/* If the given path is valid, the file gets created. */
TEST(UvFsAllocateFile, success, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    ALLOCATE_FILE(dir,   /* dir */
                  "foo", /* filename */
                  4096 /* size */);
    munit_assert_true(test_dir_has_file(dir, "foo"));
    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
TEST(UvFsAllocateFile, dirNoExists, NULL, NULL, 0, NULL)
{
    ALLOCATE_FILE_ERROR("/non/existing/dir", /* dir */
                        "foo",               /* filename */
                        64,                  /* size */
                        UV__ERROR,           /* status */
                        "open: no such file or directory");
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST(UvFsAllocateFile, fileAlreadyExists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    char buf[8];
    test_dir_write_file(dir, "foo", buf, sizeof buf);
    ALLOCATE_FILE_ERROR(dir,       /* dir */
                        "foo",     /* filename */
                        64,        /* size */
                        UV__ERROR, /* status */
                        "open: file already exists");
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST(UvFsAllocateFile, noSpace, setupDir, tearDownDir, 0, dir_tmpfs_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    ALLOCATE_FILE_ERROR(dir,          /* dir */
                        "foo",        /* filename */
                        4096 * 32768, /* size */
                        UV__ERROR,    /* status */
                        "posix_fallocate: no space left on device");
    munit_assert_false(test_dir_has_file(dir, "foo"));
    return MUNIT_OK;
}
