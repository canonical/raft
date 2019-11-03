#include "../../src/uv_error.h"
#include "../../src/uv_fs.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * UvFsCreateFile
 *
 *****************************************************************************/

/* Create a file with the given parameters and assert that no error occured. */
#define CREATE_FILE(DIR, FILENAME, SIZE)                           \
    {                                                              \
        uv_file fd_;                                               \
        struct ErrMsg errmsg_;                                     \
        int rv_;                                                   \
        rv_ = UvFsCreateFile(DIR, FILENAME, SIZE, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, 0);                              \
        munit_assert_int(UvOsClose(fd_), ==, 0);                   \
    }

/* Assert that creating a file with the given parameters fails with the given
 * code and error message. */
#define CREATE_FILE_ERROR(DIR, FILENAME, SIZE, RV, ERRMSG)         \
    {                                                              \
        uv_file fd_;                                               \
        struct ErrMsg errmsg_;                                     \
        int rv_;                                                   \
        rv_ = UvFsCreateFile(DIR, FILENAME, SIZE, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, RV);                             \
        munit_assert_string_equal(ErrMsgString(&errmsg_), ERRMSG); \
    }

SUITE(UvFsCreateFile)

/* If the given path is valid, the file gets created. */
TEST(UvFsCreateFile, success, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    CREATE_FILE(dir,   /* dir */
                "foo", /* filename */
                4096 /* size */);
    munit_assert_true(test_dir_has_file(dir, "foo"));
    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
TEST(UvFsCreateFile, dirNoExists, NULL, NULL, 0, NULL)
{
    CREATE_FILE_ERROR("/non/existing/dir", /* dir */
                      "foo",               /* filename */
                      64,                  /* size */
                      UV__ERROR,           /* status */
                      "open: no such file or directory");
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST(UvFsCreateFile, fileAlreadyExists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    char buf[8];
    test_dir_write_file(dir, "foo", buf, sizeof buf);
    CREATE_FILE_ERROR(dir,       /* dir */
                      "foo",     /* filename */
                      64,        /* size */
                      UV__ERROR, /* status */
                      "open: file already exists");
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST(UvFsCreateFile, noSpace, setupDir, tearDownDir, 0, dir_tmpfs_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    CREATE_FILE_ERROR(dir,          /* dir */
                      "foo",        /* filename */
                      4096 * 32768, /* size */
                      UV__ERROR,    /* status */
                      "posix_fallocate: no space left on device");
    munit_assert_false(test_dir_has_file(dir, "foo"));
    return MUNIT_OK;
}
