#include "../../src/uv_error.h"
#include "../../src/uv_fs.h"
#include "../lib/dir.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a plain UvFs object along with a test dir.
 *
 *****************************************************************************/

struct fs
{
    FIXTURE_DIR;
    FIXTURE_LOOP;
    struct UvFs fs;
};

/* Initialize a the fixture's UvFs object. */
static void *setupFs(MUNIT_UNUSED const MunitParameter params[],
                     MUNIT_UNUSED void *user_data)
{
    struct fs *f = munit_malloc(sizeof *f);
    SETUP_DIR_OR_SKIP;
    SETUP_LOOP;
    UvFsInit(&f->fs, &f->loop);
    return f;
}

/* Cleanup the fixture's UvFs object. */
static void tearDownFs(void *data)
{
    struct fs *f = data;
    if (f == NULL) {
        return; /* Was skipped. */
    }
    UvFsClose(&f->fs);
    TEAR_DOWN_LOOP;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * UvFsCreateFile
 *
 *****************************************************************************/

/* Create a file with the given parameters and assert that no error occured. */
#define CREATE_FILE(DIR, FILENAME, SIZE)                          \
    {                                                             \
        uv_file fd_;                                              \
        int rv_;                                                  \
        rv_ = UvFsCreateFile2(&f->fs, DIR, FILENAME, SIZE, &fd_); \
        munit_assert_int(rv_, ==, 0);                             \
        munit_assert_int(UvOsClose(fd_), ==, 0);                  \
    }

/* Assert that creating a file with the given parameters fails with the given
 * code and error message. */
#define CREATE_FILE_ERROR(DIR, FILENAME, SIZE, RV, ERRMSG)        \
    {                                                             \
        uv_file fd_;                                              \
        int rv_;                                                  \
        rv_ = UvFsCreateFile2(&f->fs, DIR, FILENAME, SIZE, &fd_); \
        munit_assert_int(rv_, ==, RV);                            \
        munit_assert_string_equal(UvFsErrMsg(&f->fs), ERRMSG);    \
    }

SUITE(UvFsCreateFile)

/* If the given path is valid, the file gets created. */
TEST(UvFsCreateFile, success, setupFs, tearDownFs, 0, NULL)
{
    struct fs *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_FILE(f->dir, /* dir */
                "foo",  /* filename */
                4096 /* size */);
    munit_assert_true(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/* The directory of given path does not exist, an error is returned. */
TEST(UvFsCreateFile, dirNoExists, setupFs, tearDownFs, 0, NULL)
{
    struct fs *f = data;
    CREATE_FILE_ERROR("/non/existing/dir", /* dir */
                      "foo",               /* filename */
                      64,                  /* size */
                      UV__ERROR,           /* status */
                      "open: no such file or directory");
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST(UvFsCreateFile, fileAlreadyExists, setupFs, tearDownFs, 0, NULL)
{
    struct fs *f = data;
    char buf[8];
    test_dir_write_file(f->dir, "foo", buf, sizeof buf);
    CREATE_FILE_ERROR(f->dir,    /* dir */
                      "foo",     /* filename */
                      64,        /* size */
                      UV__ERROR, /* status */
                      "open: file already exists");
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST(UvFsCreateFile, noSpace, setupFs, tearDownFs, 0, dir_tmpfs_params)
{
    struct fs *f = data;
    SKIP_IF_NO_FIXTURE;
    CREATE_FILE_ERROR(f->dir,       /* dir */
                      "foo",        /* filename */
                      4096 * 32768, /* size */
                      UV__ERROR,    /* status */
                      "posix_fallocate: no space left on device");
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}
