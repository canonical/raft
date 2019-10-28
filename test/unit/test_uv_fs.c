#include <unistd.h>

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

struct createFileResult
{
    int status;
    const char *errmsg;
    bool done;
};

static void createFileCbAssertOk(struct UvFsCreateFile *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    munit_assert_int(req->fd, >=, 0);
    munit_assert_int(close(req->fd), ==, 0);
    *done = true;
}

static void createFileCbAssertFail(struct UvFsCreateFile *req, int status)
{
    struct createFileResult *result = req->data;
    munit_assert_int(status, !=, 0);
    munit_assert_int(status, ==, result->status);
    munit_assert_string_equal(UvFsErrMsg(req->fs), result->errmsg);
    result->done = true;
}

/* Start creating a file with the given parameters and wait for the operation
 * to successfully complete. */
#define CREATE_FILE(DIR, FILENAME, SIZE)                         \
    {                                                            \
        struct UvFsCreateFile req_;                              \
        bool done_ = false;                                      \
        int rv_;                                                 \
        int i_;                                                  \
        req_.data = &done_;                                      \
        rv_ = UvFsCreateFile(&f->fs, &req_, DIR, FILENAME, SIZE, \
                             createFileCbAssertOk);              \
        munit_assert_int(rv_, ==, 0);                            \
        for (i_ = 0; i_ < 2; i_++) {                             \
            LOOP_RUN(1);                                         \
            if (done_) {                                         \
                break;                                           \
            }                                                    \
        }                                                        \
        munit_assert_true(done_);                                \
    }

/* Start creating a file with the given parameters and wait for the operation to
 * fail with the given code and message. */
#define CREATE_FILE_FAILURE(DIR, FILENAME, SIZE, STATUS, ERRMSG)   \
    {                                                              \
        struct UvFsCreateFile req_;                                \
        struct createFileResult result_ = {STATUS, ERRMSG, false}; \
        int rv_;                                                   \
        int i_;                                                    \
        req_.data = &result_;                                      \
        rv_ = UvFsCreateFile(&f->fs, &req_, DIR, FILENAME, SIZE,   \
                             createFileCbAssertFail);              \
        munit_assert_int(rv_, ==, 0);                              \
        for (i_ = 0; i_ < 2; i_++) {                               \
            LOOP_RUN(1);                                           \
            if (result_.done) {                                    \
                break;                                             \
            }                                                      \
        }                                                          \
        munit_assert_true(result_.done);                           \
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
    CREATE_FILE_FAILURE("/non/existing/dir", /* dir */
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
    CREATE_FILE_FAILURE(f->dir,    /* dir */
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
    CREATE_FILE_FAILURE(f->dir,       /* dir */
                        "foo",        /* filename */
                        4096 * 32768, /* size */
                        UV__ERROR,    /* status */
                        "posix_fallocate: No space left on device");
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/* Cancel a create file request just after having issued it, the request had
 * actually succeeded. */
TEST(UvFsCreateFile, cancelOk, setupFs, tearDownFs, 0, NULL)
{
    struct fs *f = data;
    struct UvFsCreateFile req;
    struct createFileResult result = {UV__CANCELED, "canceled", false};
    int rv;
    req.data = &result;
    rv = UvFsCreateFile(&f->fs, &req, "/non/existing/dir", "foo", 4096,
                        createFileCbAssertFail);
    munit_assert_int(rv, ==, 0);
    UvFsCreateFileCancel(&req);
    LOOP_RUN(1);
    munit_assert_true(result.done);
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}

/* Cancel a create file request just after having issued it, the request had
 * actually failed. */
TEST(UvFsCreateFile, cancelFail, setupFs, tearDownFs, 0, NULL)
{
    struct fs *f = data;
    struct UvFsCreateFile req;
    struct createFileResult result = {UV__CANCELED, "canceled", false};
    int rv;
    req.data = &result;
    rv = UvFsCreateFile(&f->fs, &req, f->dir, "foo", 4096,
                        createFileCbAssertFail);
    munit_assert_int(rv, ==, 0);
    UvFsCreateFileCancel(&req);
    LOOP_RUN(1);
    munit_assert_true(result.done);
    munit_assert_false(test_dir_has_file(f->dir, "foo"));
    return MUNIT_OK;
}
