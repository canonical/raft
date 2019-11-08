#include "../../src/uv_error.h"
#include "../../src/uv_os.h"
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

/* If the directory exists, the function suceeds. */
TEST(UvFsEnsureDir, exists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    ENSURE_DIR(dir);
    return MUNIT_OK;
}

/* If the directory doesn't exist, it an error is returned. */
TEST(UvFsEnsureDir, doesNotExist, setupDir, tearDownDir, 0, NULL)
{
    const char *parent = data;
    char dir[1024];
    sprintf(dir, "%s/sub", parent);
    ENSURE_DIR_ERROR(dir, RAFT_IOERR, "stat: no such file or directory");
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
    ENSURE_DIR_ERROR("/proc/1/root", RAFT_IOERR, "stat: permission denied");
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST(UvFsEnsureDir, notDir, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/dev/null", RAFT_IOERR, "not a directory");
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
    SYNC_DIR_ERROR("/abcdef", UV__ERROR,
                   "open directory: no such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvFsOpenFileForReading
 *
 *****************************************************************************/

/* Open a file in the given dir. */
#define OPEN_FILE_FOR_READING_ERROR(DIR, FILENAME, RV, ERRMSG)           \
    {                                                                    \
        uv_file fd_;                                                     \
        struct ErrMsg errmsg_;                                           \
        int rv_ = UvFsOpenFileForReading(DIR, FILENAME, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, RV);                                   \
        munit_assert_string_equal(ErrMsgString(&errmsg_), ERRMSG);       \
    }

SUITE(UvFsOpenFileForReading)

/* If the directory doesn't exist, an error is returned. */
TEST(UvFsOpenFileForReading, noExists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    OPEN_FILE_FOR_READING_ERROR(dir, "foo", UV__ERROR,
                                "open: no such file or directory");
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

/******************************************************************************
 *
 * UvFsProbeCapabilities
 *
 *****************************************************************************/

/* Invoke UvFsProbeCapabilities against the given dir and assert that it returns
 * the given values for direct I/O and async I/O. */
#define PROBE_CAPABILITIES(DIR, DIRECT_IO, ASYNC_IO)                         \
    {                                                                        \
        size_t direct_io_;                                                   \
        bool async_io_;                                                      \
        struct ErrMsg errmsg_;                                               \
        int rv_;                                                             \
        rv_ = UvFsProbeCapabilities(DIR, &direct_io_, &async_io_, &errmsg_); \
        munit_assert_int(rv_, ==, 0);                                        \
        munit_assert_int(direct_io_, ==, DIRECT_IO);                         \
        if (ASYNC_IO) {                                                      \
            munit_assert_true(async_io_);                                    \
        } else {                                                             \
            munit_assert_false(async_io_);                                   \
        }                                                                    \
    }

/* Invoke UvFsProbeCapabilities and check that the given error occurs. */
#define PROBE_CAPABILITIES_ERROR(DIR, RV, ERRMSG)                            \
    {                                                                        \
        size_t direct_io_;                                                   \
        bool async_io_;                                                      \
        struct ErrMsg errmsg_;                                               \
        int rv_;                                                             \
        rv_ = UvFsProbeCapabilities(DIR, &direct_io_, &async_io_, &errmsg_); \
        munit_assert_int(rv_, ==, RV);                                       \
        munit_assert_string_equal(ErrMsgString(&errmsg_), ERRMSG);           \
    }

SUITE(UvFsProbeCapabilities)

TEST(UvFsProbeCapabilities, tmpfs, setupTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES(dir, 0, false);
    return MUNIT_OK;
}

/* ZFS 0.8 reports that it supports direct I/O, but does not support fully
 * asynchronous kernel AIO. */
TEST(UvFsProbeCapabilities, zfsDirectIO, setupZfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    size_t direct_io = 0;
#if defined(RAFT_HAVE_ZFS_WITH_DIRECT_IO)
    direct_io = 4096;
#endif
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES(dir, direct_io, false);
    return MUNIT_OK;
}

#if defined(RWF_NOWAIT)

/* File systems that fully support DIO. */
TEST(UvFsProbeCapabilities, aio, setupDir, tearDownDir, 0, dir_aio_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES(dir, 4096, true);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST(UvFsProbeCapabilities, noAccess, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    test_dir_unexecutable(dir);
    PROBE_CAPABILITIES_ERROR(dir, RAFT_IOERR, "mkstemp: permission denied");
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST(UvFsProbeCapabilities, noSpace, setupTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    test_dir_fill(dir, 0);
    PROBE_CAPABILITIES_ERROR(dir, RAFT_IOERR,
                             "posix_fallocate: no space left on device");
    return MUNIT_OK;
}

#if defined(RWF_NOWAIT)

/* The uvIoSetup() call fails with EAGAIN. */
TEST(UvFsProbeCapabilities, noResources, setupBtrfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    aio_context_t ctx = 0;
    int rv;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES_ERROR(dir, RAFT_IOERR,
                             "io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */
