#include "../../src/uv_fs.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * UvFsCheckDir
 *
 *****************************************************************************/

/* Invoke UvFsCheckDir passing it the given dir. */
#define CHECK_DIR(DIR)                      \
    {                                       \
        int _rv;                            \
        char _errmsg[RAFT_ERRMSG_BUF_SIZE]; \
        _rv = UvFsCheckDir(DIR, _errmsg);   \
        munit_assert_int(_rv, ==, 0);       \
    }

/* Invoke UvFsCheckDir passing it the given dir and check that the given error
 * occurs. */
#define CHECK_DIR_ERROR(DIR, RV, ERRMSG)            \
    {                                               \
        int _rv;                                    \
        char _errmsg[RAFT_ERRMSG_BUF_SIZE];         \
        _rv = UvFsCheckDir(DIR, _errmsg);           \
        munit_assert_int(_rv, ==, RV);              \
        munit_assert_string_equal(_errmsg, ERRMSG); \
    }

SUITE(UvFsCheckDir)

/* If the directory exists, the function suceeds. */
TEST(UvFsCheckDir, exists, setUpDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    CHECK_DIR(dir);
    return MUNIT_OK;
}

/* If the directory doesn't exist, it an error is returned. */
TEST(UvFsCheckDir, doesNotExist, setUpDir, tearDownDir, 0, NULL)
{
    const char *parent = data;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    char dir[128];
    sprintf(errmsg, "%s/sub", parent);
    sprintf(errmsg, "directory '%s' does not exist", dir);
    CHECK_DIR_ERROR(dir, RAFT_NOTFOUND, errmsg);
    return MUNIT_OK;
}

/* If the process can't access the directory, an error is returned. */
TEST(UvFsCheckDir, permissionDenied, NULL, NULL, 0, NULL)
{
    bool has_access = test_dir_has_file("/proc/1", "root");
    /* Skip the test is the process actually has access to /proc/1/root. */
    if (has_access) {
        return MUNIT_SKIP;
    }
    CHECK_DIR_ERROR("/proc/1/root", RAFT_UNAUTHORIZED,
                    "can't access directory '/proc/1/root'");
    return MUNIT_OK;
}

/* If the given path contains a non-directory prefix, an error is returned. */
TEST(UvFsCheckDir, notDirPrefix, NULL, NULL, 0, NULL)
{
    CHECK_DIR_ERROR("/dev/null/foo", RAFT_INVALID,
                    "path '/dev/null/foo' is not a directory");
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST(UvFsCheckDir, notDir, NULL, NULL, 0, NULL)
{
    CHECK_DIR_ERROR("/dev/null", RAFT_INVALID,
                    "path '/dev/null' is not a directory");
    return MUNIT_OK;
}

/* If the given directory is not writable, an error is returned. */
TEST(UvFsCheckDir, notWritable, setUpDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    sprintf(errmsg, "directory '%s' is not writable", dir);
    test_dir_unwritable(dir);
    CHECK_DIR_ERROR(dir, RAFT_INVALID, errmsg);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvFsSyncDir
 *
 *****************************************************************************/

/* Invoke UvFsSyncDir passing it the given dir. */
#define SYNC_DIR_ERROR(DIR, RV, ERRMSG)                      \
    {                                                        \
        char _errmsg[RAFT_ERRMSG_BUF_SIZE];                  \
        munit_assert_int(UvFsSyncDir(DIR, _errmsg), ==, RV); \
        munit_assert_string_equal(_errmsg, ERRMSG);          \
    }

SUITE(UvFsSyncDir)

/* If the directory doesn't exist, an error is returned. */
TEST(UvFsSyncDir, noExists, NULL, NULL, 0, NULL)
{
    SYNC_DIR_ERROR("/abcdef", RAFT_IOERR,
                   "open directory: no such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * UvFsOpenFileForReading
 *
 *****************************************************************************/

/* Open a file in the given dir. */
#define OPEN_FILE_FOR_READING_ERROR(DIR, FILENAME, RV, ERRMSG)          \
    {                                                                   \
        uv_file fd_;                                                    \
        char errmsg_[RAFT_ERRMSG_BUF_SIZE];                             \
        int rv_ = UvFsOpenFileForReading(DIR, FILENAME, &fd_, errmsg_); \
        munit_assert_int(rv_, ==, RV);                                  \
        munit_assert_string_equal(errmsg_, ERRMSG);                     \
    }

SUITE(UvFsOpenFileForReading)

/* If the directory doesn't exist, an error is returned. */
TEST(UvFsOpenFileForReading, noExists, setUpDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    OPEN_FILE_FOR_READING_ERROR(dir, "foo", RAFT_IOERR,
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
        char errmsg_;                                                \
        int rv_;                                                     \
        rv_ = UvFsAllocateFile(DIR, FILENAME, SIZE, &fd_, &errmsg_); \
        munit_assert_int(rv_, ==, 0);                                \
        munit_assert_int(UvOsClose(fd_), ==, 0);                     \
    }

/* Assert that creating a file with the given parameters fails with the given
 * code and error message. */
#define ALLOCATE_FILE_ERROR(DIR, FILENAME, SIZE, RV, ERRMSG)        \
    {                                                               \
        uv_file fd_;                                                \
        char errmsg_[RAFT_ERRMSG_BUF_SIZE];                         \
        int rv_;                                                    \
        rv_ = UvFsAllocateFile(DIR, FILENAME, SIZE, &fd_, errmsg_); \
        munit_assert_int(rv_, ==, RV);                              \
        munit_assert_string_equal(errmsg_, ERRMSG);                 \
    }

SUITE(UvFsAllocateFile)

/* If the given path is valid, the file gets created. */
TEST(UvFsAllocateFile, success, setUpDir, tearDownDir, 0, NULL)
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
                        RAFT_IOERR,          /* status */
                        "open: no such file or directory");
    return MUNIT_OK;
}

/* If the given path already exists, an error is returned. */
TEST(UvFsAllocateFile, fileAlreadyExists, setUpDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    char buf[8];
    test_dir_write_file(dir, "foo", buf, sizeof buf);
    ALLOCATE_FILE_ERROR(dir,        /* dir */
                        "foo",      /* filename */
                        64,         /* size */
                        RAFT_IOERR, /* status */
                        "open: file already exists");
    return MUNIT_OK;
}

/* The file system has run out of space. */
TEST(UvFsAllocateFile, noSpace, setUpDir, tearDownDir, 0, dir_tmpfs_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    ALLOCATE_FILE_ERROR(dir,          /* dir */
                        "foo",        /* filename */
                        4096 * 32768, /* size */
                        RAFT_NOSPACE, /* status */
                        "not enough space to allocate 134217728 bytes");
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
        char errmsg_;                                                        \
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
#define PROBE_CAPABILITIES_ERROR(DIR, RV, ERRMSG)                           \
    {                                                                       \
        size_t direct_io_;                                                  \
        bool async_io_;                                                     \
        char errmsg_[RAFT_ERRMSG_BUF_SIZE];                                 \
        int rv_;                                                            \
        rv_ = UvFsProbeCapabilities(DIR, &direct_io_, &async_io_, errmsg_); \
        munit_assert_int(rv_, ==, RV);                                      \
        munit_assert_string_equal(errmsg_, ERRMSG);                         \
    }

SUITE(UvFsProbeCapabilities)

TEST(UvFsProbeCapabilities, tmpfs, setUpTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES(dir, 0, false);
    return MUNIT_OK;
}

/* ZFS 0.8 reports that it supports direct I/O, but does not support fully
 * support asynchronous kernel AIO. */
TEST(UvFsProbeCapabilities, zfsDirectIO, setUpZfsDir, tearDownDir, 0, NULL)
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
TEST(UvFsProbeCapabilities, aio, setUpDir, tearDownDir, 0, dir_aio_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    /* FIXME: btrfs doesn't like that we perform a first write to the probe file
     * to detect the direct I/O buffer size. */
    if (strcmp(munit_parameters_get(params, DIR_FS_PARAM), "btrfs") == 0) {
        return MUNIT_SKIP;
    }
    PROBE_CAPABILITIES(dir, 4096, true);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST(UvFsProbeCapabilities, noAccess, setUpDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    test_dir_unexecutable(dir);
    PROBE_CAPABILITIES_ERROR(
        dir, RAFT_IOERR,
        "create I/O capabilities probe file: open: permission denied");
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST(UvFsProbeCapabilities, noSpace, setUpTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    test_dir_fill(dir, 0);
    PROBE_CAPABILITIES_ERROR(dir, RAFT_NOSPACE,
                             "create I/O capabilities probe file: not enough "
                             "space to allocate 4096 bytes");
    return MUNIT_OK;
}

#if defined(RWF_NOWAIT)

/* The uvIoSetup() call fails with EAGAIN. */
TEST(UvFsProbeCapabilities, noResources, setUpBtrfsDir, tearDownDir, 0, NULL)
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
                             "io_setup: resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */
