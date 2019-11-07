#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

#include "../../src/uv_error.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * uvOpenFile
 *
 *****************************************************************************/

/* Open a file the fixture's tmpdir. */
#define OPEN_FILE_ERROR(DIR, FILENAME, FLAGS, RV, ERRMSG)         \
    {                                                             \
        int fd;                                                   \
        char *errmsg;                                             \
        int rv_ = uvOpenFile(DIR, FILENAME, FLAGS, &fd, &errmsg); \
        munit_assert_int(rv_, ==, RV);                            \
        munit_assert_string_equal(errmsg, ERRMSG);                \
        raft_free(errmsg);                                        \
    }

SUITE(uvOpenFile)

/* If the directory doesn't exist, an error is returned. */
TEST(uvOpenFile, noExists, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    OPEN_FILE_ERROR(dir, "foo", O_RDONLY, UV__NOENT,
                    "open: no such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvProbeIoCapabilities
 *
 *****************************************************************************/

/* Invoke uvProbeIoCapabilities against the given dir and assert that it returns
 * the given values for direct I/O and async I/O. */
#define PROBE_IO_CAPABILITIES(DIR, DIRECT_IO, ASYNC_IO)                      \
    {                                                                        \
        size_t direct_io_;                                                   \
        bool async_io_;                                                      \
        char *errmsg_;                                                       \
        int rv_;                                                             \
        rv_ = uvProbeIoCapabilities(DIR, &direct_io_, &async_io_, &errmsg_); \
        munit_assert_int(rv_, ==, 0);                                        \
        munit_assert_int(direct_io_, ==, DIRECT_IO);                         \
        if (ASYNC_IO) {                                                      \
            munit_assert_true(async_io_);                                    \
        } else {                                                             \
            munit_assert_false(async_io_);                                   \
        }                                                                    \
    }

/* Invoke uvProbeIoCapabilities and check that the given error occurs. */
#define PROBE_IO_CAPABILITIES_ERROR(DIR, RV, ERRMSG)                         \
    {                                                                        \
        size_t direct_io_;                                                   \
        bool async_io_;                                                      \
        char *errmsg_;                                                       \
        int rv_;                                                             \
        rv_ = uvProbeIoCapabilities(DIR, &direct_io_, &async_io_, &errmsg_); \
        munit_assert_int(rv_, ==, RV);                                       \
        munit_assert_string_equal(errmsg_, ERRMSG);                          \
        raft_free(errmsg_);                                                  \
    }

SUITE(uvProbeIoCapabilities)

TEST(uvProbeIoCapabilities, tmpfs, setupTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_IO_CAPABILITIES(dir, 0, false);
    return MUNIT_OK;
}

/* ZFS 0.8 reports that it supports direct I/O, but does not support fully
 * asynchronous kernel AIO. */
TEST(uvProbeIoCapabilities, zfsDirectIO, setupZfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    size_t direct_io = 0;
#if defined(RAFT_HAVE_ZFS_WITH_DIRECT_IO)
    direct_io = 4096;
#endif
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_IO_CAPABILITIES(dir, direct_io, false);
    return MUNIT_OK;
}

#if defined(RWF_NOWAIT)

/* File systems that fully support DIO. */
TEST(uvProbeIoCapabilities, aio, setupDir, tearDownDir, 0, dir_aio_params)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    PROBE_IO_CAPABILITIES(dir, 4096, true);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST(uvProbeIoCapabilities, noAccess, setupDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    test_dir_unexecutable(dir);
    PROBE_IO_CAPABILITIES_ERROR(dir, UV__ERROR, "mkstemp: permission denied");
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST(uvProbeIoCapabilities, noSpace, setupTmpfsDir, tearDownDir, 0, NULL)
{
    const char *dir = data;
    if (dir == NULL) {
        return MUNIT_SKIP;
    }
    test_dir_fill(dir, 0);
    PROBE_IO_CAPABILITIES_ERROR(dir, UV__ERROR,
                                "posix_fallocate: no space left on device");
    return MUNIT_OK;
}

#if defined(RWF_NOWAIT)

/* The uvIoSetup() call fails with EAGAIN. */
TEST(uvProbeIoCapabilities, noResources, setupBtrfsDir, tearDownDir, 0, NULL)
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
    PROBE_IO_CAPABILITIES_ERROR(dir, UV__ERROR,
                                "io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#endif /* RWF_NOWAIT */
