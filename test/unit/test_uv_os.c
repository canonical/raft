#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

#include "../../src/uv_error.h"
#include "../../src/uv_os.h"
#include "../lib/dir.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * uvJoin
 *
 *****************************************************************************/

SUITE(uvJoin)

/* Join a directory path and a filename into a full path. */
TEST(uvJoin, path, NULL, NULL, 0, NULL)
{
    const uvDir dir = "/foo";
    const uvFilename filename = "bar";
    uvPath path;
    uvJoin(dir, filename, path);
    munit_assert_string_equal(path, "/foo/bar");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvEnsureDir
 *
 *****************************************************************************/

/* Invoke uvEnsureDir passing it the given dir. */
#define ENSURE_DIR(DIR)                                    \
    {                                                      \
        uvErrMsg errmsg;                                   \
        munit_assert_int(uvEnsureDir(DIR, errmsg), ==, 0); \
    }
#define ENSURE_DIR_ERROR(DIR, RV, ERRMSG)                   \
    {                                                       \
        uvErrMsg errmsg;                                    \
        munit_assert_int(uvEnsureDir(DIR, errmsg), ==, RV); \
        munit_assert_string_equal(errmsg, ERRMSG);          \
    }

SUITE(uvEnsureDir)

/* If the directory doesn't exist, it is created. */
TEST(uvEnsureDir, doesNotExist, dirSetup, dirTearDown, 0, NULL)
{
    const char *parent = data;
    uvDir dir;
    sprintf(dir, "%s/sub", parent);
    ENSURE_DIR(dir);
    munit_assert_true(test_dir_exists(dir));
    return MUNIT_OK;
}

/* If the directory exists, nothing is needed. */
TEST(uvEnsureDir, exists, dirSetup, dirTearDown, 0, NULL)
{
    const char *dir = data;
    ENSURE_DIR(dir);
    return MUNIT_OK;
}

/* If the directory can't be created, an error is returned. */
TEST(uvEnsureDir, mkdirError, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/foobarbazegg", UV__ERROR, "mkdir: Permission denied");
    return MUNIT_OK;
}

/* If the directory can't be probed for existence, an error is returned. */
TEST(uvEnsureDir, statError, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/proc/1/root", UV__ERROR, "stat: Permission denied");
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST(uvEnsureDir, notDir, NULL, NULL, 0, NULL)
{
    ENSURE_DIR_ERROR("/dev/null", UV__ERROR, "Not a directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvSyncDir
 *
 *****************************************************************************/

/* Invoke uvSyncDir passing it the given dir. */
#define SYNC_DIR_ERROR(DIR, RV, ERRMSG)                   \
    {                                                     \
        uvErrMsg errmsg;                                  \
        munit_assert_int(uvSyncDir(DIR, errmsg), ==, RV); \
        munit_assert_string_equal(errmsg, ERRMSG);        \
    }

SUITE(uvSyncDir)

/* If the directory doesn't exist, an error is returned. */
TEST(uvSyncDir, noExists, NULL, NULL, 0, NULL)
{
    SYNC_DIR_ERROR("/abcdef", UV__ERROR, "open: No such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvOpenFile
 *
 *****************************************************************************/

/* Open a file the fixture's tmpdir. */
#define OPEN_FILE_ERROR(DIR, FILENAME, FLAGS, RV, ERRMSG)        \
    {                                                            \
        int fd;                                                  \
        uvErrMsg errmsg;                                         \
        int rv_ = uvOpenFile(DIR, FILENAME, FLAGS, &fd, errmsg); \
        munit_assert_int(rv_, ==, RV);                           \
        munit_assert_string_equal(errmsg, ERRMSG);               \
    }

SUITE(uvOpenFile)

/* If the directory doesn't exist, an error is returned. */
TEST(uvOpenFile, noExists, dirSetup, dirTearDown, 0, NULL)
{
    const char *dir = data;
    OPEN_FILE_ERROR(dir, "foo", O_RDONLY, UV__NOENT,
                    "open: No such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvProbeIoCapabilities
 *
 *****************************************************************************/

/* Invoke uvProbeIoCapabilities against the fixture's tmpdir. */
#define PROBE_IO_CAPABILITIES_RV(...) \
    uvProbeIoCapabilities(f->dir, __VA_ARGS__, f->errmsg)
#define PROBE_IO_CAPABILITIES(DIRECT_IO, ASYNC_IO) \
    munit_assert_int(PROBE_IO_CAPABILITIES_RV(DIRECT_IO, ASYNC_IO), ==, 0)
#define PROBE_IO_CAPABILITIES_ERROR(RV)                            \
    {                                                              \
        size_t direct_io;                                          \
        bool async_io;                                             \
        int rv_ = PROBE_IO_CAPABILITIES_RV(&direct_io, &async_io); \
        munit_assert_int(rv_, ==, RV);                             \
    }

struct fixture
{
    FIXTURE_DIR;
    char errmsg[2048];
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_DIR;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_DIR;
    free(f);
}

SUITE(uvProbeIoCapabilities)

TEST(uvProbeIoCapabilities, tmpfs, setup, tear_down, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_false(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#if defined(RAFT_HAVE_ZFS_WITH_DIRECT_IO)

/* ZFS 0.8 reports that it supports direct I/O, but does not support fully
 * asynchronous kernel AIO. */
TEST(uvProbeIoCapabilities, zfsDirectIO, setup, tear_down, 0, dir_zfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_true(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#elif defined(RAFT_HAVE_ZFS)

TEST(uvProbeIoCapabilities, zfs, setup, tear_down, 0, dir_zfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_false(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#endif /* RAFT_HAVE_ZFS_GE_0_8 */

/* Assert that the fixture's errmsg string matches the given value. */
#define ASSERT_ERRMSG(MSG) munit_assert_string_equal(f->errmsg, MSG)

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST(uvProbeIoCapabilities, noAccess, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_unexecutable(f->dir);
    PROBE_IO_CAPABILITIES_ERROR(UV__ERROR);
    ASSERT_ERRMSG("mkstemp: Permission denied");
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST(uvProbeIoCapabilities, noSpace, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_fill(f->dir, 0);
    PROBE_IO_CAPABILITIES_ERROR(UV__ERROR);
    ASSERT_ERRMSG("posix_fallocate: No space left on device");
    return MUNIT_OK;
}

#if defined(RAFT_HAVE_BTRFS) && defined(RWF_NOWAIT)

/* The uvIoSetup() call fails with EAGAIN. */
TEST(uvProbeIoCapabilities, noResources, setup, tear_down, 0, dir_btrfs_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    test_aio_fill(&ctx, 0);
    PROBE_IO_CAPABILITIES_ERROR(UV__ERROR);
    ASSERT_ERRMSG("io_setup: Resource temporarily unavailable");
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#endif /* RAFT_HAVE_BTRFS && RWF_NOWAIT */
