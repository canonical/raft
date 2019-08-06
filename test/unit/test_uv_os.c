#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

#include "../lib/dir.h"
#include "../lib/runner.h"

#include "../../src/uv_os.h"

TEST_MODULE(uv_os);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_DIR;
    uvDir tmpdir; /* Path to a temp directory, defaults to f->dir */
    char errmsg[2048];
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_DIR;
    strcpy(f->tmpdir, f->dir);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Invoke uvEnsureDir passing it the fixture's tmpdir. */
#define ENSURE_DIR_RV uvEnsureDir(f->tmpdir, f->errmsg)
#define ENSURE_DIR munit_assert_int(ENSURE_DIR_RV, ==, 0)
#define ENSURE_DIR_ERROR(RV) munit_assert_int(ENSURE_DIR_RV, ==, RV)

/* Invoke uvSyncDir passing it the fixture's tmpdir. */
#define SYNC_DIR_RV uvSyncDir(f->tmpdir, f->errmsg)
#define SYNC_DIR munit_assert_int(SYNC_DIR_RV, ==, 0)
#define SYNC_DIR_ERROR(RV) munit_assert_int(SYNC_DIR_RV, ==, RV)

/* Open a file the fixture's tmpdir. */
#define OPEN_FILE_RV(...) uvOpenFile(f->tmpdir, __VA_ARGS__, f->errmsg)
#define OPEN_FILE_ERROR(RV, ...) \
    munit_assert_int(OPEN_FILE_RV(__VA_ARGS__), ==, RV)

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

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the fixture's errmsg string matches the given value. */
#define ASSERT_ERRMSG(MSG) munit_assert_string_equal(f->errmsg, MSG)

/******************************************************************************
 *
 * uvJoin
 *
 *****************************************************************************/

/* Join a directory path and a filename into a full path. */
TEST_CASE(join, NULL)
{
    const uvDir dir = "/foo";
    const uvFilename filename = "bar";
    uvPath path;
    (void)data;
    (void)params;
    uvJoin(dir, filename, path);
    munit_assert_string_equal(path, "/foo/bar");
    return MUNIT_OK;
}

/* Extract the directory name from a full path. */
TEST_CASE(dirname, NULL)
{
    const uvPath path = "/foo/bar";
    uvDir dir;
    (void)data;
    (void)params;
    uvDirname(path, dir);
    munit_assert_string_equal(dir, "/foo");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvEnsureDir
 *
 *****************************************************************************/

TEST_SUITE(ensure_dir);
TEST_SETUP(ensure_dir, setup);
TEST_TEAR_DOWN(ensure_dir, tear_down);

/* If the directory doesn't exist, it is created. */
TEST_CASE(ensure_dir, does_not_exists, NULL)
{
    struct fixture *f = data;
    (void)params;
    sprintf(f->tmpdir, "%s/sub", f->dir);
    ENSURE_DIR;
    munit_assert_true(test_dir_exists(f->tmpdir));
    return MUNIT_OK;
}

/* If the directory exists, nothing is needed. */
TEST_CASE(ensure_dir, exists, NULL)
{
    struct fixture *f = data;
    (void)params;
    ENSURE_DIR;
    return MUNIT_OK;
}

TEST_GROUP(ensure_dir, error)

/* If the directory can't be created, an error is returned. */
TEST_CASE(ensure_dir, error, mkdir, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/foobarbazegg");
    ENSURE_DIR_ERROR(RAFT_IOERR);
    ASSERT_ERRMSG("mkdir: Permission denied");
    return MUNIT_OK;
}

/* If the directory can't be probed for existence, an error is returned. */
TEST_CASE(ensure_dir, error, stat, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/proc/1/root");
    ENSURE_DIR_ERROR(RAFT_IOERR);
    ASSERT_ERRMSG("stat: Permission denied");
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST_CASE(ensure_dir, error, not_a_dir, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/proc/1/cmdline");
    ENSURE_DIR_ERROR(RAFT_IOERR);
    ASSERT_ERRMSG("not a directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvSyncDir
 *
 *****************************************************************************/

TEST_SUITE(sync_dir);
TEST_SETUP(sync_dir, setup);
TEST_TEAR_DOWN(sync_dir, tear_down);

TEST_GROUP(sync_dir, error)

/* If the directory doesn't exist, an error is returned. */
TEST_CASE(sync_dir, error, open, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/foobarbazegg");
    SYNC_DIR_ERROR(RAFT_IOERR);
    ASSERT_ERRMSG("open: No such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvOpenFile
 *
 *****************************************************************************/

TEST_SUITE(open_file);
TEST_SETUP(open_file, setup);
TEST_TEAR_DOWN(open_file, tear_down);

TEST_GROUP(open_file, error)

/* If the directory doesn't exist, an error is returned. */
TEST_CASE(open_file, error, open, NULL)
{
    struct fixture *f = data;
    int fd;
    (void)params;
    OPEN_FILE_ERROR(RAFT_IOERR, "foo", O_RDONLY, &fd);
    ASSERT_ERRMSG("open: No such file or directory");
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvProbeIoCapabilities
 *
 *****************************************************************************/

TEST_SUITE(probe);
TEST_SETUP(probe, setup);
TEST_TEAR_DOWN(probe, tear_down);

TEST_CASE(probe, tmpfs, dir_tmpfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    (void)params;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_false(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#if defined(RAFT_HAVE_ZFS_WITH_DIRECT_IO)

/* ZFS 0.8 reports that it supports direct I/O, but does not support fully
 * asynchronous kernel AIO. */
TEST_CASE(probe, zfs_direct_io, dir_zfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    (void)params;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_true(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#else

TEST_CASE(probe, zfs, dir_zfs_params)
{
    struct fixture *f = data;
    size_t direct_io;
    bool async_io;
    (void)params;
    PROBE_IO_CAPABILITIES(&direct_io, &async_io);
    munit_assert_false(direct_io);
    munit_assert_false(async_io);
    return MUNIT_OK;
}

#endif /* RAFT_HAVE_ZFS_GE_0_8 */

TEST_GROUP(probe, error)

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST_CASE(probe, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_unexecutable(f->dir);
    PROBE_IO_CAPABILITIES_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST_CASE(probe, error, no_space, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_fill(f->dir, 0);
    PROBE_IO_CAPABILITIES_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

#if defined(RAFT_HAVE_BTRFS) && defined(RWF_NOWAIT)

/* The uvIoSetup() call fails with EAGAIN. */
TEST_CASE(probe, error, no_resources, dir_btrfs_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    (void)params;
    test_aio_fill(&ctx, 0);
    PROBE_IO_CAPABILITIES_ERROR(RAFT_IOERR);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#endif /* RAFT_HAVE_BTRFS && RWF_NOWAIT */
