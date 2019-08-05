#include <errno.h>
#include <stdio.h>

#include "../lib/uv.h"
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
    FIXTURE_TRACER;
    FIXTURE_DIR;
    uvDir tmpdir; /* Path to a temp directory, defaults to f->dir */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_TRACER;
    SETUP_DIR;
    strcpy(f->tmpdir, f->dir);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_DIR;
    TEAR_DOWN_TRACER;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Invoke @uvEnsureDir and assert that it returns the given code. */
#define ENSURE_DIR(RV)                            \
    {                                             \
        int rv_;                                  \
        rv_ = uvEnsureDir(&f->tracer, f->tmpdir); \
        munit_assert_int(rv_, ==, RV);            \
    }

/* Invoke @uvProbeIoCapabilities assert that it returns the given code. */
#define ASSERT_PROBE_IO(RV)                                         \
    {                                                               \
        size_t direct_io;                                           \
        bool async_io;                                              \
        int rv2;                                                    \
        rv2 = uvProbeIoCapabilities(f->dir, &direct_io, &async_io); \
        munit_assert_int(rv2, ==, RV);                              \
    }

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
    ENSURE_DIR(0);
    munit_assert_true(test_dir_exists(f->tmpdir));
    return MUNIT_OK;
}

/* If the directory exists, nothing is needed. */
TEST_CASE(ensure_dir, exists, NULL)
{
    struct fixture *f = data;
    (void)params;
    ENSURE_DIR(0);
    return MUNIT_OK;
}

TEST_GROUP(ensure_dir, error)

/* If the directory can't be created, an error is returned. */
TEST_CASE(ensure_dir, error, cant_create, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/foobarbazegg");
    ENSURE_DIR(EACCES);
    return MUNIT_OK;
}

/* If the directory can't be probed for existence, an error is returned. */
TEST_CASE(ensure_dir, error, cant_probe, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/proc/1/root");
    ENSURE_DIR(EACCES);
    return MUNIT_OK;
}

/* If the given path is not a directory, an error is returned. */
TEST_CASE(ensure_dir, error, not_a_dir, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->tmpdir, "/proc/1/cmdline");
    ENSURE_DIR(ENOTDIR);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * uvProbeIoCapabilities
 *
 *****************************************************************************/

#if defined(RWF_NOWAIT)

TEST_SUITE(probe);
TEST_SETUP(probe, setup);
TEST_TEAR_DOWN(probe, tear_down);

TEST_GROUP(probe, error)

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
TEST_CASE(probe, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_unexecutable(f->dir);
    ASSERT_PROBE_IO(EACCES);
    return MUNIT_OK;
}

#    if defined(RAFT_HAVE_BTRFS)

/* No space is left on the target device. */
TEST_CASE(probe, error, no_space, dir_btrfs_params)
{
    struct fixture *f = data;
    (void)params;
    test_dir_fill(f->dir, 0);
    ASSERT_PROBE_IO(ENOSPC);
    return MUNIT_OK;
}

/* The uvIoSetup() call fails with EAGAIN. */
TEST_CASE(probe, error, no_resources, dir_btrfs_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    (void)params;
    test_aio_fill(&ctx, 0);
    ASSERT_PROBE_IO(EAGAIN);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

#    endif /* RAFT_HAVE_BTRFS */

#endif /* RWF_NOWAIT */
