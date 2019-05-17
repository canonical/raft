#include <errno.h>

#include "../lib/fs.h"
#include "../lib/runner.h"

#include "../../src/os.h"

TEST_MODULE(os);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_DIR;
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

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Invoke @osProbeIO assert that it returns the given code. */
#define ASSERT_PROBE_IO(RV)                             \
    {                                                   \
        size_t direct_io;                               \
        bool async_io;                                  \
        int rv_;                                        \
        rv_ = osProbeIO(f->dir, &direct_io, &async_io); \
        munit_assert_int(rv_, ==, RV);                  \
    }

/******************************************************************************
 *
 * osProbeIO
 *
 *****************************************************************************/

TEST_SUITE(probe_io);
TEST_SETUP(probe_io, setup);
TEST_TEAR_DOWN(probe_io, tear_down);

TEST_GROUP(probe_io, error)

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
#if defined(RWF_NOWAIT)
TEST_CASE(probe_io, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_unexecutable(f->dir);
    ASSERT_PROBE_IO(EACCES);
    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST_CASE(probe_io, error, no_space, dir_fs_btrfs_params)
{
    struct fixture *f = data;
    (void)params;
    test_dir_fill(f->dir, 0);
    ASSERT_PROBE_IO(ENOSPC);
    return MUNIT_OK;
}

/* The io_setup() call fails with EAGAIN. */
TEST_CASE(probe_io, error, no_resources, dir_fs_btrfs_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    (void)params;
    test_aio_fill(&ctx, 0);
    ASSERT_PROBE_IO(EAGAIN);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}
#endif /* RWF_NOWAIT */
