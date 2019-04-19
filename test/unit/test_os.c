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

/* Invoke @osBlockSize assert that it returns the given code. */
#define ASSERT_BLOCK_SIZE_ERROR(RV)                    \
    {                                     \
        size_t size;                      \
        int rv2;                          \
        rv2 = osBlockSize(f->dir, &size); \
        munit_assert_int(rv2, ==, RV);    \
    }

/******************************************************************************
 *
 * osBlockSize
 *
 *****************************************************************************/

TEST_SUITE(block_size);
TEST_SETUP(block_size, setup);

TEST_TEAR_DOWN(block_size, tear_down);

TEST_GROUP(block_size, error)

/* If the given path is not executable, the block size of the underlying file
 * system can't be determined and an error is returned. */
#if defined(RWF_NOWAIT)
TEST_CASE(block_size, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;

    test_dir_unexecutable(f->dir);

    ASSERT_BLOCK_SIZE_ERROR(EACCES);

    return MUNIT_OK;
}

/* No space is left on the target device. */
TEST_CASE(block_size, error, no_space, dir_fs_btrfs_params)
{
    struct fixture *f = data;
    (void)params;

    test_dir_fill(f->dir, 0);

    ASSERT_BLOCK_SIZE_ERROR(ENOSPC);

    return MUNIT_OK;
}

/* The io_setup() call fails with EAGAIN. */
TEST_CASE(block_size, error, no_resources, dir_fs_btrfs_params)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    test_aio_fill(&ctx, 0);

    ASSERT_BLOCK_SIZE_ERROR(EAGAIN);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}
#endif /* RWF_NOWAIT */
