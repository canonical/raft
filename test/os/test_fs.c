#include <stdio.h>
#include <errno.h>

#include "../lib/dir.h"
#include "../lib/runner.h"

#include "../../src/os.h"

TEST_MODULE(fs);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_DIR;
    osDir tmpdir; /* Path to a temp directory, defaults to f->dir */
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

/* Invoke @osEnsureDir and assert that it returns the given code. */
#define ENSURE_DIR(RV)                 \
    {                                  \
        int rv_;                       \
        rv_ = osEnsureDir(f->tmpdir);  \
        munit_assert_int(rv_, ==, RV); \
    }

/******************************************************************************
 *
 * osEnsureDir
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
