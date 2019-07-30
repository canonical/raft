#include "../lib/dir.h"
#include "../lib/runner.h"

#include "../../src/os.h"

TEST_MODULE(fs);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

static void *setup(const MunitParameter params[], void *user_data)
{
    (void)user_data;
    return test_dir_setup(params);
}

static void tear_down(void *data)
{
    test_dir_tear_down(data);
}

/******************************************************************************
 *
 * osEnsureDir
 *
 *****************************************************************************/

TEST_SUITE(ensure_dir);
TEST_SETUP(ensure_dir, setup);
TEST_TEAR_DOWN(ensure_dir, tear_down);

/* If the directory exists, nothing is needed. */
TEST_CASE(ensure_dir, exits, NULL)
{
    osDir dir;
    int rv;
    (void)params;
    strcpy(dir, data);
    rv = osEnsureDir(dir);
    munit_assert_int(rv, ==, 0);
    return MUNIT_OK;
}
