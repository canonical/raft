#include "../lib/cluster.h"
#include "../lib/runner.h"

TEST_MODULE(election);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static char *cluster_n[] = {"3", "5", "7", NULL};

static MunitParameterEnum _params[] = {
    {CLUSTER_N_PARAM, cluster_n},
    {NULL, NULL},
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(CLUSTER_N_PARAM_GET);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * Tests
 *
 *****************************************************************************/

TEST_SUITE(run);
TEST_SETUP(run, setup);
TEST_TEAR_DOWN(run, tear_down);

/* A leader is eventually elected */
TEST_CASE(run, win, _params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    return MUNIT_OK;
}

/* A new leader is elected if the current one dies. */
TEST_CASE(run, change, _params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    CLUSTER_KILL_LEADER;
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    return MUNIT_OK;
}

/* If no majority of servers is online, no leader is elected. */
TEST_CASE(run, no_quorum, _params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_KILL_MAJORITY;
    CLUSTER_STEP_UNTIL_ELAPSED(30000);
    munit_assert_false(CLUSTER_HAS_LEADER);
    return MUNIT_OK;
}
