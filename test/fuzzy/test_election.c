#include "../lib/cluster.h"
#include "../lib/runner.h"

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

static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(0);
    CLUSTER_BOOTSTRAP;
    CLUSTER_RANDOMIZE;
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

SUITE(election)

/* A leader is eventually elected */
TEST(election, win, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    return MUNIT_OK;
}

/* A new leader is elected if the current one dies. */
TEST(election, change, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    CLUSTER_KILL_LEADER;
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(10000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(20000);
    return MUNIT_OK;
}

/* If no majority of servers is online, no leader is elected. */
TEST(election, noQuorum, setup, tear_down, 0, _params)
{
    struct fixture *f = data;
    CLUSTER_KILL_MAJORITY;
    CLUSTER_STEP_UNTIL_ELAPSED(30000);
    munit_assert_false(CLUSTER_HAS_LEADER);
    return MUNIT_OK;
}
