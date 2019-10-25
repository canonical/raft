#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture holding a pristine raft instance.
 *
 *****************************************************************************/

struct cluster
{
    FIXTURE_CLUSTER;
};

static void *setupCluster(const MunitParameter params[],
                          MUNIT_UNUSED void *user_data)
{
    struct cluster *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(1);
    return f;
}

static void tearDownCluster(void *data)
{
    struct cluster *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * Bootstrap tests.
 *
 *****************************************************************************/

SUITE(raft_bootstrap)

/* Attempting to bootstrap an instance that's already started results in
 * RAFT_CANTBOOTSTRAP. */
TEST(raft_bootstrap, alreadyStarted, setupCluster, tearDownCluster, 0, NULL)
{
    struct cluster *f = data;
    struct raft *raft;
    struct raft_configuration configuration;
    int rv;

    /* Bootstrap and the first server. */
    CLUSTER_BOOTSTRAP_N_VOTING(1);
    CLUSTER_START;

    raft = CLUSTER_RAFT(0);
    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_bootstrap(raft, &configuration);
    munit_assert_int(rv, ==, RAFT_CANTBOOTSTRAP);
    raft_configuration_close(&configuration);

    return MUNIT_OK;
}
