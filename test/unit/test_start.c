#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../lib/snapshot.h"

TEST_MODULE(start);

/******************************************************************************
 *
 * Start with a snapshot present on disk.
 *
 *****************************************************************************/

struct snapshot_fixture
{
    FIXTURE_CLUSTER;
};

TEST_SUITE(snapshot);

TEST_SETUP(snapshot)
{
    struct snapshot_fixture *f = munit_malloc(sizeof *f);
    struct raft_configuration configuration;
    struct raft *raft;
    int rc;
    (void)user_data;
    SETUP_CLUSTER(2);

    /* Bootstrap the second server */
    CLUSTER_CONFIGURATION(&configuration);
    raft = CLUSTER_RAFT(1);
    rc = raft_bootstrap(raft, &configuration);
    munit_assert_int(rc, ==, 0);
    raft_configuration_close(&configuration);

    return f;
}

TEST_TEAR_DOWN(snapshot)
{
    struct snapshot_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* Only the snapshot is present and no other entries. */
TEST_CASE(snapshot, no_entries, NULL)
{
    struct snapshot_fixture *f = data;
    (void)params;
    CLUSTER_SET_SNAPSHOT(0 /*                                               */,
                         6 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         7 /* y                                             */);
    CLUSTER_SET_TERM(0, 2);
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}
