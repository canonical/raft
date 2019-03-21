#include "../lib/cluster.h"
#include "../lib/runner.h"
#include "../lib/snapshot.h"

TEST_MODULE(start);

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Start the given raft instance and check that no error occurs. */
#define START(RAFT)                  \
    {                                \
        int rc;                      \
        rc = raft_start(RAFT);       \
        munit_assert_int(rc, ==, 0); \
    }

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
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_SET_SNAPSHOT(0, 6, 2, 1, 123, 666);
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
    START(CLUSTER_RAFT(0));
    START(CLUSTER_RAFT(1));
    return MUNIT_OK;
}
