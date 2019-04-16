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

/* There's a snapshot along with some follow-up entries. */
TEST_CASE(snapshot, followup_entries, NULL)
{
    struct snapshot_fixture *f = data;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    struct raft_fsm *fsm;
    (void)params;

    entries[0].type = RAFT_COMMAND;
    entries[0].term = 2;
    test_fsm_encode_set_x(6, &entries[0].buf);

    entries[1].type = RAFT_COMMAND;
    entries[1].term = 2;
    test_fsm_encode_add_y(2, &entries[1].buf);

    CLUSTER_SET_SNAPSHOT(0 /*                                               */,
                         6 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         7 /* y                                             */);
    CLUSTER_SET_ENTRIES(0, entries, 2);
    CLUSTER_SET_TERM(0, 2);
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;

    fsm = CLUSTER_FSM(0);
    munit_assert_int(test_fsm_get_x(fsm), ==, 7);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Start with entries present on disk.
 *
 *****************************************************************************/

struct entries_fixture
{
    FIXTURE_CLUSTER;
    struct raft_configuration configuration;
};

TEST_SUITE(entries);

TEST_SETUP(entries)
{
    struct entries_fixture *f = munit_malloc(sizeof *f);
    struct raft *raft;
    unsigned i;
    int rc;
    (void)user_data;
    SETUP_CLUSTER(3);

    /* Bootstrap the second and third server. */
    CLUSTER_CONFIGURATION(&f->configuration);
    for (i = 0; i < 2; i++) {
        raft = CLUSTER_RAFT(i + 1);
        rc = raft_bootstrap(raft, &f->configuration);
        munit_assert_int(rc, ==, 0);
    }
    return f;
}

TEST_TEAR_DOWN(entries)
{
    struct entries_fixture *f = data;
    raft_configuration_close(&f->configuration);
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* No entries are present at all */
TEST_CASE(entries, empty, NULL)
{
    struct entries_fixture *f = data;
    (void)params;
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* Two entries are present. */
TEST_CASE(entries, two, NULL)
{
    struct entries_fixture *f = data;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    struct raft_fsm *fsm;
    unsigned i;
    int rv;
    (void)params;

    entries[0].type = RAFT_CONFIGURATION;
    entries[0].term = 1;
    rv = configuration__encode(&f->configuration, &entries[0].buf);
    munit_assert_int(rv, ==, 0);

    entries[1].type = RAFT_COMMAND;
    entries[1].term = 3;
    test_fsm_encode_set_x(123, &entries[1].buf);

    CLUSTER_SET_ENTRIES(0, entries, 2);
    CLUSTER_SET_TERM(0, 3);

    CLUSTER_START;
    CLUSTER_ELECT(0);
    CLUSTER_MAKE_PROGRESS;

    for (i = 0; i < CLUSTER_N; i++) {
        CLUSTER_STEP_UNTIL_APPLIED(i, 3, 5000);
        fsm = CLUSTER_FSM(i);
        munit_assert_int(test_fsm_get_x(fsm), ==, 124);
    }

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Cluster with single voting server.
 *
 *****************************************************************************/

struct single_voting_fixture
{
    FIXTURE_CLUSTER;
};

TEST_SUITE(single_voting);

TEST_SETUP(single_voting)
{
    struct single_voting_fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(1);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    return f;
}

TEST_TEAR_DOWN(single_voting)
{
    struct single_voting_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* The server immediately elects itself */
TEST_CASE(single_voting, self_elect, NULL)
{
    struct single_voting_fixture *f = data;
    (void)params;
    munit_assert_int(CLUSTER_STATE(0), ==, RAFT_LEADER);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Cluster with single voting server that is not us.
 *
 *****************************************************************************/

struct single_voting_not_us_fixture
{
    FIXTURE_CLUSTER;
};

TEST_SUITE(single_voting_not_us);

TEST_SETUP(single_voting_not_us)
{
    struct single_voting_not_us_fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP_N_VOTING(1);
    CLUSTER_START;
    return f;
}

TEST_TEAR_DOWN(single_voting_not_us)
{
    struct single_voting_not_us_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* The server immediately elects itself */
TEST_CASE(single_voting_not_us, dont_self_elect, NULL)
{
    struct single_voting_not_us_fixture *f = data;
    (void)params;
    munit_assert_int(CLUSTER_STATE(1), ==, RAFT_FOLLOWER);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}
