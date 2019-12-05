#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Start with a snapshot present on disk.
 *
 *****************************************************************************/

struct snapshot_fixture
{
    FIXTURE_CLUSTER;
};

SUITE(snapshot)

static void *setup(const MunitParameter params[], void *user_data)
{
    struct snapshot_fixture *f = munit_malloc(sizeof *f);
    struct raft_configuration configuration;
    struct raft *raft;
    int rv;
    (void)user_data;
    SETUP_CLUSTER(2);

    /* Bootstrap the second server. */
    CLUSTER_CONFIGURATION(&configuration);
    raft = CLUSTER_RAFT(1);
    rv = raft_bootstrap(raft, &configuration);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    return f;
}

static void tear_down(void *data)
{
    struct snapshot_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* Only the snapshot is present and no other entries. */
TEST(snapshot, no_entries, setup, tear_down, 0, NULL)
{
    struct snapshot_fixture *f = data;
    (void)params;
    CLUSTER_SET_SNAPSHOT(0 /* server index                                  */,
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
TEST(snapshot, followup_entries, setup, tear_down, 0, NULL)
{
    struct snapshot_fixture *f = data;
    struct raft_entry entries[2];
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
    CLUSTER_ADD_ENTRY(0, &entries[0]);
    CLUSTER_ADD_ENTRY(1, &entries[1]);
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

SUITE(entries)

static void *setUpEntries(const MunitParameter params[], void *user_data)
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

static void tearDownEntries(void *data)
{
    struct entries_fixture *f = data;
    raft_configuration_close(&f->configuration);
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* No entries are present at all */
TEST(entries, empty, setUpEntries, tearDownEntries, 0, NULL)
{
    struct entries_fixture *f = data;
    (void)params;
    CLUSTER_START;
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* Two entries are present. */
TEST(entries, two, setUpEntries, tearDownEntries, 0, NULL)
{
    struct entries_fixture *f = data;
    struct raft_entry entry;
    struct raft_fsm *fsm;
    unsigned i;
    int rv;
    (void)params;

    rv = raft_bootstrap(CLUSTER_RAFT(0), &f->configuration);
    munit_assert_int(rv, ==, 0);

    entry.type = RAFT_COMMAND;
    entry.term = 3;
    test_fsm_encode_set_x(123, &entry.buf);

    CLUSTER_ADD_ENTRY(0, &entry);
    CLUSTER_SET_TERM(0, 3);

    CLUSTER_START;
    CLUSTER_ELECT(0);
    CLUSTER_MAKE_PROGRESS;

    CLUSTER_STEP_UNTIL_APPLIED(CLUSTER_N, 3, 3000);

    for (i = 0; i < CLUSTER_N; i++) {
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

SUITE(single_voting)

static void *setUpSingleVoting(const MunitParameter params[], void *user_data)
{
    struct single_voting_fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(1);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    return f;
}

static void tearDownSingleVoting(void *data)
{
    struct single_voting_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* The server immediately elects itself */
TEST(single_voting,
     self_elect,
     setUpSingleVoting,
     tearDownSingleVoting,
     0,
     NULL)
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

SUITE(single_voting_not_us)

static void *setUpSingleVotingNotUs(const MunitParameter params[],
                                    void *user_data)
{
    struct single_voting_not_us_fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP_N_VOTING(1);
    CLUSTER_START;
    return f;
}

static void tearDownSingleVotingNotUs(void *data)
{
    struct single_voting_not_us_fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/* The server immediately elects itself */
TEST(single_voting_not_us,
     dont_self_elect,
     setUpSingleVotingNotUs,
     tearDownSingleVotingNotUs,
     0,
     NULL)
{
    struct single_voting_not_us_fixture *f = data;
    (void)params;
    munit_assert_int(CLUSTER_STATE(1), ==, RAFT_FOLLOWER);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}
