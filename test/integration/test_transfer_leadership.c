#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a fake raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void transferLeadershipCb(struct raft *raft)
{
    bool *done = raft->data;
    munit_assert_false(*done);
    *done = true;
}

static bool transferLeadershipCbHasFired(struct raft_fixture *f, void *arg)
{
    bool *done = arg;
    (void)f;
    return *done;
}

/* Submit a transfer leadership request against the I'th server. */
#define TRANSFER_LEADERSHIP_SUBMIT(I, ID)                            \
    struct raft *_raft = CLUSTER_RAFT(I);                            \
    bool _done = false;                                              \
    int _rv;                                                         \
    _raft->data = &_done;                                            \
    _rv = raft_transfer_leadership(_raft, ID, transferLeadershipCb); \
    munit_assert_int(_rv, ==, 0);

/* Wait until the transfer leadership request comletes. */
#define TRANSFER_LEADERSHIP_WAIT \
    CLUSTER_STEP_UNTIL(transferLeadershipCbHasFired, &_done, 2000)

/* Submit a transfer leadership request and wait for it to complete. */
#define TRANSFER_LEADERSHIP(I, ID)         \
    do {                                   \
        TRANSFER_LEADERSHIP_SUBMIT(I, ID); \
        TRANSFER_LEADERSHIP_WAIT;          \
    } while (0)

/* Submit a transfer leadership request against the I'th server and assert that
 * the given error is returned. */
#define TRANSFER_LEADERSHIP_ERROR(I, ID, RV, ERRMSG)                \
    do {                                                            \
        int __rv;                                                   \
        __rv = raft_transfer_leadership(CLUSTER_RAFT(I), ID, NULL); \
        munit_assert_int(__rv, ==, RV);                             \
        munit_assert_string_equal(CLUSTER_ERRMSG(I), ERRMSG);       \
    } while (0)

/******************************************************************************
 *
 * Set up a cluster with a three servers.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(3);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    free(f);
}

/******************************************************************************
 *
 * raft_transfer_leadership
 *
 *****************************************************************************/

SUITE(raft_transfer_leadership)

/* The follower we ask to transfer leadership to is up-to-date. */
TEST(raft_transfer_leadership, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_LEADERSHIP(0, 2);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to needs to catch up. */
TEST(raft_transfer_leadership, catchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req, 1, NULL);
    TRANSFER_LEADERSHIP(0, 2);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    return MUNIT_OK;
}

/* The follower we ask to transfer leadership to is down and the leadership
 * transfer does not succeed. */
TEST(raft_transfer_leadership, expire, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    CLUSTER_APPLY_ADD_X(CLUSTER_LEADER, &req, 1, NULL);
    CLUSTER_KILL(1);
    TRANSFER_LEADERSHIP(0, 2);
    munit_assert_int(CLUSTER_LEADER, ==, 0);
    return MUNIT_OK;
}

/* The given ID doesn't match any server in the current configuration. */
TEST(raft_transfer_leadership, unknownServer, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_LEADERSHIP_ERROR(0, 4, RAFT_BADID, "server ID is not valid");
    return MUNIT_OK;
}

/* Submitting a transfer request twice is an error. */
TEST(raft_transfer_leadership, twice, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_LEADERSHIP_SUBMIT(0, 2);
    TRANSFER_LEADERSHIP_ERROR(0, 3, RAFT_NOTLEADER, "server is not the leader");
    TRANSFER_LEADERSHIP_WAIT;
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. */
TEST(raft_transfer_leadership, autoSelect, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    TRANSFER_LEADERSHIP(0, 0);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);
    return MUNIT_OK;
}

/* If the given ID is zero, the target is selected automatically. Followers that
 * are up-to-date are preferred. */
TEST(raft_transfer_leadership, autoSelectUpToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_KILL(1);
    CLUSTER_MAKE_PROGRESS;
    TRANSFER_LEADERSHIP(0, 0);
    CLUSTER_STEP_UNTIL_HAS_LEADER(1000);
    munit_assert_int(CLUSTER_LEADER, ==, 2);
    return MUNIT_OK;
}
