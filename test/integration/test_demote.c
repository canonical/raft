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

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
    {                                       \
        int rv__;                           \
        CLUSTER_GROW;                       \
        rv__ = raft_start(CLUSTER_RAFT(2)); \
        munit_assert_int(rv__, ==, 0);      \
    }

static void changeCbAssertResult(struct raft_change_configuration *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static bool changeCbHasFired(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->done;
}

/* Submit an add request. */
#define ADD_SUBMIT(I, ID)                                                     \
    struct raft_change_configuration _req;                                                  \
    char _address[16];                                                        \
    struct result _result = {0, false};                                       \
    int _rv;                                                                  \
    _req.data = &_result;                                                     \
    sprintf(_address, "%d", ID);                                              \
    _rv =                                                                     \
        raft_add(CLUSTER_RAFT(I), &_req, ID, _address, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Wait until an add request comletes. */
#define ADD_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000)

#define ADD(I, ID)         \
    do {                   \
        ADD_SUBMIT(I, ID); \
        ADD_WAIT;          \
    } while (0)

/* Submit a demote request. */
#define DEMOTE_SUBMIT(I, ID, ROLE)                                             \
    struct raft_change_configuration _req;                                                   \
    struct result _result = {0, false};                                        \
    int _rv;                                                                   \
    _req.data = &_result;                                                      \
    _rv = raft_demote(CLUSTER_RAFT(I), &_req, ID, ROLE, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Wait until a demote request comletes. */
#define DEMOTE_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000)

/* Submit a request to demote the I'th server to the given role and wait for
 * the operation to succeed. */
#define DEMOTE(I, ID, ROLE)         \
    do {                            \
        DEMOTE_SUBMIT(I, ID, ROLE); \
        DEMOTE_WAIT;                \
    } while (0)

/* Invoke raft_demote() against the I'th server and assert it the given error
 * code. */
#define DEMOTE_ERROR(I, ID, ROLE, RV, ERRMSG)                        \
    {                                                                \
        struct raft_change_configuration __req;                                    \
        int __rv;                                                    \
        __rv = raft_demote(CLUSTER_RAFT(I), &__req, ID, ROLE, NULL); \
        munit_assert_int(__rv, ==, RV);                              \
        munit_assert_string_equal(ERRMSG, CLUSTER_ERRMSG(I));        \
    }

/******************************************************************************
 *
 * Set Set up a cluster of 2 servers, with the first as leader.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
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
 * raft_demote
 *
 *****************************************************************************/

SUITE(raft_demote)

/* Demote a voter node to stand-by. */
TEST(raft_demote, standBy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DEMOTE(0, 2, RAFT_STANDBY);
    return MUNIT_OK;
}

/* Demote the leader to stand-by. */
TEST(raft_demote, leader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DEMOTE_SUBMIT(0, 1, RAFT_STANDBY);
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(1000); /* Current leader steps down */
    CLUSTER_STEP_UNTIL_HAS_LEADER(3000);    /* The other node elects itself */
    munit_assert_int(CLUSTER_LEADER, ==, 1);
    DEMOTE_WAIT;
    return MUNIT_OK;
}

/* Trying to demote a server to the voter role results in an * error. */
TEST(raft_demote, badRole, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DEMOTE_ERROR(0, 3, RAFT_VOTER, RAFT_BADROLE, "server role is not valid");
    return MUNIT_OK;
}

/* Trying to demote a server while another server is being added results in
 * an error. */
TEST(raft_demote, changeRequestAlreadyInProgress, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    ADD_SUBMIT(0, 3);
    DEMOTE_ERROR(0, 2, RAFT_IDLE, RAFT_CANTCHANGE,
                 "a configuration change is already in progress");
    ADD_WAIT;
    return MUNIT_OK;
}

/* Trying to promote a server whose ID is unknown results in an
 * error. */
TEST(raft_demote, unknownId, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DEMOTE_ERROR(0, 3, RAFT_IDLE, RAFT_NOTFOUND, "no server has ID 3");
    return MUNIT_OK;
}

/* Demoting a server which has already the RAFT_IDLE role results in an error. */
TEST(raft_demote, alreadyVoter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    DEMOTE(0, 2, RAFT_IDLE);
    DEMOTE_ERROR(0, 2, RAFT_IDLE, RAFT_BADROLE, "server is already idle");
    return MUNIT_OK;
}

