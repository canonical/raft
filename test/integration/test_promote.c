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

static void changeCbAssertResult(struct raft_change *req, int status)
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
    struct raft_change _req;                                                  \
    char _address[16];                                                        \
    struct result _result = {0, false};                                       \
    int _rv;                                                                  \
    _req.data = &_result;                                                     \
    sprintf(_address, "%d", ID);                                              \
    _rv =                                                                     \
        raft_add(CLUSTER_RAFT(I), &_req, ID, _address, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

#define ADD(I, ID)                                            \
    do {                                                      \
        ADD_SUBMIT(I, ID);                                    \
        CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 2000); \
    } while (0)

/* Submit a promote request. */
#define PROMOTE_SUBMIT(I, ID, ROLE)                                           \
    struct raft_change _req;                                                  \
    struct result _result = {0, false};                                       \
    int _rv;                                                                  \
    _req.data = &_result;                                                     \
    _rv =                                                                     \
        raft_promote(CLUSTER_RAFT(I), &_req, ID, ROLE, changeCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Expect the promote callback to fire with the given status. */
#define PROMOTE_EXPECT(STATUS) _result.status = STATUS;

/* Wait until a promote request comletes. */
#define PROMOTE_WAIT CLUSTER_STEP_UNTIL(changeCbHasFired, &_result, 10000)

/* Submit a request to promote the I'th server to the given role and wait for
 * the operation to succeed. */
#define PROMOTE(I, ID, ROLE)         \
    do {                             \
        PROMOTE_SUBMIT(I, ID, ROLE); \
        PROMOTE_WAIT;                \
    } while (0)

/* Invoke raft_promote() against the I'th server and assert it the given error
 * code. */
#define PROMOTE_ERROR(I, ID, ROLE, RV, ERRMSG)                        \
    {                                                                 \
        struct raft_change __req;                                     \
        int __rv;                                                     \
        __rv = raft_promote(CLUSTER_RAFT(I), &__req, ID, ROLE, NULL); \
        munit_assert_int(__rv, ==, RV);                               \
        munit_assert_string_equal(ERRMSG, CLUSTER_ERRMSG(I));         \
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
 * Assertions
 *
 *****************************************************************************/

/* Assert the values of the committed and uncommitted configuration indexes on
 * the raft instance with the given index. */
#define ASSERT_CONFIGURATION_INDEXES(I, COMMITTED, UNCOMMITTED)      \
    {                                                                \
        struct raft *raft_ = CLUSTER_RAFT(I);                        \
        munit_assert_int(raft_->configuration_index, ==, COMMITTED); \
        munit_assert_int(raft_->configuration_uncommitted_index, ==, \
                         UNCOMMITTED);                               \
    }

/* Assert that the state of the current catch up round matches the given
 * values. */
#define ASSERT_CATCH_UP_ROUND(I, PROMOTEED_ID, NUMBER, DURATION)              \
    {                                                                         \
        struct raft *raft_ = CLUSTER_RAFT(I);                                 \
        munit_assert_int(raft_->leader_state.promotee_id, ==, PROMOTEED_ID);  \
        munit_assert_int(raft_->leader_state.round_number, ==, NUMBER);       \
        munit_assert_int(                                                     \
            raft_->io->time(raft_->io) - raft_->leader_state.round_start, >=, \
            DURATION);                                                        \
    }

/******************************************************************************
 *
 * raft_promote
 *
 *****************************************************************************/

SUITE(raft_promote)

/* Promoting a server whose log is already up-to-date results in the relevant
 * configuration change to be submitted immediately. */
TEST(raft_promote, upToDate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    const struct raft_server *server;
    GROW;
    ADD(0, 3);
    CLUSTER_STEP_N(3);

    PROMOTE(0, 3, RAFT_VOTER);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    raft = CLUSTER_RAFT(0);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->role, ==, RAFT_VOTER);

    /* The configuration change request eventually succeeds. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 2000);

    return MUNIT_OK;
}

static bool thirdServerHasCaughtUp(struct raft_fixture *f, void *arg)
{
    struct raft *raft = raft_fixture_get(f, 0);
    (void)arg;
    return raft->leader_state.promotee_id == 0;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. When the server has caught up, the configuration change request gets
 * submitted. */
TEST(raft_promote, catchUp, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft;
    const struct raft_server *server;
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3);

    PROMOTE_SUBMIT(0, 3, RAFT_VOTER);

    /* Server 3 is not being considered as voting, since its log is behind. */
    raft = CLUSTER_RAFT(0);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->role, ==, RAFT_IDLE);

    /* Advance the match index of server 3, by acknowledging the AppendEntries
     * request that the leader has sent to it. */
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 2000);

    /* Disconnect the second server, so it doesn't participate in the quorum */
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    /* Eventually the leader notices that the third server has caught. */
    CLUSTER_STEP_UNTIL(thirdServerHasCaughtUp, NULL, 2000);

    /* The leader has submitted a onfiguration change request, but it's
     * uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(0, 3, 4);

    /* The third server notifies that it has appended the new
     * configuration. Since it's considered voting already, it counts for the
     * majority and the entry gets committed. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 4, 2000);

    /* The promotion is completed. */
    ASSERT_CONFIGURATION_INDEXES(0, 4, 0);

    return MUNIT_OK;
}

static bool thirdServerHasCompletedFirstRound(struct raft_fixture *f, void *arg)
{
    struct raft *raft = raft_fixture_get(f, 0);
    (void)arg;
    return raft->leader_state.round_number != 1;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. If new entries are appended after a round is started, a new round is
 * initiated once the former one completes. */
TEST(raft_promote, newRound, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned election_timeout = CLUSTER_RAFT(0)->election_timeout;
    struct raft_apply *req = munit_malloc(sizeof *req);
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3);

    PROMOTE_SUBMIT(0, 3, RAFT_VOTER);
    ASSERT_CATCH_UP_ROUND(0, 3, 1, 0);

    /* Now that the catch-up round started, submit a new entry and set a very
     * high latency on the server being promoted, so it won't deliver
     * AppendEntry results within the round duration. */
    CLUSTER_APPLY_ADD_X(0, req, 1, NULL);
    CLUSTER_STEP_UNTIL_ELAPSED(election_timeout + 100);

    // FIXME: unstable with 0xcf1f25b6
    // ASSERT_CATCH_UP_ROUND(0, 3, 1, election_timeout + 100);

    /* The leader eventually receives the AppendEntries result from the
     * promotee, acknowledging all entries except the last one. The first round
     * has completes and a new one has starts. */
    CLUSTER_STEP_UNTIL(thirdServerHasCompletedFirstRound, NULL, 2000);

    /* Eventually the server is promoted and everyone applies the entry. */
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index, 5000);

    /* The promotion is eventually completed. */
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index + 1, 5000);
    ASSERT_CONFIGURATION_INDEXES(0, 5, 0);

    free(req);

    return MUNIT_SKIP;
}

static bool secondServerHasNewConfiguration(struct raft_fixture *f, void *arg)
{
    struct raft *raft = raft_fixture_get(f, 1);
    (void)arg;
    return raft->configuration.servers[2].role == RAFT_VOTER;
}

/* If a follower receives an AppendEntries RPC containing a RAFT_CHANGE entry
 * which promotes a non-voting server, the configuration change is immediately
 * applied locally, even if the entry is not yet committed. Once the entry is
 * committed, the change becomes permanent.*/
TEST(raft_promote, changeIsImmediate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    CLUSTER_MAKE_PROGRESS;
    ADD(0, 3);
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 2000);

    PROMOTE_SUBMIT(0, 3, RAFT_VOTER);
    CLUSTER_STEP_UNTIL(secondServerHasNewConfiguration, NULL, 3000);
    ASSERT_CONFIGURATION_INDEXES(1, 3, 4);

    PROMOTE_WAIT;

    return MUNIT_OK;
}

/* Promote an idle node to stand-by. */
TEST(raft_promote, standBy, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    ADD(0, 3);
    PROMOTE(0, 3, RAFT_STANDBY);
    return MUNIT_OK;
}

/* Trying to promote a server on a node which is not the leader results in an
 * error. */
TEST(raft_promote, notLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PROMOTE_ERROR(1, 3, RAFT_VOTER, RAFT_NOTLEADER, "server is not the leader");
    return MUNIT_OK;
}

/* Trying to promote a server whose ID is unknown results in an
 * error. */
TEST(raft_promote, unknownId, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PROMOTE_ERROR(0, 3, RAFT_VOTER, RAFT_NOTFOUND, "no server has ID 3");
    return MUNIT_OK;
}

/* Trying to promote a server to the idle role results in an * error. */
TEST(raft_promote, badRole, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PROMOTE_ERROR(0, 3, RAFT_IDLE, RAFT_BADROLE, "server role is not valid");
    return MUNIT_OK;
}

/* Promoting a server which has already the RAFT_VOTER results in an error. */
TEST(raft_promote, alreadyVoter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    PROMOTE_ERROR(0, 1, RAFT_VOTER, RAFT_BADROLE, "server is already voter");
    return MUNIT_OK;
}

/* Trying to promote a server while another server is being promoted results in
 * an error. */
TEST(raft_promote, changeRequestAlreadyInProgress, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    ADD(0, 3);
    PROMOTE_SUBMIT(0, 3, RAFT_VOTER);
    PROMOTE_ERROR(0, 3, RAFT_VOTER, RAFT_CANTCHANGE,
                  "a configuration change is already in progress");
    PROMOTE_WAIT;
    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for promoting
 * the new server is committed, the leader configuration gets rolled back and
 * the server being promoted is not considered any more as voting. */
TEST(raft_promote, leadershipLost, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    /* TODO: fix */
    return MUNIT_SKIP;
    GROW;
    ADD(0, 3);
    CLUSTER_STEP_N(2);

    PROMOTE_SUBMIT(0, 3, RAFT_LEADER);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    ASSERT_CATCH_UP_ROUND(0, 0, 0, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 3);
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_int(server->role, ==, RAFT_VOTER);

    /* Lose leadership. */
    CLUSTER_DEPOSE;

    /* A new leader gets elected */
    CLUSTER_ELECT(1);
    CLUSTER_STEP_N(5);

    /* Server 3 is not being considered voting anymore. */
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_int(server->role, ==, RAFT_STANDBY);

    return MUNIT_OK;
}

/* Trying to promote an unresponsive server eventually fails. */
TEST(raft_promote, unresponsive, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3);

    PROMOTE_SUBMIT(0, 3, RAFT_VOTER);
    CLUSTER_KILL(2);

    PROMOTE_EXPECT(RAFT_NOCONNECTION);
    PROMOTE_WAIT;

    return MUNIT_OK;
}
