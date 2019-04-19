#include <stdio.h>

#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/configuration.h"

TEST_MODULE(membership);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
    struct raft_change req;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
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
 * Helper macros
 *
 *****************************************************************************/

/* Add a an empty server to the cluster and start it. */
#define GROW                                \
    {                                       \
        int rv__;                           \
        CLUSTER_GROW;                       \
        rv__ = raft_start(CLUSTER_RAFT(2)); \
        munit_assert_int(rv__, ==, 0);      \
    }

/* Invoke raft_add against the I'th node and assert it returns the given
 * value. */
#define ADD(I, ID, RV)                                                \
    {                                                                 \
        int rv_;                                                      \
        char address_[16];                                            \
        sprintf(address_, "%d", ID);                                  \
        rv_ = raft_add(CLUSTER_RAFT(I), &f->req, ID, address_, NULL); \
        munit_assert_int(rv_, ==, RV);                                \
    }

/* Invoke raft_promote against the I'th node and assert it returns the given
 * value. */
#define PROMOTE(I, ID, RV)                                      \
    {                                                           \
        int rv_;                                                \
        rv_ = raft_promote(CLUSTER_RAFT(I), &f->req, ID, NULL); \
        munit_assert_int(rv_, ==, RV);                          \
    }

/* Invoke raft_remove against the I'th node and assert it returns the given
 * value. */
#define REMOVE(I, ID, RV)                                      \
    {                                                          \
        int rv_;                                               \
        rv_ = raft_remove(CLUSTER_RAFT(I), &f->req, ID, NULL); \
        munit_assert_int(rv_, ==, RV);                         \
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
 * raft_add
 *
 *****************************************************************************/

TEST_SUITE(add);
TEST_SETUP(add, setup)
TEST_TEAR_DOWN(add, tear_down)

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
TEST_CASE(add, committed, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    const struct raft_server *server;
    (void)params;
    ADD(0 /*   I                                                     */,
        3 /*   ID                                                    */, 0);

    /* The new configuration is already effective. */
    munit_assert_int(raft->configuration.n, ==, 3);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_false(server->voting);

    /* The new configuration is marked as uncommitted. */
    ASSERT_CONFIGURATION_INDEXES(0, 1, 2);

    /* The next/match indexes now include an entry for the new server. */
    munit_assert_int(raft->leader_state.progress[2].next_index, ==, 3);
    munit_assert_int(raft->leader_state.progress[2].match_index, ==, 0);

    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 0);

    /* The new configuration is marked as committed. */

    return MUNIT_OK;
}

TEST_GROUP(add, error);

/* Trying to add a server on a node which is not the leader results in an
 * error. */
TEST_CASE(add, error, not_leader, NULL)
{
    struct fixture *f = data;
    (void)params;
    ADD(1 /*   I                                                     */,
        3 /*   ID                                                    */,
        RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
TEST_CASE(add, error, busy, NULL)
{
    struct fixture *f = data;
    (void)params;
    ADD(0 /*   I                                                     */,
        3 /*   ID                                                    */, 0);
    ADD(0 /*   I                                                     */,
        4 /*   ID                                                    */,
        RAFT_CANTCHANGE);
    munit_log(MUNIT_LOG_INFO, "done");
    return MUNIT_OK;
}

/* Trying to add a server with an ID which is already in use results in an
 * error. */
TEST_CASE(add, error, dup_id, NULL)
{
    struct fixture *f = data;
    (void)params;
    ADD(0 /*   I                                                     */,
        2 /*   ID                                                    */,
        RAFT_DUPLICATEID);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_promote
 *
 *****************************************************************************/

TEST_SUITE(promote);
TEST_SETUP(promote, setup)
TEST_TEAR_DOWN(promote, tear_down)

/* Promoting a server whose log is already up-to-date results in the relevant
 * configuration change to be submitted immediately. */
TEST_CASE(promote, up_to_date, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    (void)params;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 1, 2000);
    CLUSTER_STEP_N(3);

    PROMOTE(0, 3, 0);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_true(server->voting);

    /* The configuration change request eventually succeeds. */
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);

    return MUNIT_OK;
}

static bool third_server_has_caught_up(struct raft_fixture *f, void *arg)
{
    struct raft *raft = raft_fixture_get(f, 0);
    (void)arg;
    return raft->leader_state.promotee_id == 0;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. When the server has caught up, the configuration change request gets
 * submitted. */
TEST_CASE(promote, catch_up, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    (void)params;
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 2000);

    PROMOTE(0, 3, 0);

    /* Server 3 is not being considered as voting, since its log is behind. */
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_false(server->voting);

    /* Advance the match index of server 3, by acknowledging the AppendEntries
     * request that the leader has sent to it. */
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 2000);

    /* Disconnect the second server, so it doesn't participate in the quorum */
    CLUSTER_DISCONNECT(0, 1);

    /* Eventually the leader notices that the third server has caught. */
    CLUSTER_STEP_UNTIL(third_server_has_caught_up, NULL, 2000);

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

static bool third_server_has_completed_first_round(struct raft_fixture *f,
                                                   void *arg)
{
    struct raft *raft = raft_fixture_get(f, 0);
    (void)arg;
    return raft->leader_state.round_number != 1;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. If new entries are appended after a round is started, a new round is
 * initiated once the former one completes. */
TEST_CASE(promote, new_round, NULL)
{
    struct fixture *f = data;
    unsigned election_timeout = CLUSTER_RAFT(0)->election_timeout;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    CLUSTER_MAKE_PROGRESS;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 2000);

    PROMOTE(0, 3, 0);
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
    CLUSTER_STEP_UNTIL(third_server_has_completed_first_round, NULL, 2000);

    /* Eventually the server is promoted and everyone applies the entry. */
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index, 5000);

    /* The promotion is eventually completed. */
    CLUSTER_STEP_UNTIL_APPLIED(0, req->index + 1, 5000);
    ASSERT_CONFIGURATION_INDEXES(0, 5, 0);

    free(req);

    return MUNIT_SKIP;
}

static bool second_server_has_new_configuration(struct raft_fixture *f,
                                                void *arg)
{
    struct raft *raft = raft_fixture_get(f, 1);
    (void)arg;
    return raft->configuration.servers[2].voting;
}

/* If a follower receives an AppendEntries RPC containing a RAFT_CHANGE entry
 * which promotes a non-voting server, the configuration change is immediately
 * applied locally, even if the entry is not yet committed. Once the entry is
 * committed, the change becomes permanent.*/
TEST_CASE(promote, change_is_immediate, NULL)
{
    struct fixture *f = data;
    (void)params;
    GROW;
    CLUSTER_MAKE_PROGRESS;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 2000);

    PROMOTE(0, 3, 0);
    CLUSTER_STEP_UNTIL(second_server_has_new_configuration, NULL, 3000);
    ASSERT_CONFIGURATION_INDEXES(1, 3, 4);

    return MUNIT_OK;
}

TEST_GROUP(promote, error);

/* Trying to promote a server on a node which is not the leader results in an
 * error. */
TEST_CASE(promote, error, not_leader, NULL)
{
    struct fixture *f = data;
    (void)params;
    PROMOTE(1, 3, RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to promote a server whose ID is unknown results in an
 * error. */
TEST_CASE(promote, error, bad_id, NULL)
{
    struct fixture *f = data;
    (void)params;
    PROMOTE(0, 3, RAFT_BADID);
    return MUNIT_OK;
}

/* Promoting a server which is already a voting results in an error. */
TEST_CASE(promote, error, already_voting, NULL)
{
    struct fixture *f = data;
    (void)params;
    PROMOTE(0, 1, RAFT_ALREADYVOTING);
    return MUNIT_OK;
}

/* Trying to promote a server while another server is being promoted results in
 * an error. */
TEST_CASE(promote, error, in_progress, NULL)
{
    struct fixture *f = data;
    (void)params;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);

    PROMOTE(0, 3, 0);
    PROMOTE(0, 3, RAFT_CANTCHANGE);

    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for promoting
 * the new server is committed, the leader configuration gets rolled back and
 * the server being promoted is not considered any more as voting. */
TEST_CASE(promote, error, leadership_lost, NULL)
{
    struct fixture *f = data;
    const struct raft_server *server;
    (void)params;
    /* TODO: fix */
    return MUNIT_SKIP;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 1, 2000);
    CLUSTER_STEP_N(2);

    PROMOTE(0, 3, 0);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    ASSERT_CATCH_UP_ROUND(0, 0, 0, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 3);
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_true(server->voting);

    /* Lose leadership. */
    CLUSTER_DEPOSE;

    /* A new leader gets elected */
    CLUSTER_ELECT(1);
    CLUSTER_STEP_N(5);

    /* Server 3 is not being considered voting anymore. */
    server = configurationGet(&CLUSTER_RAFT(0)->configuration, 3);
    munit_assert_false(server->voting);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_remove
 *
 *****************************************************************************/

TEST_SUITE(remove);
TEST_SETUP(remove, setup)
TEST_TEAR_DOWN(remove, tear_down)

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
TEST_CASE(remove, committed, NULL)
{
    struct fixture *f = data;
    (void)params;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(2, 1, 2000);
    CLUSTER_STEP_N(2);
    REMOVE(0, 3, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 2, 3);
    CLUSTER_STEP_UNTIL_APPLIED(0, 3, 2000);
    ASSERT_CONFIGURATION_INDEXES(0, 3, 0);
    munit_assert_int(CLUSTER_RAFT(0)->configuration.n, ==, 2);
    return MUNIT_OK;
}

/* A leader gets a request to remove itself. */
TEST_CASE(remove, self, NULL)
{
    struct fixture *f = data;
    (void)params;
    REMOVE(0, 1, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    /* TODO: the second server does not get notified */
    return MUNIT_SKIP;
    // CLUSTER_STEP_UNTIL_APPLIED(1, 2, 2000);
    return MUNIT_OK;
}

TEST_GROUP(remove, error);

/* Trying to remove a server on a node which is not the leader results in an
 * error. */
TEST_CASE(remove, error, not_leader, NULL)
{
    struct fixture *f = data;
    (void)params;
    REMOVE(1 /*   I                                                     */,
           3 /*   ID                                                    */,
           RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to remove a server while a configuration change is already in progress
 * results in an error. */
TEST_CASE(remove, error, in_progress, NULL)
{
    struct fixture *f = data;
    (void)params;
    ADD(0, 3, 0);
    REMOVE(0, 3, RAFT_CANTCHANGE);
    return MUNIT_OK;
}

/* Trying to remove a server with an unknwon ID results in an error. */
TEST_CASE(remove, error, bad_id, NULL)
{
    struct fixture *f = data;
    (void)params;
    REMOVE(0, 3, RAFT_BADID);
    return MUNIT_OK;
}
