#include "../../src/configuration.h"
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
    struct raft_change req;
};

/* Set up a cluster of 2 servers, with the first as leader. */
static void *setup(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
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

/* Submit a request to assign the given ROLE to the server with the given ID. */
#define ASSIGN(I, ID, ROLE)                                          \
    {                                                                \
        int _rv;                                                     \
        _rv = raft_assign(CLUSTER_RAFT(I), &f->req, ID, ROLE, NULL); \
        munit_assert_int(_rv, ==, 0);                                \
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

/******************************************************************************
 *
 * raft_add
 *
 *****************************************************************************/

SUITE(raft_add)

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
TEST(raft_add, committed, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    const struct raft_server *server;
    ADD(0 /*   I                                                     */,
        3 /*   ID                                                    */, 0);

    /* The new configuration is already effective. */
    munit_assert_int(raft->configuration.n, ==, 3);
    server = &raft->configuration.servers[2];
    munit_assert_int(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_int(server->role, ==, RAFT_SPARE);

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

/* Trying to add a server on a node which is not the leader results in an
 * error. */
TEST(raft_add, notLeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    ADD(1 /*   I                                                     */,
        3 /*   ID                                                    */,
        RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
TEST(raft_add, busy, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
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
TEST(raft_add, duplicateId, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    ADD(0 /*   I                                                     */,
        2 /*   ID                                                    */,
        RAFT_DUPLICATEID);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_remove
 *
 *****************************************************************************/

SUITE(raft_remove)

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
TEST(raft_remove, committed, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    GROW;
    ADD(0, 3, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    ASSIGN(0, 3, RAFT_STANDBY);
    CLUSTER_STEP_UNTIL_APPLIED(2, 1, 2000);
    CLUSTER_STEP_N(2);
    REMOVE(0, 3, 0);
    ASSERT_CONFIGURATION_INDEXES(0, 3, 4);
    CLUSTER_STEP_UNTIL_APPLIED(0, 4, 2000);
    ASSERT_CONFIGURATION_INDEXES(0, 4, 0);
    munit_assert_int(CLUSTER_RAFT(0)->configuration.n, ==, 2);
    return MUNIT_OK;
}

/* A leader gets a request to remove itself. */
TEST(raft_remove, self, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    REMOVE(0, 1, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    /* TODO: the second server does not get notified */
    return MUNIT_SKIP;
    // CLUSTER_STEP_UNTIL_APPLIED(1, 2, 2000);
    return MUNIT_OK;
}

/* Trying to remove a server on a node which is not the leader results in an
 * error. */
TEST(raft_remove, notLeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    REMOVE(1 /*   I                                                     */,
           3 /*   ID                                                    */,
           RAFT_NOTLEADER);
    return MUNIT_OK;
}

/* Trying to remove a server while a configuration change is already in progress
 * results in an error. */
TEST(raft_remove, inProgress, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    ADD(0, 3, 0);
    REMOVE(0, 3, RAFT_CANTCHANGE);
    return MUNIT_OK;
}

/* Trying to remove a server with an unknwon ID results in an error. */
TEST(raft_remove, badId, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    REMOVE(0, 3, RAFT_BADID);
    return MUNIT_OK;
}
