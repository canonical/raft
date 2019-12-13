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
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    unsigned i;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    for (i = 0; i < CLUSTER_N; i++) {
        struct raft *raft = CLUSTER_RAFT(i);
        raft->data = f;
    }
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
 * Parameters
 *
 *****************************************************************************/

static char *cluster_5[] = {"5", NULL};

static MunitParameterEnum cluster_5_params[] = {
    {CLUSTER_N_PARAM, cluster_5},
    {NULL, NULL},
};

static char *cluster_3[] = {"3", NULL};

static MunitParameterEnum cluster_3_params[] = {
    {CLUSTER_N_PARAM, cluster_3},
    {NULL, NULL},
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Wait until the I'th server becomes candidate. */
#define STEP_UNTIL_CANDIDATE(I) \
    CLUSTER_STEP_UNTIL_STATE_IS(I, RAFT_CANDIDATE, 2000)

/* Wait until the I'th server becomes leader. */
#define STEP_UNTIL_LEADER(I) CLUSTER_STEP_UNTIL_STATE_IS(I, RAFT_LEADER, 2000)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the I'th server is in follower state. */
#define ASSERT_FOLLOWER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_FOLLOWER)

/* Assert that the I'th server is in candidate state. */
#define ASSERT_CANDIDATE(I) \
    munit_assert_int(CLUSTER_STATE(I), ==, RAFT_CANDIDATE)

/* Assert that the I'th server is in leader state. */
#define ASSERT_LEADER(I) munit_assert_int(CLUSTER_STATE(I), ==, RAFT_LEADER)

/* Assert that the I'th server is unavailable. */
#define ASSERT_UNAVAILABLE(I) \
    munit_assert_int(CLUSTER_STATE(I), ==, RAFT_UNAVAILABLE)

/* Assert that the I'th server has voted for the server with the given ID. */
#define ASSERT_VOTED_FOR(I, ID) munit_assert_int(CLUSTER_VOTED_FOR(I), ==, ID)

/* Assert that the I'th server has the given current term. */
#define ASSERT_TERM(I, TERM)                             \
    {                                                    \
        struct raft *raft_ = CLUSTER_RAFT(I);            \
        munit_assert_int(raft_->current_term, ==, TERM); \
    }

/* Assert that the fixture time matches the given value */
#define ASSERT_TIME(TIME) munit_assert_int(CLUSTER_TIME, ==, TIME)

/******************************************************************************
 *
 * Successful election round
 *
 *****************************************************************************/

SUITE(election)

/* Test an election round with two voters. */
TEST(election, twoVoters, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_START;

    /* The first server eventually times out and converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);

    CLUSTER_STEP; /* Server 1 tick */
    ASSERT_FOLLOWER(1);

    CLUSTER_STEP; /* Server 0 completes sending a RequestVote RPC */
    CLUSTER_STEP; /* Server 1 receives RequestVote RPC */
    ASSERT_VOTED_FOR(1, 1);
    ASSERT_TIME(1015);

    CLUSTER_STEP; /* Server 1 completes sending RequestVote RPC */
    CLUSTER_STEP; /* Server 1 receives RequestVote RPC result */
    ASSERT_LEADER(0);
    ASSERT_TIME(1030);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
TEST(election, grantAgain, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;
    raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 10000);
    raft_set_election_timeout(CLUSTER_RAFT(1), 10000);
    CLUSTER_START;

    /* The first server converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);

    CLUSTER_STEP; /* Server 1 tick */
    ASSERT_FOLLOWER(1);

    /* Disconnect the second server, so the first server does not receive the
     * result and eventually starts a new election round. */
    CLUSTER_SATURATE_BOTHWAYS(0, 1);
    CLUSTER_STEP_UNTIL_TERM_IS(0, 3, 2000);
    ASSERT_CANDIDATE(0);
    ASSERT_TIME(2000);

    /* Reconnecting the two servers eventually makes the first server win the
     * election. */
    CLUSTER_DESATURATE_BOTHWAYS(0, 1);
    STEP_UNTIL_LEADER(0);
    ASSERT_TIME(2030);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same, the vote is granted. */
TEST(election, grantIfLastIndexIsSame, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    (void)params;

    entry1.type = RAFT_COMMAND;
    entry1.term = 1;
    test_fsm_encode_set_x(1, &entry1.buf);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    test_fsm_encode_set_x(1, &entry2.buf);

    CLUSTER_ADD_ENTRY(0, &entry1);
    CLUSTER_ADD_ENTRY(1, &entry2);
    CLUSTER_SET_TERM(1, 2);

    CLUSTER_START;

    /* The first server converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);

    /* The first server eventually receives a RequestVote result RPC and
     * converts to leader */
    STEP_UNTIL_LEADER(0);
    ASSERT_TIME(1030);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher, the vote is granted. */
TEST(election, grantIfLastIndexIsHigher, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    (void)params;

    entry.type = RAFT_COMMAND;
    entry.term = 1;
    test_fsm_encode_set_x(1, &entry.buf);

    CLUSTER_ADD_ENTRY(0, &entry);
    CLUSTER_SET_TERM(1, 2);

    CLUSTER_START;

    /* The first server converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);

    /* The second server grants its vote. */
    CLUSTER_STEP_UNTIL_VOTED_FOR(1, 0, 2000);

    /* The first server receives a RequestVote result RPC and converts to
     * leader */
    CLUSTER_STEP_N(2);
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it stays candidate. */
TEST(election, waitQuorum, setUp, tearDown, 0, cluster_5_params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_START;

    /* The first server converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);

    /* All servers grant their vote. */
    CLUSTER_STEP_UNTIL_VOTED_FOR(1, 0, 2000);
    CLUSTER_STEP_UNTIL_VOTED_FOR(2, 0, 2000);
    CLUSTER_STEP_UNTIL_VOTED_FOR(3, 0, 2000);
    CLUSTER_STEP_UNTIL_VOTED_FOR(4, 0, 2000);
    ASSERT_TIME(1015);

    /* The first server receives the first RequestVote result RPC but stays
     * candidate since it has only 2 votes, and 3 are required. */
    CLUSTER_STEP_N(4); /* Send completes on all other servers */
    CLUSTER_STEP;      /* First message is delivered */
    ASSERT_TIME(1030);
    ASSERT_CANDIDATE(0);

    /* Eventually we are elected */
    CLUSTER_STEP;     /* Second message is delivered */
    ASSERT_LEADER(0); /* Server 0 reaches the quorum */
    ASSERT_TIME(1030);

    return MUNIT_OK;
}

/* The vote request gets rejected if our term is higher. */
TEST(election, rejectIfHigherTerm, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    CLUSTER_SET_TERM(1, 3);
    CLUSTER_START;

    /* The first server converts to candidate. */
    STEP_UNTIL_CANDIDATE(0);

    CLUSTER_STEP_N(3); /* Server 1 tick and RequestVote send/delivery */

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    ASSERT_VOTED_FOR(1, 0);

    CLUSTER_STEP_N(2); /* RequestVote result send/delivery */

    /* The first server receives the RequestVote result RPC and converts to
     * follower because it discovers the newer term. */
    ASSERT_FOLLOWER(0);

    return 0;
}

/* If the server already has a leader, the vote is not granted (even if the
 * request has a higher term). */
TEST(election, rejectIfHasLeader, setUp, tearDown, 0, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_START;

    /* Server 0 wins the elections. */
    STEP_UNTIL_LEADER(0);

    /* Server 2 gets disconnected and becomes candidate. */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    STEP_UNTIL_CANDIDATE(2);

    /* Server 2 stays candidate since its requests get rejected. */
    CLUSTER_STEP_N(20);
    ASSERT_CANDIDATE(2);

    return MUNIT_OK;
}

/* If a server has already voted, vote is not granted. */
TEST(election, rejectIfAlreadyVoted, setUp, tearDown, 0, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;

    /* Disconnect server 1 from server 0 and change its randomized election
     * timeout to match the one of server 0. This way server 1 will convert to
     * candidate but not receive vote requests. */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 1, 1000);
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    CLUSTER_START;

    /* Server 0 and server 1 both become candidates. */
    STEP_UNTIL_CANDIDATE(0);
    STEP_UNTIL_CANDIDATE(1);
    ASSERT_TIME(1000);

    /* Server 2 receives the vote request from server 0 and grants it. */
    CLUSTER_STEP_UNTIL_VOTED_FOR(2, 0, 2000);
    ASSERT_TIME(1015);

    /* Server 0 receives the vote result from server 2 and becomes leader. */
    STEP_UNTIL_LEADER(0);
    ASSERT_TIME(1030);

    /* Server 1 is still candidate because its vote request got rejected. */
    ASSERT_CANDIDATE(1);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
TEST(election, rejectIfLastTermIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    (void)params;

    entry1.type = RAFT_COMMAND;
    entry1.term = 1;
    test_fsm_encode_set_x(123, &entry1.buf);

    entry2.type = RAFT_COMMAND;
    entry2.term = 2;
    test_fsm_encode_set_x(456, &entry2.buf);

    CLUSTER_ADD_ENTRY(0, &entry1);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START;

    /* The first server becomes candidate. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP_UNTIL_DELIVERED(0, 1, 100);
    ASSERT_VOTED_FOR(1, 0);
    ASSERT_TIME(1015);

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP_UNTIL_DELIVERED(1, 0, 100);
    ASSERT_CANDIDATE(0);
    ASSERT_TIME(1030);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    STEP_UNTIL_LEADER(1);
    ASSERT_TIME(1130);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower, the vote is not
 * granted. */
TEST(election, rejectIfLastIndexIsLower, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    (void)params;

    entry.type = RAFT_COMMAND;
    entry.term = 2;
    test_fsm_encode_set_x(123, &entry.buf);

    CLUSTER_ADD_ENTRY(1, &entry);

    CLUSTER_START;

    /* The first server becomes candidate. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP_UNTIL_DELIVERED(0, 1, 100);
    ASSERT_VOTED_FOR(1, 0);
    ASSERT_TIME(1015);

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP_UNTIL_DELIVERED(1, 0, 100);
    ASSERT_CANDIDATE(0);
    ASSERT_TIME(1030);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    STEP_UNTIL_LEADER(1);
    ASSERT_TIME(1130);

    return MUNIT_OK;
}

static char *reject_not_voting_n[] = {"3", NULL};
static char *reject_not_voting_n_voting[] = {"2", NULL};

static MunitParameterEnum reject_not_voting_params[] = {
    {CLUSTER_N_PARAM, reject_not_voting_n},
    {CLUSTER_N_VOTING_PARAM, reject_not_voting_n_voting},
    {NULL, NULL},
};

/* If we are not a voting server, the vote is not granted. */
TEST(election, rejectIfNotVoter, setUp, tearDown, 0, reject_not_voting_params)
{
    struct fixture *f = data;

    /* Disconnect server 0 from server 1, so server 0 can't win the elections
     * (since there are only 2 voting servers). */
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    CLUSTER_START;

    /* Server 0 becomes candidate. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);

    /* Server 0 stays candidate because it can't reach a quorum. */
    CLUSTER_STEP_UNTIL_TERM_IS(0, 3, 2000);
    ASSERT_CANDIDATE(0);
    ASSERT_TIME(2000);

    return MUNIT_OK;
}

/* If a candidate server receives a response indicating that the vote was not
 * granted, nothing happens (e.g. the server has already voted for someone
 * else). */
TEST(election, receiveRejectResult, setUp, tearDown, 0, cluster_5_params)
{
    struct fixture *f = data;
    (void)params;

    /* Lower the randomized election timeout of server 4, so it becomes
     * candidate just after server 0 */
    raft_fixture_set_randomized_election_timeout(&f->cluster, 4, 1020);

    /* Disconnect server 0 from all others except server 1. */
    CLUSTER_SATURATE_BOTHWAYS(0, 2);
    CLUSTER_SATURATE_BOTHWAYS(0, 3);
    CLUSTER_SATURATE_BOTHWAYS(0, 4);

    /* Disconnect server 4 from all others except the server 1. */
    CLUSTER_SATURATE_BOTHWAYS(4, 0);
    CLUSTER_SATURATE_BOTHWAYS(4, 2);
    CLUSTER_SATURATE_BOTHWAYS(4, 3);

    CLUSTER_START;

    /* The server 0 becomes candidate, server 4 one is still follower. */
    STEP_UNTIL_CANDIDATE(0);
    ASSERT_TIME(1000);
    ASSERT_FOLLOWER(4);

    /* Server 1 receives a RequestVote RPC and grants its vote. */
    CLUSTER_STEP_UNTIL_DELIVERED(0, 1, 100);
    ASSERT_TIME(1015);
    ASSERT_VOTED_FOR(1, 1);
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(4);

    /* Disconnect server 0 from server 1, so it doesn't receive further
     * messages. */
    CLUSTER_SATURATE_BOTHWAYS(0, 1);

    /* Server 4 server eventually becomes candidate */
    STEP_UNTIL_CANDIDATE(4);
    ASSERT_TIME(1100);
    ASSERT_CANDIDATE(0);

    /* The second server receives a RequestVote RPC but rejects its vote since
     * it has already voted. */
    CLUSTER_STEP_UNTIL_DELIVERED(4, 0, 100);
    ASSERT_VOTED_FOR(1, 1);
    ASSERT_CANDIDATE(0);
    ASSERT_CANDIDATE(4);

    return MUNIT_OK;
}

static char *ioErrorConvertDelay[] = {"0", "1", NULL};
static MunitParameterEnum ioErrorConvert[] = {
    {"delay", ioErrorConvertDelay},
    {NULL, NULL},
};

/* An I/O error occurs when converting to candidate. */
TEST(election, ioErrorConvert, setUp, tearDown, 0, ioErrorConvert)
{
    struct fixture *f = data;
    const char *delay = munit_parameters_get(params, "delay");
    return MUNIT_SKIP;
    CLUSTER_START;

    /* The first server fails to convert to candidate. */
    CLUSTER_IO_FAULT(0, atoi(delay), 1);
    CLUSTER_STEP;
    ASSERT_UNAVAILABLE(0);

    return MUNIT_OK;
}

/* The I/O error occurs when sending a vote request, and gets ignored. */
TEST(election, ioErrorSendVoteRequest, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    return MUNIT_SKIP;
    CLUSTER_START;

    /* The first server fails to send a RequestVote RPC. */
    CLUSTER_IO_FAULT(0, 2, 1);
    CLUSTER_STEP;

    /* The first server is still candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    return MUNIT_OK;
}

/* The I/O error occurs when the second node tries to persist its vote. */
TEST(election, ioErrorPersistVote, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    return MUNIT_SKIP;
    CLUSTER_START;

    /* The first server becomes candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The second server receives a RequestVote RPC but fails to persist its
     * vote. */
    CLUSTER_IO_FAULT(1, 0, 1);
    CLUSTER_STEP;
    ASSERT_UNAVAILABLE(1);

    return MUNIT_OK;
}
