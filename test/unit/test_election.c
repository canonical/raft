#include "../lib/cluster.h"
#include "../lib/runner.h"

#include "../../src/configuration.h"

TEST_MODULE(election);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    unsigned i;
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;

    /* Assign a constant latency to all network messages and set a very low
     * election timeout the first server and a very high one on the others, so
     * the former will expire before. */
    for (i = 0; i < CLUSTER_N; i++) {
        unsigned timeout = 2000;
        CLUSTER_SET_LATENCY(i, 25, 26);
        if (i == 0) {
            timeout = 500;
        }
        raft_set_election_timeout(CLUSTER_RAFT(i), timeout);
    }

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

/******************************************************************************
 *
 * Successful election round
 *
 *****************************************************************************/

TEST_SUITE(win);

TEST_SETUP(win, setup);
TEST_TEAR_DOWN(win, tear_down);

/* Test an election round with two voters. */
TEST_CASE(win, two_voters, NULL)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_START;

    /* The first server converts to candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(1);

    /* The second server receives a RequestVote RPC and votes for the first
     * server. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 1);

    /* The first server receives a RequestVote result RPC and converts to
     * leader */
    CLUSTER_STEP;
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
TEST_CASE(win, dupe_vote, NULL)
{
    struct fixture *f = data;
    (void)params;
    raft_set_election_timeout(CLUSTER_RAFT(1), 10000);
    CLUSTER_START;

    /* The first server converts to candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(1);

    /* The second server receives a RequestVote RPC and votes for the first
     * server. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 1);

    /* Disconnect the second server, so the first server does not receive the
     * result and starts a new election round. */
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    CLUSTER_STEP;
    ASSERT_TERM(0, 3);

    /* Reconnecting the two servers eventually makes the first server win the
     * election. */
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same, the vote is granted. */
TEST_CASE(win, last_index_is_same, NULL)
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

    /* The first server converts to candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The second server grants its vote. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 1);

    /* The first server receives a RequestVote result RPC and converts to
     * leader */
    CLUSTER_STEP;
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher, the vote is granted. */
TEST_CASE(win, last_index_is_higher, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    (void)params;

    entry.type = RAFT_COMMAND;
    entry.term = 1;
    test_fsm_encode_set_x(1, &entry.buf);

    CLUSTER_ADD_ENTRY(0, &entry);
    CLUSTER_SET_TERM(1, 2);

    /* The first server converts to candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The second server grants its vote. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 1);

    /* The first server receives a RequestVote result RPC and converts to
     * leader */
    CLUSTER_STEP;
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

static char *win_wait_quorum_n[] = {"5", NULL};

static MunitParameterEnum win_wait_quorum_params[] = {
    {"cluster-n", win_wait_quorum_n},
    {NULL, NULL},
};

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it stays candidate. */
TEST_CASE(win, wait_quorum, win_wait_quorum_params)
{
    struct fixture *f = data;
    (void)params;

    /* Set a higher latency for the last 3 servers, so they won't reply
     * immediately to the RequestVote RPC */
    CLUSTER_SET_LATENCY(2, 100, 101);
    CLUSTER_SET_LATENCY(3, 100, 101);
    CLUSTER_SET_LATENCY(4, 100, 101);

    /* The first server converts to candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* Another server grants its vote. */
    CLUSTER_STEP;

    /* The first server receives a RequestVote result RPC but stays
     * candidate since it has only 2 votes, and 3 are required. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* Eventually we are elected */
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_LEADER(0);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Scenarios where vote is not granted
 *
 *****************************************************************************/

TEST_SUITE(reject);

TEST_SETUP(reject, setup);
TEST_TEAR_DOWN(reject, tear_down);

TEST_CASE(reject, higher_term, NULL)
{
    struct fixture *f = data;
    (void)params;

    CLUSTER_SET_TERM(1, 3);
    CLUSTER_START;

    /* The first server converts to candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(1);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 0);

    /* The first server receives the RequestVote result RPC and converts to
     * follower because it discovers the newer term. */
    CLUSTER_STEP;
    ASSERT_FOLLOWER(0);

    return 0;
}

static char *reject_has_leader_n[] = {"3", NULL};

static MunitParameterEnum reject_has_leader_params[] = {
    {"cluster-n", reject_has_leader_n},
    {NULL, NULL},
};

/* If the server already has a leader, the vote is not granted (even if the
   request has a higher term). */
TEST_CASE(reject, has_leader, reject_has_leader_params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_START;

    /* The first server wins the elections. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    ASSERT_LEADER(0);

    /* The third server gets disconnected and becomes candidate. */
    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_STEP_N(200);
    ASSERT_CANDIDATE(2);

    /* The second server stays candidate since its requests get rejected it. */
    CLUSTER_STEP_N(20);
    ASSERT_CANDIDATE(2);

    return MUNIT_OK;
}

static char *reject_already_voted_n[] = {"3", NULL};

static MunitParameterEnum reject_already_voted_params[] = {
    {"cluster-n", reject_already_voted_n},
    {NULL, NULL},
};

/* If a server has already voted, vote is not granted. */
TEST_CASE(reject, already_voted, reject_already_voted_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(1);
    (void)params;

    /* Lower the election timeout of the second server and set an even higher
     * timeout for the third. Then disconnect the second server from the first
     * server. This way the second server will convert to candidate but not
     * receive vote requests, and the third server won't ever convert to
     * candidate. */
    raft_set_election_timeout(raft, 1000);
    raft_set_election_timeout(CLUSTER_RAFT(2), 10000);
    CLUSTER_DISCONNECT(0, 1);

    CLUSTER_START;

    /* The first server becomes candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The third server receives the vote request and grants it. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(2, 1);

    /* Prevent the first server from receiving the vote result. */
    CLUSTER_DISCONNECT(0, 2);

    /* The third server converts to candidate. */
    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);
    ASSERT_CANDIDATE(1);

    /* The third server stays candidate because the second server has already
     * voted. */
    CLUSTER_STEP_N(3);
    ASSERT_CANDIDATE(1);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
TEST_CASE(reject, last_term_is_lower, NULL)
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

    /* The first server becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 0);

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_LEADER(1);

    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 500);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower, the vote is not
 * granted. */
TEST_CASE(reject, last_index_is_lower, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    (void)params;

    entry.type = RAFT_COMMAND;
    entry.term = 2;
    test_fsm_encode_set_x(123, &entry.buf);

    CLUSTER_ADD_ENTRY(1, &entry);

    /* The first server becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The second server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 0);

    /* The first server receives the response and stays candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* Eventually the second server becomes leader because it has a longer
     * log. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_LEADER(1);

    return MUNIT_OK;
}

static char *reject_not_voting_n[] = {"3", NULL};
static char *reject_not_voting_n_voting[] = {"2", NULL};

static MunitParameterEnum reject_not_voting_params[] = {
    {"cluster-n", reject_not_voting_n},
    {"cluster-n-voting", reject_not_voting_n_voting},
    {NULL, NULL},
};

/* If we are not a voting server, the vote is not granted. */
TEST_CASE(reject, non_voting, reject_not_voting_params)
{
    struct fixture *f = data;
    (void)params;

    /* Disconnect the first server from the second, so it can't win the
     * elections (since there are only 2 voting servers). */
    CLUSTER_DISCONNECT(0, 1);

    /* The first server becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    /* The third server receives a RequestVote RPC and rejects the vote for the
     * first server. */
    CLUSTER_STEP;
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(2, 0);

    /* The first server stays candidate. */
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);

    return MUNIT_OK;
}

static char *reject_not_granted_n[] = {"5", NULL};

static MunitParameterEnum reject_not_granted_params[] = {
    {"cluster-n", reject_not_granted_n},
    {NULL, NULL},
};

/* If a candidate server receives a response indicating that the vote was not
 * granted, nothing happens (e.g. the server has already voted for someone
 * else). */
TEST_CASE(reject, not_granted, reject_not_granted_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(4);
    (void)params;

    /* Lower the election timeout of the fifth server, so it becomes candidate
     * after the first server */
    raft_set_election_timeout(raft, 1000);

    /* Disconnect the first server from all others except the 2nd. */
    CLUSTER_DISCONNECT(0, 2);
    CLUSTER_DISCONNECT(0, 3);
    CLUSTER_DISCONNECT(0, 4);

    /* Disconnect the fifth server from all others except the 2nd. */
    CLUSTER_DISCONNECT(4, 0);
    CLUSTER_DISCONNECT(4, 2);
    CLUSTER_DISCONNECT(4, 3);

    /* The first server becomes candidate, the fifth one is still follower. */
    CLUSTER_START;
    CLUSTER_STEP;
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(4);

    /* The second server receives a RequestVote RPC and grants its vote. */
    CLUSTER_STEP;
    ASSERT_VOTED_FOR(1, 1);
    ASSERT_CANDIDATE(0);
    ASSERT_FOLLOWER(4);

    /* Disconnect the second server from the first, so it doesn't receive
     * further RequestVote RPCs. */
    CLUSTER_DISCONNECT(0, 1);

    /* The fifth server eventually becomes candidate */
    CLUSTER_STEP_UNTIL_STATE_IS(4, RAFT_CANDIDATE, 10000);
    ASSERT_CANDIDATE(0);
    ASSERT_CANDIDATE(4);

    /* The second server receives a RequestVote RPC but rejects its vote since
     * it has already voted. */
    CLUSTER_STEP_N(4);
    ASSERT_VOTED_FOR(1, 1);
    ASSERT_CANDIDATE(0);
    ASSERT_CANDIDATE(4);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * I/O errors
 *
 *****************************************************************************/

TEST_SUITE(io_error);

TEST_SETUP(io_error, setup);
TEST_TEAR_DOWN(io_error, tear_down);

static char *io_error_candidate_delay[] = {"0", "1", NULL};
static MunitParameterEnum io_error_candidate_params[] = {
    {"delay", io_error_candidate_delay},
    {NULL, NULL},
};

/* The I/O error occurs when converting to candidate. */
TEST_CASE(io_error, candidate, io_error_candidate_params)
{
    struct fixture *f = data;
    const char *delay = munit_parameters_get(params, "delay");
    CLUSTER_START;

    /* The first server fails to convert to candidate. */
    CLUSTER_IO_FAULT(0, atoi(delay), 1);
    CLUSTER_STEP;
    ASSERT_UNAVAILABLE(0);

    return MUNIT_OK;
}

/* The I/O error occurs when sending a vote request, and gets ignored. */
TEST_CASE(io_error, send_vote_request, NULL)
{
    struct fixture *f = data;
    (void)params;
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
TEST_CASE(io_error, persist_vote, NULL)
{
    struct fixture *f = data;
    (void)params;
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
