#include "../lib/cluster.h"
#include "../lib/runner.h"

TEST_MODULE(tick);

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
    const char *n_voting_param = munit_parameters_get(params, "n_voting");
    unsigned n = 3;
    unsigned n_voting = n;
    (void)user_data;
    if (n_voting_param != NULL) {
        n_voting = atoi(n_voting_param);
    }
    SETUP_CLUSTER(n);
    CLUSTER_BOOTSTRAP_N_VOTING(n_voting);
    CLUSTER_START;
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

/* Assert the current value of the timer of the I'th raft instance */
#define ASSERT_ELECTION_TIMER(I, MSECS)                       \
    {                                                         \
        struct raft *raft_ = CLUSTER_RAFT(I);                 \
        munit_assert_int(raft_->election_elapsed, ==, MSECS); \
    }

/* Assert the current state of the I'th raft instance.  */
#define ASSERT_STATE(I, STATE) munit_assert_int(CLUSTER_STATE(I), ==, STATE);

/******************************************************************************
 *
 * Tick callback
 *
 *****************************************************************************/

TEST_SUITE(elapse);
TEST_SETUP(elapse, setup);
TEST_TEAR_DOWN(elapse, tear_down);

/* Internal timers are updated according to the given time delta. */
TEST_CASE(elapse, election_timer, NULL)
{
    struct fixture *f = data;
    (void)params;

    CLUSTER_ADVANCE(100);
    ASSERT_ELECTION_TIMER(0, 100);

    CLUSTER_ADVANCE(100);
    ASSERT_ELECTION_TIMER(0, 200);

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
TEST_CASE(elapse, candidate, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    /* Prevent the timer of the second server from expiring. */
    raft_set_election_timeout(CLUSTER_RAFT(1), 10000);

    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);

    /* The term has been incremeted. */
    munit_assert_int(raft->current_term, ==, 2);

    /* We have voted for ouselves. */
    munit_assert_int(raft->voted_for, ==, 1);

    /* We are candidate */
    ASSERT_STATE(0, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_true(raft->candidate_state.votes[0]);
    munit_assert_false(raft->candidate_state.votes[1]);

    return MUNIT_OK;
}

/* If the electippon timeout has not elapsed, stay follower. */
TEST_CASE(elapse, timer_not_expired, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    /* Prevent the timer of the second server from expiring. */
    raft_set_election_timeout(CLUSTER_RAFT(1), 10000);

    CLUSTER_ADVANCE(raft->randomized_election_timeout - 100);
    ASSERT_STATE(0, RAFT_FOLLOWER);

    return MUNIT_OK;
}

static char *elapse_non_voter_n_voting[] = {"1", NULL};

static MunitParameterEnum elapse_non_voter_params[] = {
    {"n_voting", elapse_non_voter_n_voting},
    {NULL, NULL},
};

/* If the election timeout has elapsed, but we're not voters, stay follower. */
TEST_CASE(elapse, not_voter, elapse_non_voter_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(1);
    (void)params;

    /* Prevent the timer of the first server from expiring. */
    raft_set_election_timeout(CLUSTER_RAFT(0), 10000);

    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);
    ASSERT_STATE(1, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has elapsed, send empty
 * AppendEntries RPCs. */
TEST_CASE(elapse, heartbeat, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_ELECT(0);

    /* Expire the heartbeat timeout */
    CLUSTER_ADVANCE(raft->heartbeat_timeout + 100);

    /* We have sent a heartbeat to our follower */
    //__assert_heartbeat(f, 2, 2, 1, 1);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has not elapsed, do nothing. */
TEST_CASE(elapse, no_heartbeat, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_ELECT(0);

    /* Advance timer but not enough to expire the heartbeat timeout */
    CLUSTER_ADVANCE(raft->heartbeat_timeout - 100);

    /* We have sent no heartbeats */
    // munit_assert_int(raft_io_stub_n_sending(&f->io), ==, 0);

    return MUNIT_OK;
}

static bool first_server_has_stepped_down(struct raft_fixture *cluster,
                                          void *arg)
{
    struct fixture *f = arg;
    (void)cluster;
    return CLUSTER_STATE(0) == RAFT_FOLLOWER;
}

/* If we're leader election timeout elapses without hearing from a majority of
 * the cluster, step down. */
TEST_CASE(elapse, no_contact, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_ELECT(0);
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Wait for the leader to step down. */
    raft_fixture_step_until(&f->cluster, first_server_has_stepped_down, f,
                            raft->election_timeout * 2);
    ASSERT_STATE(0, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
TEST_CASE(elapse, new_election, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    int election_timeout;

    (void)params;

    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Become candidate */
    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);

    election_timeout = raft->randomized_election_timeout;

    /* Expire the election timeout */
    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(raft->current_term, ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(raft->voted_for, ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(raft->randomized_election_timeout, !=, election_timeout);

    /* We are still candidate */
    ASSERT_STATE(0, RAFT_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(raft->candidate_state.votes);
    munit_assert_true(raft->candidate_state.votes[0]);
    munit_assert_false(raft->candidate_state.votes[1]);

    /* We have sent vote requests again */
    //__assert_request_vote(f, 2, 3, 1, 1);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay candidate. */
TEST_CASE(elapse, during_election, NULL)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Become candidate */
    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);

    /* Make some time elapse, but not enough to trigger the timeout */
    CLUSTER_ADVANCE(raft->randomized_election_timeout - 100);

    /* We are still candidate */
    ASSERT_STATE(0, RAFT_CANDIDATE);

    /* No new vote request has been sent */
    // munit_assert_int(raft_io_stub_n_sending(&f->io), ==, 0);

    return MUNIT_OK;
}

static char *elapse_request_vote_only_to_voters_n_voting[] = {"2", NULL};

static MunitParameterEnum elapse_request_vote_only_to_voters_params[] = {
    {"n_voting", elapse_request_vote_only_to_voters_n_voting},
    {NULL, NULL},
};

/* Vote requests are sent only to voting servers. */
TEST_CASE(elapse,
          request_vote_only_to_voters,
          elapse_request_vote_only_to_voters_params)
{
    struct fixture *f = data;
    struct raft *raft = CLUSTER_RAFT(0);
    (void)params;

    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Become candidate */
    CLUSTER_ADVANCE(raft->randomized_election_timeout + 100);

    /* We have sent vote requests only to the voting server */
    //__assert_request_vote(f, 2, 2, 1, 1);

    return MUNIT_OK;
}
