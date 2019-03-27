#include "../lib/cluster.h"
#include "../lib/runner.h"

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
    (void)user_data;
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    /* Assign a constant latency to all network messages */
    CLUSTER_SET_LATENCY(0, 25, 26);
    CLUSTER_SET_LATENCY(1, 25, 26);
    /* Set a very low election timeout the first server and a very high one on
     * the second, so the former will expire before. */
    raft_set_election_timeout(CLUSTER_RAFT(0), 500);
    raft_set_election_timeout(CLUSTER_RAFT(1), 2000);
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
