#include "../lib/cluster.h"
#include "../lib/runner.h"

TEST_MODULE(replication);

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

/* Standard startup sequence, bootstrappin the cluster and electing server 0 */
#define BOOTSTRAP_START_AND_ELECT \
    CLUSTER_BOOTSTRAP;            \
    CLUSTER_START;                \
    CLUSTER_ELECT(0)

/******************************************************************************
 *
 * Send AppendEntries messages
 *
 *****************************************************************************/

TEST_SUITE(send);

TEST_SETUP(send, setup);
TEST_TEAR_DOWN(send, tear_down);

TEST_GROUP(send, error);

static char *send_oom_heap_fault_delay[] = {"5", NULL};
static char *send_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_oom_heap_fault_repeat},
    {NULL, NULL},
};

static void apply_cb(struct raft_apply *req, int status)
{
    (void)status;
    free(req);
}

/* Out of memory failures. */
TEST_CASE(send, error, oom, send_oom_params)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    test_heap_fault_enable(&f->heap);

    CLUSTER_APPLY_ADD_X(req, 1, apply_cb);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST_CASE(send, error, io, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    CLUSTER_IO_FAULT(0, 1, 1);

    CLUSTER_APPLY_ADD_X(req, 1, apply_cb);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Receive AppendEntries messages
 *
 *****************************************************************************/

TEST_SUITE(recv);

TEST_SETUP(recv, setup);
TEST_TEAR_DOWN(recv, tear_down);

TEST_GROUP(recv, reject);

static char *recv_reject_stale_term_n[] = {"3", NULL};

static MunitParameterEnum recv_reject_stale_term_params[] = {
    {"cluster-n", recv_reject_stale_term_n},
    {NULL, NULL},
};

/* If the term in the request is stale, the server rejects it. */
TEST_CASE(recv, reject, stale_term, recv_reject_stale_term_params)
{
    struct fixture *f = data;
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high election timeout and the disconnect the leader so it will
     * keep sending heartbeats. */
    raft_set_election_timeout(CLUSTER_RAFT(0), 50000);
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Eventually a new leader gets elected. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(5000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);

    /* Reconnect the old leader to the current follower. */
    CLUSTER_RECONNECT(0, CLUSTER_LEADER == 1 ? 2 : 1);

    /* Step a few times, so the old leader sends heartbeats to the follower,
     * which rejects them. */
    CLUSTER_STEP_N(10);

    return MUNIT_OK;
}
