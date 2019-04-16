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
 * Parameters
 *
 *****************************************************************************/

static char *cluster_3[] = {"3", NULL};

static MunitParameterEnum cluster_3_params[] = {
    {"cluster-n", cluster_3},
    {NULL, NULL},
};

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
 * Receive AppendEntries requests
 *
 *****************************************************************************/

TEST_SUITE(receive);

TEST_SETUP(receive, setup);
TEST_TEAR_DOWN(receive, tear_down);

/* Receive the same entry a second time, before the first has been persisted. */
TEST_CASE(receive, twice, NULL)
{
    struct fixture *f = data;
    (void)params;
    BOOTSTRAP_START_AND_ELECT;
    CLUSTER_SET_DISK_LATENCY(1, 350);
    CLUSTER_MAKE_PROGRESS;
    return MUNIT_OK;
}

/* If the term in the request is stale, the server rejects it. */
TEST_CASE(receive, stale_term, cluster_3_params)
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

/* If server's log is shorter than prevLogIndex, the request is rejected . */
TEST_CASE(receive, missing_entries, NULL) {
    struct fixture *f = data;
    struct raft_entry entry;
    (void)params;
    CLUSTER_BOOTSTRAP;

    /* The first server has an entry that the second doesn't have */
    entry.type = RAFT_COMMAND;
    entry.term = 1;
    test_fsm_encode_set_x(1, &entry.buf);
    CLUSTER_ADD_ENTRY(0, &entry);

    /* The first server wins the election since it has a longer log. */
    CLUSTER_START;
    CLUSTER_STEP_UNTIL_HAS_LEADER(3000);
    munit_assert_int(CLUSTER_LEADER, ==, 0);

    /* The first server replicates missing entries to the second. */
    CLUSTER_STEP_UNTIL_APPLIED(1, 2, 3000);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is greater than server's commit commit
 * index (i.e. this is a normal inconsistency), we reject the request. */
TEST_CASE(receive, prev_log_term_mismatch, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    (void)params;
    CLUSTER_BOOTSTRAP;

    /* The servers have an entry with a conflicting term. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    test_fsm_encode_set_x(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    test_fsm_encode_set_x(2, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* The follower eventually replicates the entry */
    CLUSTER_STEP_UNTIL_APPLIED(1, 2, 3000);

    return MUNIT_OK;
}

/* If any of the new entry has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST_CASE(receive, prev_index_conflict, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry1;
    struct raft_entry entry2;
    (void)params;
    CLUSTER_BOOTSTRAP;

    /* The servers have an entry with a conflicting term. */
    entry1.type = RAFT_COMMAND;
    entry1.term = 2;
    test_fsm_encode_set_x(1, &entry1.buf);
    CLUSTER_ADD_ENTRY(0, &entry1);

    entry2.type = RAFT_COMMAND;
    entry2.term = 1;
    test_fsm_encode_set_x(2, &entry2.buf);
    CLUSTER_ADD_ENTRY(1, &entry2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* Artificially bump the commit index on the second server */
    CLUSTER_RAFT(1)->commit_index = 2;
    CLUSTER_STEP;
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST_CASE(receive, skip, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    /* Submit an entry */
    CLUSTER_APPLY_ADD_X(req, 1, NULL);

    /* The leader replicates the entry to the follower however it does not get
     * notified about the result, so it sends the entry again. */
    CLUSTER_STEP;
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_STEP_N(2);

    /* The follower reconnects and receives again the same entry. This time the
     * leader receives the notification. */
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP_N(3);

    munit_assert_int(CLUSTER_LAST_APPLIED(0), ==, 2);

    free(req);

    return MUNIT_OK;
}

/* If the index and term of the last snapshot on the server match prevLogIndex
 * and prevLogTerm the request is accepted. */
TEST_CASE(receive, match_last_snapshot, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_configuration configuration;
    (void)params;
    int rv;

    CLUSTER_CONFIGURATION(&configuration);
    rv = raft_bootstrap(CLUSTER_RAFT(0), &configuration);
    munit_assert_int(rv, ==, 0);
    raft_configuration_close(&configuration);

    /* The first server has entry 2 */
    entry.type = RAFT_COMMAND;
    entry.term = 2;
    test_fsm_encode_set_x(5, &entry.buf);
    CLUSTER_ADD_ENTRY(0, &entry);

    /* The second server has a snapshot up to entry 2 */
    CLUSTER_SET_SNAPSHOT(1 /*                                               */,
                         2 /* last index                                    */,
                         2 /* last term                                     */,
                         1 /* conf index                                    */,
                         5 /* x                                             */,
                         0 /* y                                             */);
    CLUSTER_SET_TERM(1, 2);

    CLUSTER_START;
    CLUSTER_ELECT(0);

    /* Apply an additional entry and check that it gets replicated on the
     * follower. */
    CLUSTER_MAKE_PROGRESS;
    CLUSTER_STEP_UNTIL_APPLIED(1, 3, 3000);

    return MUNIT_OK;
}
TEST_GROUP(receive, candidate);

/* If a candidate server receives a request contaning the same term as its
 * own, it it steps down to follower and accept the request . */
TEST_CASE(receive, candidate, same_term, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_BOOTSTRAP;

    raft_set_election_timeout(CLUSTER_RAFT(1), 20000); /* Will stay follower */

    /* Disconnect the third server from the other two and set a very low
     * election timeout on it, so it will immediately start an election. */
    CLUSTER_DISCONNECT(2, 0);
    CLUSTER_DISCONNECT(2, 1);
    raft_set_election_timeout(CLUSTER_RAFT(2), 150);

    /* The third server becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    raft_set_election_timeout(CLUSTER_RAFT(2), 20000); /* Will stay candidate */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);

    /* Let the election succeed and replicate an entry. */
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_TERM(0), ==, 2);
    munit_assert_int(CLUSTER_TERM(1), ==, 2);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);
    CLUSTER_MAKE_PROGRESS;

    /* Now reconnect the third server, which eventually steps down and
     * replicates the entry. */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    CLUSTER_RECONNECT(2, 0);
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_FOLLOWER, 3000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 3000);

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning an higher term as its
 * own, it it steps down to follower and accept the request . */
TEST_CASE(receive, candidate, higher_term, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;
    CLUSTER_BOOTSTRAP;

    /* Disconnect the third server from the other two and set a very low
     * election timeout on it, so it will immediately start an election. */
    CLUSTER_DISCONNECT(2, 0);
    CLUSTER_DISCONNECT(2, 1);
    raft_set_election_timeout(CLUSTER_RAFT(2), 150);

    /* Also disconnect the first and second servers, so it takes a couple of
     * terms to elect a leader. */
    CLUSTER_DISCONNECT(0, 1);

    /* The third server becomes candidate. */
    CLUSTER_START;
    CLUSTER_STEP;
    raft_set_election_timeout(CLUSTER_RAFT(2), 20000); /* Will stay candidate */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);

    /* Make an election round elapse */
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 4000);
    CLUSTER_ADVANCE(4000);

    munit_assert_int(CLUSTER_TERM(0), ==, 3);
    munit_assert_int(CLUSTER_TERM(2), ==, 2);

    /* Reconnect the first and second server and let the election succeed and
     * replicate an entry. */
    CLUSTER_RECONNECT(0, 1);
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    CLUSTER_MAKE_PROGRESS;

    /* Now reconnect the third server, which eventually steps down and
     * replicates the entry. */
    munit_assert_int(CLUSTER_STATE(2), ==, RAFT_CANDIDATE);
    CLUSTER_RECONNECT(2, 0);
    CLUSTER_RECONNECT(2, 1);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_FOLLOWER, 2000);
    CLUSTER_STEP_UNTIL_APPLIED(2, 2, 2000);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Receive AppendEntries responses
 *
 *****************************************************************************/

TEST_SUITE(result);

TEST_SETUP(result, setup);
TEST_TEAR_DOWN(result, tear_down);


/* If the server handling the response is not the leader, the result
 * is ignored. */
TEST_CASE(result, not_leader, NULL)
{
    struct fixture *f = data;
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high-latency for the second server's outgoing messages, so the
     * first server won't get notified about the results for a while. */
    CLUSTER_SET_LATENCY(1, 400, 500);

    /* Set a low election timeout on the first server so it will step down very
     * soon. */
    raft_set_election_timeout(CLUSTER_RAFT(0), 200);

    /* Eventually leader steps down. */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(1000);

    /* The AppendEntries result eventually gets delivered, but the candidate
     * ignores it. */
    CLUSTER_STEP_N(3);

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
TEST_CASE(result, lower_term, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high-latency for the second server's outgoing messages, so the
     * first server won't get notified about the results for a while. */
    CLUSTER_SET_LATENCY(1, 3000, 3100);
    raft_set_election_timeout(CLUSTER_RAFT(1), 20000); /* Won't timeout */

    /* Depose and re-elect the first server, so its term gets bumped. */
    CLUSTER_DISCONNECT(0, 2);
    raft_set_election_timeout(CLUSTER_RAFT(0), 150);
    raft_set_election_timeout(CLUSTER_RAFT(2), 700);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_CANDIDATE, 2000);
    CLUSTER_STEP_UNTIL_STATE_IS(2, RAFT_CANDIDATE, 2000);
    CLUSTER_RECONNECT(0, 2);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_LEADER, 2000);

    /* Eventually deliver the result message. */
    CLUSTER_STEP_UNTIL_ELAPSED(3500);

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
TEST_CASE(result, higher_term, cluster_3_params)
{
    struct fixture *f = data;
    (void)params;
    BOOTSTRAP_START_AND_ELECT;

    /* Set a very high election timeout for the first server so it won't step
     * down. */
    raft_set_election_timeout(CLUSTER_RAFT(0), 20000);

    /* Disconnect the first server from the rest of the cluster. */
    CLUSTER_DISCONNECT(0, 1);
    CLUSTER_DISCONNECT(0, 2);

    /* Eventually a new leader gets electected */
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(5000);
    CLUSTER_STEP_UNTIL_HAS_LEADER(5000);
    munit_assert_int(CLUSTER_LEADER, !=, 0);

    /* Reconnect the old leader to the current follower, which eventually
     * replies with an AppendEntries result containing an higher term. */
    CLUSTER_RECONNECT(0, CLUSTER_LEADER == 1 ? 2 : 1);
    CLUSTER_STEP_UNTIL_STATE_IS(0, RAFT_FOLLOWER, 5000);

    return MUNIT_OK;
}
