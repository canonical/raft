#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "../../include/raft/fixture.h"

#include "fsm.h"
#include "io.h"
#include "munit.h"

#define FIXTURE_CLUSTER                             \
    struct raft_fsm fsms[RAFT_FIXTURE_MAX_SERVERS]; \
    struct raft_fixture cluster;

#define SETUP_CLUSTER(N)                                            \
    {                                                               \
        unsigned i;                                                 \
        int rc;                                                     \
        for (i = 0; i < N; i++) {                                   \
            test_fsm_setup(NULL, &f->fsms[i]);                      \
        }                                                           \
        rc = raft_fixture_init(&f->cluster, N, f->fsms);            \
        munit_assert_int(rc, ==, 0);                                \
        raft_fixture_set_random(&f->cluster, munit_rand_int_range); \
    }

#define TEAR_DOWN_CLUSTER                    \
    {                                        \
        unsigned n = CLUSTER_N;              \
        unsigned i;                          \
        raft_fixture_close(&f->cluster);     \
        for (i = 0; i < n; i++) {            \
            test_fsm_tear_down(&f->fsms[i]); \
        }                                    \
    }

#define CLUSTER_N raft_fixture_n(&f->cluster)

#define CLUSTER_N_PARAM "cluster-n"
#define CLUSTER_N_PARAM_GET \
    (unsigned)atoi(munit_parameters_get(params, CLUSTER_N_PARAM))

/**
 * Bootstrap all servers in the cluster. All of them will be voting.
 */
#define CLUSTER_BOOTSTRAP                                    \
    {                                                        \
        int rc;                                              \
        rc = raft_fixture_bootstrap(&f->cluster, CLUSTER_N); \
        munit_assert_int(rc, ==, 0);                         \
    }

/**
 * Start all servers in the test cluster.
 */
#define CLUSTER_START                         \
    {                                         \
        int rc;                               \
        rc = raft_fixture_start(&f->cluster); \
        munit_assert_int(rc, ==, 0);          \
    }

/**
 * Index of the current leader, or CLUSTER_N if there's no leader.
 */
#define CLUSTER_LEADER raft_fixture_leader_index(&f->cluster)

#define CLUSTER_GET(I) raft_fixture_get(&f->cluster, I)

/**
 * Step the cluster until a leader is elected or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS) \
    raft_fixture_step_until_has_leader(&f->cluster, MAX_MSECS)

/**
 * Step the cluster until there's no leader or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS) \
    raft_fixture_step_until_has_no_leader(&f->cluster, MAX_MSECS)

/**
 * Step the cluster until all nodes have applied the given index or #MAX_MSECS
 * have elapsed.
 */
#define CLUSTER_STEP_UNTIL_APPLIED(INDEX, MAX_MSECS) \
    raft_fixture_step_until_applied(&f->cluster, INDEX, MAX_MSECS)

/**
 * Request to apply an FSM command to add the given value to x.
 */
#define CLUSTER_APPLY_ADD_X(REQ, VALUE, CB)                   \
    {                                                         \
        struct raft_buffer buf;                               \
        struct raft *raft;                                    \
        int rc;                                               \
        munit_assert_int(CLUSTER_LEADER, !=, CLUSTER_N);      \
        test_fsm_encode_add_x(VALUE, &buf);                   \
        raft = raft_fixture_get(&f->cluster, CLUSTER_LEADER); \
        rc = raft_apply(raft, REQ, &buf, 1, CB);              \
        munit_assert_int(rc, ==, 0);                          \
    }

/**
 * Return the last index applied on server I.
 */
#define CLUSTER_LAST_APPLIED(I) \
    raft_last_applied(raft_fixture_get(&f->cluster, I))

/**
 * Munit parameter defining after how many servers to run. Default is 3.
 */
#define TEST_CLUSTER_SERVERS "cluster-servers"

/**
 * Munit parameter defining after how many of the servers are voting
 * servers. Default is 3.
 */
#define TEST_CLUSTER_VOTING "cluster-voting"

struct test_cluster
{
    struct raft_fixture fixture;
    struct raft_fsm fsms[RAFT_FIXTURE_MAX_SERVERS];
};

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c);
void test_cluster_tear_down(struct test_cluster *c);

/**
 * Run a single cluster loop iteration.
 */
void test_cluster_run_once(struct test_cluster *c);

/**
 * Run until the given stop condition becomes true, or max_msecs have elapsed.
 */
int test_cluster_run_until(struct test_cluster *c,
                           bool (*stop)(struct test_cluster *c),
                           unsigned max_msecs);

/**
 * Return the server ID of the leader, or 0 if there's no leader.
 */
unsigned test_cluster_leader(struct test_cluster *c);

/**
 * Return true if the cluster has a leader.
 */
bool test_cluster_has_leader(struct test_cluster *c);

/**
 * Return true if the cluster has no leader.
 */
bool test_cluster_has_no_leader(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to accept a new entry.
 */
void test_cluster_propose(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to accept a new entry containing a
 * configuration change that adds a new non-voting server.
 */
void test_cluster_add_server(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to promote the last server that was
 * added.
 */
void test_cluster_promote(struct test_cluster *c);

/**
 * Return true if at least two log entries were committed to the log.
 */
bool test_cluster_committed_2(struct test_cluster *c);

/**
 * Return true if at least three log entries were committed to the log.
 */
bool test_cluster_committed_3(struct test_cluster *c);

/**
 * Kill a server.
 */
void test_cluster_kill(struct test_cluster *c, unsigned id);

/**
 * Kill the majority of servers (excluding the current leader).
 */
void test_cluster_kill_majority(struct test_cluster *c);

/**
 * Disconnect a server from another.
 */
void test_cluster_disconnect(struct test_cluster *c,
                             unsigned id1,
                             unsigned id2);

/**
 * Reconnect two servers.
 */
void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2);

#endif /* TEST_CLUSTER_H */
