#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include <stdlib.h>

#include "../../include/raft.h"
#include "../../include/raft/fixture.h"

#include "fsm.h"
#include "heap.h"
#include "io.h"
#include "munit.h"

#define FIXTURE_CLUSTER                             \
    FIXTURE_HEAP;                                   \
    struct raft_fsm fsms[RAFT_FIXTURE_MAX_SERVERS]; \
    struct raft_fixture cluster;

#define SETUP_CLUSTER(N)                                  \
    SETUP_HEAP;                                           \
    {                                                     \
        unsigned i2;                                      \
        int rc2;                                          \
        for (i2 = 0; i2 < N; i2++) {                      \
            test_fsm_setup(NULL, &f->fsms[i2]);           \
        }                                                 \
        rc2 = raft_fixture_init(&f->cluster, N, f->fsms); \
        munit_assert_int(rc2, ==, 0);                     \
        for (i2 = 0; i2 < N; i2++) {                      \
            CLUSTER_SET_RANDOM(i2, munit_rand_int_range); \
        }                                                 \
    }

#define TEAR_DOWN_CLUSTER                    \
    {                                        \
        unsigned n = CLUSTER_N;              \
        unsigned i;                          \
        raft_fixture_close(&f->cluster);     \
        for (i = 0; i < n; i++) {            \
            test_fsm_tear_down(&f->fsms[i]); \
        }                                    \
    }                                        \
    TEAR_DOWN_HEAP;

#define CLUSTER_N_PARAM "cluster-n"
#define CLUSTER_N_PARAM_GET \
    (unsigned)atoi(munit_parameters_get(params, CLUSTER_N_PARAM))

/**
 * Get the number of servers in the cluster.
 */
#define CLUSTER_N raft_fixture_n(&f->cluster)

/**
 * Index of the current leader, or CLUSTER_N if there's no leader.
 */
#define CLUSTER_LEADER raft_fixture_leader_index(&f->cluster)

/**
 * True if the cluster has a leader.
 */
#define CLUSTER_HAS_LEADER CLUSTER_LEADER < CLUSTER_N

/**
 * Get the struct raft object of the I'th server.
 */
#define CLUSTER_RAFT(I) raft_fixture_get(&f->cluster, I)

/**
 * Get the state of the I'th server.
 */
#define CLUSTER_STATE(I) raft_state(raft_fixture_get(&f->cluster, I))

/**
 * Get the struct fsm object of the I'th server.
 */
#define CLUSTER_FSM(I) &f->fsms[I]

/**
 * Return the last applied index on the I'th server.
 */
#define CLUSTER_LAST_APPLIED(I) \
    raft_last_applied(raft_fixture_get(&f->cluster, I))

/**
 * Return the ID of the server the I'th server has voted for.
 */
#define CLUSTER_VOTED_FOR(I) raft_fixture_voted_for(&f->cluster, I)

/**
 * Populate the given configuration with all servers in the fixture. All servers
 * will be voting.
 */
#define CLUSTER_CONFIGURATION(CONF)                                     \
    {                                                                   \
        int rc2;                                                        \
        rc2 = raft_fixture_configuration(&f->cluster, CLUSTER_N, CONF); \
        munit_assert_int(rc2, ==, 0);                                   \
    }

/**
 * Bootstrap all servers in the cluster. All servers will be voting.
 */
#define CLUSTER_BOOTSTRAP                                         \
    {                                                             \
        int rc;                                                   \
        struct raft_configuration configuration;                  \
        CLUSTER_CONFIGURATION(&configuration);                    \
        rc = raft_fixture_bootstrap(&f->cluster, &configuration); \
        munit_assert_int(rc, ==, 0);                              \
        raft_configuration_close(&configuration);                 \
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
 * Step the cluster.
 */
#define CLUSTER_STEP raft_fixture_step(&f->cluster);

/**
 * Step the cluster until a leader is elected or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_ELAPSED(MSECS) \
    raft_fixture_step_until_elapsed(&f->cluster, MSECS)

/**
 * Step the cluster until a leader is elected or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS)                           \
    {                                                                      \
        bool done;                                                         \
        done = raft_fixture_step_until_has_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                           \
        munit_assert_true(CLUSTER_HAS_LEADER);                             \
    }

/**
 * Step the cluster until there's no leader or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS)                           \
    {                                                                         \
        bool done;                                                            \
        done = raft_fixture_step_until_has_no_leader(&f->cluster, MAX_MSECS); \
        munit_assert_true(done);                                              \
        munit_assert_false(CLUSTER_HAS_LEADER);                               \
    }

/**
 * Step the cluster until the given index was applied by the given server (or
 * all if N) or #MAX_MSECS have elapsed.
 */
#define CLUSTER_STEP_UNTIL_APPLIED(I, INDEX, MAX_MSECS)                        \
    {                                                                          \
        bool done;                                                             \
        done =                                                                 \
            raft_fixture_step_until_applied(&f->cluster, I, INDEX, MAX_MSECS); \
        munit_assert_true(done);                                               \
    }

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
 * Kill the I'th server.
 */
#define CLUSTER_KILL(I) raft_fixture_kill(&f->cluster, I);

/**
 * Kill the leader.
 */
#define CLUSTER_KILL_LEADER CLUSTER_KILL(CLUSTER_LEADER)

/**
 * Kill a majority of servers, except the leader (if there is one).
 */
#define CLUSTER_KILL_MAJORITY                                \
    {                                                        \
        size_t i2;                                           \
        size_t n;                                            \
        for (i2 = 0, n = 0; n < (CLUSTER_N / 2) + 1; i2++) { \
            if (i2 == CLUSTER_LEADER) {                      \
                continue;                                    \
            }                                                \
            CLUSTER_KILL(i2)                                 \
            n++;                                             \
        }                                                    \
    }

/**
 * Grow the cluster adding one server.
 */
#define CLUSTER_GROW                                              \
    {                                                             \
        int rv;                                                   \
        rv = raft_fixture_grow(&f->cluster, &f->fsms[CLUSTER_N]); \
        munit_assert_int(rv, ==, 0);                              \
    }

/**
 * Add a new pristine server to the cluster, connected to all others. Then
 * submit a request to add it to the configuration as non-voting server.
 */
#define CLUSTER_ADD                                                      \
    {                                                                    \
        int rc;                                                          \
        struct raft *new_raft;                                           \
        test_fsm_setup(NULL, &f->fsms[CLUSTER_N]);                       \
        CLUSTER_GROW;                                                    \
        rc = raft_start(CLUSTER_RAFT(CLUSTER_N - 1));                    \
        munit_assert_int(rc, ==, 0);                                     \
        raft_fixture_set_random(&f->cluster, CLUSTER_N - 1,              \
                                munit_rand_int_range);                   \
        new_raft = CLUSTER_RAFT(CLUSTER_N - 1);                          \
        rc = raft_add_server(CLUSTER_RAFT(CLUSTER_LEADER), new_raft->id, \
                             new_raft->address);                         \
        munit_assert_int(rc, ==, 0);                                     \
    }

/**
 * Promote the server that was added last.
 */
#define CLUSTER_PROMOTE                                      \
    {                                                        \
        unsigned id;                                         \
        int rc;                                              \
        id = CLUSTER_N; /* Last server that was added. */    \
        rc = raft_promote(CLUSTER_RAFT(CLUSTER_LEADER), id); \
        munit_assert_int(rc, ==, 0);                         \
    }

/**
 * Ensure that the cluster can make progress from the current state.
 *
 * - If no leader is present, wait for one to be elected.
 * - Submit a request to apply a new FSM command and wait for it to complete.
 */
#define CLUSTER_MAKE_PROGRESS                                               \
    {                                                                       \
        struct raft_apply *req = munit_malloc(sizeof *req);                 \
        if (!(CLUSTER_HAS_LEADER)) {                                        \
            CLUSTER_STEP_UNTIL_HAS_LEADER(3000);                            \
        }                                                                   \
        CLUSTER_APPLY_ADD_X(req, 1, NULL);                                  \
        CLUSTER_STEP_UNTIL_APPLIED(                                         \
            CLUSTER_LEADER, CLUSTER_LAST_APPLIED(CLUSTER_LEADER) + 1, 500); \
        free(req);                                                          \
    }

/**
 * Elect the I'th server.
 */
#define CLUSTER_ELECT(I) raft_fixture_elect(&f->cluster, I)

/**
 * Disconnect two servers.
 */
#define CLUSTER_DISCONNECT(I, J) raft_fixture_disconnect(&f->cluster, I, J)

/**
 * Reconnect two servers.
 */
#define CLUSTER_RECONNECT(I, J) raft_fixture_reconnect(&f->cluster, I, J)

/**
 * Set the random function used by the I'th server.
 */
#define CLUSTER_SET_RANDOM(I, RANDOM) \
    raft_fixture_set_random(&f->cluster, I, RANDOM)

/**
 * Set the minimum and maximum network latency of outgoing messages of server I.
 */
#define CLUSTER_SET_LATENCY(I, MIN, MAX) \
    raft_fixture_set_latency(&f->cluster, I, MIN, MAX)

/**
 * Set the term persisted on the I'th server. This must be called before
 * starting the cluster.
 */
#define CLUSTER_SET_TERM(I, TERM) raft_fixture_set_term(&f->cluster, I, TERM)

/**
 * Set the snapshot persisted on the I'th server. This must be called before
 * starting the cluster.
 */
#define CLUSTER_SET_SNAPSHOT(I, LAST_INDEX, LAST_TERM, CONF_INDEX, X, Y) \
    {                                                                    \
        struct raft_configuration configuration;                         \
        struct raft_snapshot *snapshot;                                  \
        CLUSTER_CONFIGURATION(&configuration);                           \
        CREATE_SNAPSHOT(snapshot, LAST_INDEX, LAST_TERM, &configuration, \
                        CONF_INDEX, X, Y);                               \
        raft_configuration_close(&configuration);                        \
        raft_fixture_set_snapshot(&f->cluster, I, snapshot);             \
    }

/**
 * Make an I/O error occur on the I'th server after @DELAY operations.
 */
#define CLUSTER_IO_FAULT(I, DELAY, REPEAT) \
    raft_fixture_io_fault(&f->cluster, I, DELAY, REPEAT)

#endif /* TEST_CLUSTER_H */
