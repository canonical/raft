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
 * Add a new pristine server to the cluster, connected to all others. Then
 * submit a request to add it to the configuration as non-voting server.
 */
#define CLUSTER_ADD                                                     \
    {                                                                   \
        int rc;                                                         \
        struct raft *raft;                                              \
        test_fsm_setup(NULL, &f->fsms[CLUSTER_N]);                      \
        rc = raft_fixture_add_server(&f->cluster, &f->fsms[CLUSTER_N]); \
        munit_assert_int(rc, ==, 0);                                    \
        raft = CLUSTER_GET(CLUSTER_N - 1);                              \
        rc = raft_add_server(CLUSTER_GET(CLUSTER_LEADER), raft->id,     \
                             raft->address);                            \
        munit_assert_int(rc, ==, 0);                                    \
    }

#define CLUSTER_PROMOTE                                     \
    {                                                       \
        unsigned id;                                        \
        int rc;                                             \
        id = CLUSTER_N; /* Last server that was added. */   \
        rc = raft_promote(CLUSTER_GET(CLUSTER_LEADER), id); \
        munit_assert_int(rc, ==, 0);                        \
    }

#endif /* TEST_CLUSTER_H */
