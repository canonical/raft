#include "../../include/raft.h"

#include "../lib/cluster.h"
#include "../lib/heap.h"

/**
 *
 * Helpers
 *
 */

struct fixture
{
    struct raft_heap heap;
    struct test_cluster cluster;
};

static char *servers[] = {"3", "5", "7", NULL};

static MunitParameterEnum params[] = {
    {TEST_CLUSTER_SERVERS, servers},
    {NULL, NULL},
};

/**
 *
 * Setup and tear down
 *
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    test_heap_setup(params, &f->heap);

    test_cluster_setup(params, &f->cluster);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_cluster_tear_down(&f->cluster);

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 *
 * Election tests
 *
 */

/* A leader is eventually elected */
static MunitResult test_leader_elected(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* A new leader is elected if the current one dies. */
static MunitResult test_leader_change(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_kill(&f->cluster, test_cluster_leader(&f->cluster));

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_no_leader, 10000);
    munit_assert_int(rv, ==, 0);

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* If no majority of servers is online, no leader is elected. */
static MunitResult test_no_quorum(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_cluster_kill_majority(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 30000);
    munit_assert_int(rv, !=, 0);

    return MUNIT_OK;
}

static MunitTest election_tests[] = {
    {"/leader-elected", test_leader_elected, setup, tear_down, 0, params},
    {"/leader-change", test_leader_change, setup, tear_down, 0, params},
    {"/no-quorum", test_no_quorum, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * Test suite
 *
 */

MunitSuite raft_election_suites[] = {
    {"", election_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
