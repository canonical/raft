#include "../../include/raft.h"

#include "../lib/cluster.h"
#include "../lib/heap.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct test_cluster cluster;
};

static char *servers[] = {"3", "4", "5", NULL};

static MunitParameterEnum params[] = {
    {TEST_CLUSTER_SERVERS, servers},
    {NULL, NULL},
};

/**
 * Setup and tear down
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
 * Membership tests
 */

static MunitResult test_add_non_voting(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    unsigned leader_id;
    const struct raft_server *server;
    struct raft *raft;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_add_server(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 2000);
    munit_assert_int(rv, ==, 0);

    leader_id = test_cluster_leader(&f->cluster);
    raft = &f->cluster.fixture.servers[leader_id - 1].raft;

    server = &raft->configuration.servers[f->cluster.fixture.n - 1];
    munit_assert_int(server->id, ==, f->cluster.fixture.n);

    return MUNIT_OK;
}

static MunitResult test_add_voting(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned leader_id;
    const struct raft_server *server;
    struct raft *raft;
    int rv;

    (void)params;

    /* First add the new server as non-voting. */
    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_add_server(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 2000);
    munit_assert_int(rv, ==, 0);

    /* Then promote it. */
    test_cluster_promote(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_3, 2000);
    munit_assert_int(rv, ==, 0);

    leader_id = test_cluster_leader(&f->cluster);
    raft = &f->cluster.fixture.servers[leader_id - 1].raft;

    server = &raft->configuration.servers[f->cluster.fixture.n - 1];
    munit_assert_true(server->voting);

    return MUNIT_OK;
}

static MunitResult test_remove(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned leader_id;
    struct raft *raft;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    leader_id = test_cluster_leader(&f->cluster);
    raft = &f->cluster.fixture.servers[leader_id - 1].raft;

    rv = raft_remove_server(raft, leader_id % f->cluster.fixture.n + 1);
    munit_assert_int(rv, ==, 0);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 2000);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(raft->configuration.n, ==, f->cluster.fixture.n - 1);

    return 0;
}

static MunitTest membership_tests[] = {
    {"/add-non-voting", test_add_non_voting, setup, tear_down, 0, params},
    {"/add-voting", test_add_voting, setup, tear_down, 0, params},
    {"/remove", test_remove, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_membership_suites[] = {
    {"", membership_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
