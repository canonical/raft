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

static char *servers[] = {"3", "5", "7", NULL};

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
 * Replication tests
 */

/* New entries on the leader are eventually replicated to followers. */
static MunitResult test_append_entries(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_propose(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 2000);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* The cluster remains available even if the current leader dies and a new
 * leader gets elected. */
static MunitResult test_availability(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_propose(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 2000);

    munit_assert_int(rv, ==, 0);

    test_cluster_kill(&f->cluster, test_cluster_leader(&f->cluster));

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_no_leader, 10000);
    munit_assert_int(rv, ==, 0);

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_propose(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_3, 2000);

    return MUNIT_OK;
}

/* If no quorum is available, entries don't get committed. */
static MunitResult test_no_quorum(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    test_cluster_propose(&f->cluster);

    test_cluster_kill_majority(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 10000);
    munit_assert_int(rv, !=, 0);

    return MUNIT_OK;
}

/* If no quorum is available, entries don't get committed. */
static MunitResult test_partitioning(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned leader_id;
    size_t i;
    size_t n;
    int rv;

    (void)params;

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    leader_id = test_cluster_leader(&f->cluster);

    /* Disconnect the leader from a majority of servers */
    n = 0;
    for (i = 0; n < (f->cluster.n / 2) + 1; i++) {
      struct raft *raft = &f->cluster.rafts[i];

      if (raft->id == leader_id) {
	continue;
      }

      test_cluster_disconnect(&f->cluster, leader_id, raft->id);
      n++;
    }

    /* Try to append a new entry using the disconnected leader. */
    test_cluster_propose(&f->cluster);

    /* A new leader gets elected. */
    rv = test_cluster_run_until(&f->cluster, test_cluster_has_no_leader, 10000);
    munit_assert_int(rv, ==, 0);

    /* The entry does not get committed. */
    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 5000);
    munit_assert_int(rv, !=, 0);

    /* Reconnect the old leader */
    for (i = 0; i < f->cluster.n; i++) {
      struct raft *raft = &f->cluster.rafts[i];
      if (raft->id == leader_id) {
	continue;
      }
      test_cluster_reconnect(&f->cluster, leader_id, raft->id);
    }

    /* FIXME: wait a bit more otherwise test_cluster_has_leader would return
     * immediately. */
    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 100);

    rv = test_cluster_run_until(&f->cluster, test_cluster_has_leader, 10000);
    munit_assert_int(rv, ==, 0);

    /* Re-try now to append the entry. */
    test_cluster_propose(&f->cluster);

    rv = test_cluster_run_until(&f->cluster, test_cluster_committed_2, 10000);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest replication_tests[] = {
    {"/append-entries", test_append_entries, setup, tear_down, 0, params},
    {"/availability", test_availability, setup, tear_down, 0, params},
    {"/no-quorum", test_no_quorum, setup, tear_down, 0, params},
    {"/partitioning", test_partitioning, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_replication_suites[] = {
    {"", replication_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
