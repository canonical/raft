#include "../../include/raft.h"

#include "../lib/cluster.h"
#include "../lib/heap.h"

/**
 * Maximum number of cluster loop iterations each test should perform.
 */
#define MAX_ITERATIONS 50000

/**
 * Maximum number of cluster loop iterations a pair of servers should stay
 * disconnected.
 */
#define MAX_DISCONNECT 150

/**
 * Helpers
 */

struct disconnection
{
    unsigned id1;
    unsigned id2;
    int start;
    int duration;
};

struct fixture
{
    struct raft_heap heap;
    struct test_cluster cluster;
    struct disconnection *disconnections;
};

static char *servers[] = {"3", NULL};

static MunitParameterEnum params[] = {
    {TEST_CLUSTER_SERVERS, servers},
    {NULL, NULL},
};

/**
 * Return the number of distinct server pairs in the cluster.
 */
static int __server_pairs(struct fixture *f)
{
    return f->cluster.n * (f->cluster.n - 1) / 2;
}

/**
 * Update the cluster connectivity for the given iteration.
 */
static void __update_connectivity(struct fixture *f, int i)
{
    int p;
    int pairs = __server_pairs(f);

    for (p = 0; p < pairs; p++) {
        struct disconnection *disconnection = &f->disconnections[p];
        unsigned id1 = disconnection->id1;
        unsigned id2 = disconnection->id2;

        if (disconnection->start == 0) {
            /* Decide whether to disconnect this pair. */
            if (munit_rand_int_range(1, 10) <= 1) {
                disconnection->start = i;
                disconnection->duration =
                    munit_rand_int_range(50, MAX_DISCONNECT);
                test_cluster_disconnect(&f->cluster, id1, id2);
            }
        } else {
            /* Decide whether to reconnect this pair. */
            if (i - disconnection->start > disconnection->duration) {
                test_cluster_reconnect(&f->cluster, id1, id2);
                disconnection->start = 0;
            }
        }
    }
}

/**
 * Setup and tear down
 */
static int cnt = 0;

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int pairs;
    size_t i, j, k;

    (void)user_data;

    cnt++;
    munit_logf(MUNIT_LOG_WARNING, "START TEST %d", cnt);
    test_heap_setup(params, &f->heap);

    test_cluster_setup(params, &f->cluster);

    /* Number of distinct pairs of servers. */
    pairs = __server_pairs(f);

    f->disconnections = munit_malloc(pairs * sizeof *f->disconnections);

    k = 0;
    for (i = 0; i < f->cluster.n; i++) {
        for (j = i + 1; j < f->cluster.n; j++) {
            struct disconnection *disconnection = &f->disconnections[k];
            disconnection->id1 = i + 1;
            disconnection->id2 = j + 1;
            disconnection->start = 0;
            disconnection->duration = 0;
            k++;
        }
    }

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_cluster_tear_down(&f->cluster);

    test_heap_tear_down(&f->heap);

    free(f->disconnections);
    free(f);
}

/**
 * Liveness tests
 */

/* The system makes progress even in case of network disruptions. */
static MunitResult test_network_disconnect(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    int i = 0;

    (void)params;

    for (i = 0; i < MAX_ITERATIONS; i++) {
        __update_connectivity(f, i);
        test_cluster_run_once(&f->cluster);

        if (test_cluster_has_leader(&f->cluster)) {
            test_cluster_accept(&f->cluster);
        }

        if (f->cluster.commit_index >= 2) {
            break;
        }
    }

    munit_assert_int(f->cluster.commit_index, >=, 2);

    return MUNIT_OK;
}

static MunitTest network_tests[] = {
    {"/disconnect", test_network_disconnect, setup, tear_down, 0, params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_liveness_suites[] = {
    {"/network", network_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
