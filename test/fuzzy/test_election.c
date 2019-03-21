#include "../lib/cluster.h"
#include "../lib/heap.h"
#include "../lib/runner.h"

TEST_MODULE(election);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_CLUSTER;
};

static char *n[] = {"3", "5", "7", NULL};

static MunitParameterEnum params[] = {
    {CLUSTER_N_PARAM, n},
    {NULL, NULL},
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_HEAP;
    SETUP_CLUSTER(CLUSTER_N_PARAM_GET);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_CLUSTER;
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define STEP_UNTIL_HAS_LEADER(MAX_MSECS) \
    CLUSTER_STEP_UNTIL_HAS_LEADER(MAX_MSECS)

#define STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS) \
    CLUSTER_STEP_UNTIL_HAS_NO_LEADER(MAX_MSECS)

#define KILL(I) raft_fixture_kill(&f->cluster, I);
#define KILL_LEADER KILL(CLUSTER_LEADER)
#define KILL_MAJORITY                                      \
    {                                                      \
        size_t i;                                          \
        size_t n;                                          \
        for (i = 0, n = 0; n < (CLUSTER_N / 2) + 1; i++) { \
            KILL(i)                                        \
            n++;                                           \
        }                                                  \
    }

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

#define ASSERT_HAS_LEADER munit_assert_int(CLUSTER_LEADER, <, CLUSTER_N)
#define ASSERT_HAS_NO_LEADER munit_assert_int(CLUSTER_LEADER, ==, CLUSTER_N)

/******************************************************************************
 *
 * Tests
 *
 *****************************************************************************/

TEST_SUITE(run);
TEST_SETUP(run, setup);
TEST_TEAR_DOWN(run, tear_down);

/* A leader is eventually elected */
TEST_CASE(run, win, params)
{
    struct fixture *f = data;
    (void)params;
    STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_HAS_LEADER;
    return MUNIT_OK;
}

/* A new leader is elected if the current one dies. */
TEST_CASE(run, change, params)
{
    struct fixture *f = data;
    (void)params;
    STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_HAS_LEADER;
    KILL_LEADER;
    STEP_UNTIL_HAS_NO_LEADER(10000);
    STEP_UNTIL_HAS_LEADER(10000);
    ASSERT_HAS_LEADER;
    return MUNIT_OK;
}

/* If no majority of servers is online, no leader is elected. */
TEST_CASE(run, no_quorum, params)
{
    struct fixture *f = data;
    (void)params;
    KILL_MAJORITY;
    STEP_UNTIL_HAS_LEADER(30000);
    ASSERT_HAS_NO_LEADER;
    return MUNIT_OK;
}
