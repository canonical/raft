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
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
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
 * Send AppendEntries messages
 *
 *****************************************************************************/

TEST_SUITE(send_append_entries);

TEST_SETUP(send_append_entries, setup);
TEST_TEAR_DOWN(send_append_entries, tear_down);

TEST_GROUP(send_append_entries, error);

static char *send_append_entries_oom_heap_fault_delay[] = {"5", NULL};
static char *send_append_entries_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum send_append_entries_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, send_append_entries_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, send_append_entries_oom_heap_fault_repeat},
    {NULL, NULL},
};

static void apply_cb(struct raft_apply *req, int status)
{
    (void)status;
    free(req);
}

/* Out of memory failures. */
TEST_CASE(send_append_entries, error, oom, send_append_entries_oom_params)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;

    test_heap_fault_enable(&f->heap);

    CLUSTER_APPLY_ADD_X(req, 1, apply_cb);
    CLUSTER_STEP;

    return MUNIT_OK;
}

/* A failure occurs upon submitting the I/O request. */
TEST_CASE(send_append_entries, error, io, NULL)
{
    struct fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;

    CLUSTER_IO_FAULT(0, 1, 1);

    CLUSTER_APPLY_ADD_X(req, 1, apply_cb);
    CLUSTER_STEP;

    return MUNIT_OK;
}

