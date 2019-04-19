#include "../lib/cluster.h"
#include "../lib/runner.h"

TEST_MODULE(barrier);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
    struct raft_buffer buf;
    struct raft_apply req;
    bool invoked;
    int status;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_CLUSTER(2);
    f->invoked = false;
    f->status = -1;
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
 * Helper macros
 *
 *****************************************************************************/

static void barrier_cb(struct raft_apply *req, int status)
{
    struct fixture *f = req->data;
    f->invoked = true;
    f->status = status;
}

/* Submit a request to apply a new RAFT_BARRIER entry and assert that it returns
 * the given value. */
#define BARRIER(I, RV)                                            \
    {                                                             \
        int rv_;                                                  \
        f->req.data = f;                                          \
        rv_ = raft_barrier(CLUSTER_RAFT(I), &f->req, barrier_cb); \
        munit_assert_int(rv_, ==, RV);                            \
    }

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

TEST_SUITE(success);
TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

TEST_CASE(success, cb, NULL)
{
    struct fixture *f = data;
    (void)params;
    BARRIER(0, 0);
    CLUSTER_STEP_UNTIL_APPLIED(0, 2, 2000);
    munit_assert_true(f->invoked);
    munit_assert_int(f->status, ==, 0);
    return MUNIT_OK;
}
