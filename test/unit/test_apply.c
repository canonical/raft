#include "../lib/cluster.h"
#include "../lib/runner.h"

TEST_MODULE(apply);

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

static void apply_cb(struct raft_apply *req, int status, void *result)
{
    struct fixture *f = req->data;
    (void)result;
    f->invoked = true;
    f->status = status;
}

/* Submit a request to apply a new RAFT_COMMAND entry and assert that it returns
 * the given value. */
#define APPLY(I, RV)                                                      \
    {                                                                     \
        int rv_;                                                          \
        test_fsm_encode_set_x(123, &f->buf);                              \
        f->req.data = f;                                                  \
        rv_ = raft_apply(CLUSTER_RAFT(I), &f->req, &f->buf, 1, apply_cb); \
        munit_assert_int(rv_, ==, RV);                                    \
        if (rv_ != 0) {                                                   \
            raft_free(f->buf.base);                                       \
        }                                                                 \
    }

/******************************************************************************
 *
 * Failure scenarios
 *
 *****************************************************************************/

TEST_SUITE(error);
TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* If the raft instance is not in leader state, an error is returned. */
TEST_CASE(error, not_leader, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPLY(1, RAFT_NOTLEADER);
    munit_assert_false(f->invoked);
    return MUNIT_OK;
}

/* If the raft instance steps down from leader state, the apply callback fires
 * with an error. */
TEST_CASE(error, leadership_lost, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPLY(0, 0);
    CLUSTER_DEPOSE;
    munit_assert_true(f->invoked);
    munit_assert_int(f->status, ==, RAFT_LEADERSHIPLOST);
    return MUNIT_OK;
}
