#include "../lib/cluster.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_CLUSTER;
};

static void *setUp(const MunitParameter params[], MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_CLUSTER(2);
    CLUSTER_BOOTSTRAP;
    CLUSTER_START;
    CLUSTER_ELECT(0);
    return f;
}

static void tearDown(void *data)
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

struct result
{
    int status;
    bool done;
};

static void applyCbAssertResult(struct raft_apply *req, int status, void *_)
{
    struct result *result = req->data;
    (void)_;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static bool applyCbHasFired(struct raft_fixture *f, void *arg)
{
    struct result *result = arg;
    (void)f;
    return result->done;
}

/* Submit an apply request. */
#define APPLY_SUBMIT(I)                                                      \
    struct raft_buffer _buf;                                                 \
    struct raft_apply _req;                                                  \
    struct result _result = {0, false};                                      \
    int _rv;                                                                 \
    test_fsm_encode_set_x(123, &_buf);                                       \
    _req.data = &_result;                                                    \
    _rv = raft_apply(CLUSTER_RAFT(I), &_req, &_buf, 1, applyCbAssertResult); \
    munit_assert_int(_rv, ==, 0);

/* Expect the apply callback to fire with the given status. */
#define APPLY_EXPECT(STATUS) _result.status = STATUS

/* Wait until an apply request comletes. */
#define APPLY_WAIT CLUSTER_STEP_UNTIL(applyCbHasFired, &_result, 2000)

/* Submit to the I'th server a request to apply a new RAFT_COMMAND entry and
 * wait for the operation to succeed. */
#define APPLY(I)         \
    do {                 \
        APPLY_SUBMIT(I); \
        APPLY_WAIT;      \
    } while (0)

/* Submit to the I'th server a request to apply a new RAFT_COMMAND entry and
 * assert that the given error is returned. */
#define APPLY_ERROR(I, RV, ERRMSG)                                \
    do {                                                          \
        struct raft_buffer _buf;                                  \
        struct raft_apply _req;                                   \
        int _rv;                                                  \
        test_fsm_encode_set_x(123, &_buf);                        \
        _rv = raft_apply(CLUSTER_RAFT(I), &_req, &_buf, 1, NULL); \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(CLUSTER_ERRMSG(I), ERRMSG);     \
        raft_free(_buf.base);                                     \
    } while (0)

/******************************************************************************
 *
 * Success scenarios
 *
 *****************************************************************************/

SUITE(raft_apply)

/* Append the very first command entry. */
TEST(raft_apply, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPLY(0);
    munit_assert_int(test_fsm_get_x(CLUSTER_FSM(0)), ==, 123);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Failure scenarios
 *
 *****************************************************************************/

/* If the raft instance is not in leader state, an error is returned. */
TEST(raft_apply, notLeader, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPLY_ERROR(1, RAFT_NOTLEADER, "server is not the leader");
    return MUNIT_OK;
}

/* If the raft instance steps down from leader state, the apply callback fires
 * with an error. */
TEST(raft_apply, leadershipLost, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPLY_SUBMIT(0);
    APPLY_EXPECT(RAFT_LEADERSHIPLOST);
    CLUSTER_DEPOSE;
    APPLY_WAIT;
    return MUNIT_OK;
}
