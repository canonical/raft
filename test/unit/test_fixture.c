#include "../../include/raft/fixture.h"

#include "../lib/fsm.h"
#include "../lib/runner.h"

TEST_MODULE(fixture);

#define N_SERVERS 3

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    struct raft_fsm fsms[N_SERVERS];
    struct raft_fixture fixture;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    unsigned i;
    int rc;
    (void)user_data;
    (void)params;
    for (i = 0; i < N_SERVERS; i++) {
        test_fsm_setup(params, &f->fsms[i]);
    }

    rc = raft_fixture_init(&f->fixture, N_SERVERS, f->fsms);
    munit_assert_int(rc, ==, 0);

    raft_fixture_set_random(&f->fixture, munit_rand_int_range);

    rc = raft_fixture_bootstrap(&f->fixture, N_SERVERS);
    munit_assert_int(rc, ==, 0);

    rc = raft_fixture_start(&f->fixture);
    munit_assert_int(rc, ==, 0);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    unsigned i;
    raft_fixture_close(&f->fixture);
    for (i = 0; i < N_SERVERS; i++) {
        test_fsm_tear_down(&f->fsms[i]);
    }
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define GET(I) raft_fixture_get(&f->fixture, I)
#define STATE(I) raft_state(GET(I))
#define ELECT(I) raft_fixture_elect(&f->fixture, I)
#define DEPOSE raft_fixture_depose(&f->fixture)
#define APPLY(I, REQ, BUF)                          \
    {                                               \
        int rc;                                     \
        test_fsm_encode_set_x(123, BUF);            \
        rc = raft_apply(GET(I), REQ, BUF, 1, NULL); \
        munit_assert_int(rc, ==, 0);                \
    }
#define WAIT_APPLIED(INDEX) \
    raft_fixture_wait_applied(&f->fixture, INDEX, INDEX * 1000)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the I'th server is in the given state. */
#define ASSERT_STATE(I, S) munit_assert_int(STATE(I), ==, S)

/******************************************************************************
 *
 * raft_fixture_elect
 *
 *****************************************************************************/

TEST_SUITE(elect);
TEST_SETUP(elect, setup);
TEST_TEAR_DOWN(elect, tear_down);

/* Trigger the election of the first server. */
TEST_CASE(elect, first, NULL)
{
    struct fixture *f = data;
    (void)params;
    ELECT(0);
    ASSERT_STATE(0, RAFT_LEADER);
    ASSERT_STATE(1, RAFT_FOLLOWER);
    ASSERT_STATE(2, RAFT_FOLLOWER);
    return MUNIT_OK;
}

/* Trigger the election of the second server. */
TEST_CASE(elect, second, NULL)
{
    struct fixture *f = data;
    (void)params;
    ELECT(1);
    ASSERT_STATE(0, RAFT_FOLLOWER);
    ASSERT_STATE(1, RAFT_LEADER);
    ASSERT_STATE(2, RAFT_FOLLOWER);
    return MUNIT_OK;
}

/* Trigger an election change. */
TEST_CASE(elect, change, NULL)
{
    struct fixture *f = data;
    (void)params;
    ELECT(0);
    DEPOSE;
    ASSERT_STATE(0, RAFT_FOLLOWER);
    ASSERT_STATE(1, RAFT_FOLLOWER);
    ASSERT_STATE(2, RAFT_FOLLOWER);
    ELECT(2);
    ASSERT_STATE(0, RAFT_FOLLOWER);
    ASSERT_STATE(1, RAFT_FOLLOWER);
    ASSERT_STATE(2, RAFT_LEADER);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_fixture_wait_applied
 *
 *****************************************************************************/

TEST_SUITE(wait_applied);
TEST_SETUP(wait_applied, setup);
TEST_TEAR_DOWN(wait_applied, tear_down);

/* Wait for one entry to be applied. */
TEST_CASE(wait_applied, one, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    struct raft_apply *req = munit_malloc(sizeof *req);
    (void)params;
    ELECT(0);
    APPLY(0, req, &buf);
    WAIT_APPLIED(2);
    free(req);
    return MUNIT_OK;
}

/* Wait for two entries to be applied. */
TEST_CASE(wait_applied, two, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    struct raft_apply *req1 = munit_malloc(sizeof *req1);
    struct raft_apply *req2 = munit_malloc(sizeof *req2);
    (void)params;
    ELECT(0);
    APPLY(0, req1, &buf1);
    APPLY(0, req2, &buf2);
    WAIT_APPLIED(3);
    free(req1);
    free(req2);
    return MUNIT_OK;
}
