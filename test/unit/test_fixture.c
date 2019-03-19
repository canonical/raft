#include "../../include/raft/fixture.h"

#include "../lib/fsm.h"
#include "../lib/runner.h"

TEST_MODULE(fixture);

#define N_SERVERS 3

/******************************************************************************
 *
 * Helpers
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

    rc = raft_fixture_setup(&f->fixture, N_SERVERS, N_SERVERS, f->fsms,
                            munit_rand_int_range);
    munit_assert_int(rc, ==, 0);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    unsigned i;
    raft_fixture_tear_down(&f->fixture);
    for (i = 0; i < N_SERVERS; i++) {
        test_fsm_tear_down(&f->fsms[i]);
    }
    free(f);
}

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
    raft_fixture_elect(&f->fixture, 0);
    return MUNIT_OK;
}

/* Trigger the election of the second server. */
TEST_CASE(elect, second, NULL)
{
    struct fixture *f = data;
    (void)params;
    raft_fixture_elect(&f->fixture, 1);
    return MUNIT_OK;
}

/* Trigger an election change. */
TEST_CASE(elect, change, NULL)
{
    struct fixture *f = data;
    (void)params;
    raft_fixture_elect(&f->fixture, 0);
    raft_fixture_depose(&f->fixture);
    raft_fixture_elect(&f->fixture, 1);
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
    struct raft *raft = raft_fixture_get(&f->fixture, 0);
    struct raft_apply *req = munit_malloc(sizeof *req);
    int rc;
    (void)params;
    raft_fixture_elect(&f->fixture, 0);
    test_fsm_encode_set_x(123, &buf);
    rc = raft_apply(raft, req, &buf, 1, NULL);
    munit_assert_int(rc, ==, 0);
    raft_fixture_wait_applied(&f->fixture, 2);
    free(req);
    return MUNIT_OK;
}

/* Wait for two entries to be applied. */
TEST_CASE(wait_applied, two, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf1;
    struct raft_buffer buf2;
    struct raft *raft = raft_fixture_get(&f->fixture, 0);
    struct raft_apply *req1 = munit_malloc(sizeof *req1);
    struct raft_apply *req2 = munit_malloc(sizeof *req2);
    int rc;
    (void)params;
    raft_fixture_elect(&f->fixture, 0);
    test_fsm_encode_set_x(123, &buf1);
    test_fsm_encode_set_x(123, &buf2);
    rc = raft_apply(raft, req1, &buf1, 1, NULL);
    munit_assert_int(rc, ==, 0);
    rc = raft_apply(raft, req2, &buf2, 1, NULL);
    munit_assert_int(rc, ==, 0);
    raft_fixture_wait_applied(&f->fixture, 3);
    free(req1);
    free(req2);
    return MUNIT_OK;
}
