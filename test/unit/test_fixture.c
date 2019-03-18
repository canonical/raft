#include "../../include/raft/fixture.h"

#include "../lib/fsm.h"
#include "../lib/runner.h"

TEST_MODULE(fixture);

#define N_SERVERS 3

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

TEST_SUITE(elect);
TEST_SETUP(elect, setup);
TEST_TEAR_DOWN(elect, tear_down);

TEST_CASE(elect, first, NULL)
{
    struct fixture *f = data;
    (void)params;
    raft_fixture_elect(&f->fixture, 1);
    return MUNIT_OK;
}
