#include "../../include/raft.h"

#include "../../src/election.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/raft.h"

/**
 * Helpers
 */

struct fixture
{
    TEST_RAFT_FIXTURE_FIELDS;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    TEST_RAFT_FIXTURE_SETUP(f);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    TEST_RAFT_FIXTURE_TEAR_DOWN(f);

    free(f);
}

/**
 * raft_election__reset_timer
 */

/* The timer is set to a random value between election_timeout and
 * election_timeout * 2. */
static MunitResult test_reset_timer_range(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    raft_election__reset_timer(&f->raft);

    munit_assert_int(f->raft.election_timeout_rand, >=,
                     f->raft.election_timeout);

    munit_assert_int(f->raft.election_timeout_rand, <,
                     f->raft.election_timeout * 2);

    return MUNIT_OK;
}

static MunitTest reset_timer_tests[] = {
    {"/range", test_reset_timer_range, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_election__vote
 */

/* The server requesting the vote has a new term than us. */
static MunitResult test_vote_newer_term(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_request_vote args;
    bool granted;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    args.term = 2;
    args.candidate_id = 2;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_election__vote(&f->raft, &args, &granted);
    munit_assert_int(rv, ==, 0);

    munit_assert_true(granted);

    return MUNIT_OK;
}

static MunitTest vote_tests[] = {
    {"/newer-term", test_vote_newer_term, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_election_suites[] = {
    {"/reset-timer", reset_timer_tests, NULL, 1, 0},
    {"/vote", vote_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
