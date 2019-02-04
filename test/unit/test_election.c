#include "../../include/raft.h"

#include "../../src/configuration.h"
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
 * Set the state of the fixture's raft instance to #RAFT_STATE_CANDIDATE, and
 * initialize the votes array.
 */
#define __set_state_to_candidate(F)                                           \
    {                                                                         \
        int n_voting = raft_configuration__n_voting(&F->raft.configuration);  \
                                                                              \
        F->raft.state = RAFT_STATE_CANDIDATE;                                 \
        F->raft.candidate_state.votes = raft_malloc(n_voting * sizeof(bool)); \
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
 * raft_election__start
 */

/* An error occurs while persisting the new term. */
static MunitResult test_start_term_io_err(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __set_state_to_candidate(f);

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_election__start(&f->raft);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* An error occurs while persisting the vote. */
static MunitResult test_start_vote_io_err(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __set_state_to_candidate(f);

    raft_io_stub_fault(&f->io, 1, 1);

    rv = raft_election__start(&f->raft);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* If an error occurs while sending a request vote message, it's ignored. */
static MunitResult test_start_send_io_err(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __set_state_to_candidate(f);

    raft_io_stub_fault(&f->io, 2, 1);

    rv = raft_election__start(&f->raft);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* Messages are sent to request the votes of the other voting servers. */
static MunitResult test_start_send_messages(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    struct raft_message *messages;
    unsigned n;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);

    __set_state_to_candidate(f);

    rv = raft_election__start(&f->raft);
    munit_assert_int(rv, ==, 0);

    raft_io_stub_flush(&f->io);

    /* Since there's only one other voting server, we sent only one message. */
    raft_io_stub_sent(&f->io, &messages, &n);

    munit_assert_int(n, ==, 1);

    munit_assert_int(messages[0].server_id, ==, 2);
    munit_assert_string_equal(messages[0].server_address, "2");
    munit_assert_int(messages[0].request_vote.term, ==, 2);
    munit_assert_int(messages[0].request_vote.candidate_id, ==, 1);

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/term-io-err", test_start_term_io_err, setup, tear_down, 0, NULL},
    {"/vote-io-err", test_start_vote_io_err, setup, tear_down, 0, NULL},
    {"/send-io-err", test_start_send_io_err, setup, tear_down, 0, NULL},
    {"/send-messages", test_start_send_messages, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_election__vote
 */

/* The server requesting the vote has a term newer than ours. */
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

/* An error occurs when persisting the vote. */
static MunitResult test_vote_io_err(const MunitParameter params[], void *data)
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

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_election__vote(&f->raft, &args, &granted);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

static MunitTest vote_tests[] = {
    {"/newer-term", test_vote_newer_term, setup, tear_down, 0, NULL},
    {"/io-err", test_vote_io_err, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_election_suites[] = {
    {"/reset-timer", reset_timer_tests, NULL, 1, 0},
    {"/start", start_tests, NULL, 1, 0},
    {"/vote", vote_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
