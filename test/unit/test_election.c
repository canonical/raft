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

    f->raft.state = RAFT_STATE_CANDIDATE;
    f->raft.candidate_state.votes = raft_malloc(2 * sizeof(bool));

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

    f->raft.state = RAFT_STATE_CANDIDATE;
    f->raft.candidate_state.votes = raft_malloc(2 * sizeof(bool));

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

    f->raft.state = RAFT_STATE_CANDIDATE;
    f->raft.candidate_state.votes = raft_malloc(2 * sizeof(bool));

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
    unsigned n;
    struct raft_message *message;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);

    f->raft.state = RAFT_STATE_CANDIDATE;
    f->raft.candidate_state.votes = raft_malloc(2 * sizeof(bool));

    rv = raft_election__start(&f->raft);
    munit_assert_int(rv, ==, 0);

    /* Since there's only one other voting server, we sent only one message. */
    n = raft_io_stub_sending_n(&f->io, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(n, ==, 1);

    message = raft_io_stub_sending(&f->io, RAFT_IO_REQUEST_VOTE, 0);
    munit_assert_int(message->server_id, ==, 2);
    munit_assert_string_equal(message->server_address, "2");
    munit_assert_int(message->request_vote.term, ==, 2);
    munit_assert_int(message->request_vote.candidate_id, ==, 1);

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
static MunitResult test_vote_io_err(const MunitParameter params[],
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
