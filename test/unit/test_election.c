#include "../../include/raft.h"
#include "../../include/raft/io_stub.h"

#include "../../src/configuration.h"
#include "../../src/election.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(election);

/**
 * Helpers
 */

struct fixture
{
    RAFT_FIXTURE;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    RAFT_SETUP(f);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    RAFT_TEAR_DOWN(f);

    free(f);
}

/**
 * Set the state of the fixture's raft instance to #RAFT_CANDIDATE, and
 * initialize the votes array.
 */
#define __set_state_to_candidate(F)                                           \
    {                                                                         \
        int n_voting = configuration__n_voting(&F->raft.configuration);       \
                                                                              \
        F->raft.state = RAFT_CANDIDATE;                                       \
        F->raft.candidate_state.votes = raft_malloc(n_voting * sizeof(bool)); \
    }

/**
 * raft_election__reset_timer
 */

TEST_SUITE(reset_timer);

TEST_SETUP(reset_timer, setup);
TEST_TEAR_DOWN(reset_timer, tear_down);

TEST_GROUP(reset_timer, success);

/* The timer is set to a random value between election_timeout and
 * election_timeout * 2. */
TEST_CASE(reset_timer, success, range, NULL)
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

/**
 * raft_election__start
 */

TEST_SUITE(start);

TEST_SETUP(start, setup);
TEST_TEAR_DOWN(start, tear_down);

TEST_GROUP(start, error);

/* An error occurs while persisting the new term. */
TEST_CASE(start, error, term_io_err, NULL)
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
TEST_CASE(start, error, vote_io_err, NULL)
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
TEST_CASE(start, error, send_io_err, NULL)
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
TEST_CASE(start, error, send_messages, NULL)
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

/**
 * raft_election__vote
 */

TEST_SUITE(vote);

TEST_SETUP(vote, setup);
TEST_TEAR_DOWN(vote, tear_down);

TEST_GROUP(vote, success);
TEST_GROUP(vote, error);

/* The server requesting the vote has a term newer than ours. */
TEST_CASE(vote, success, newer_term, NULL)
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
TEST_CASE(vote, error, io_err, NULL)
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
