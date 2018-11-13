#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"

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
    struct raft_heap heap;
    struct raft_logger logger;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
};

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    uint64_t id = 1;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_io_setup(params, &f->io);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);

    raft_set_logger(&f->raft, &f->logger);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);
    test_io_tear_down(&f->io);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Tick the raft instance of the given fixture and assert that no error
 * occurred.
 */
#define __tick(F, MSECS)                 \
    {                                    \
        int rv;                          \
                                         \
        rv = raft_tick(&F->raft, MSECS); \
        munit_assert_int(rv, ==, 0);     \
    }

/**
 * Assert the current value of the timer of the raft instance of the given
 * fixture.
 */
#define __assert_timer(F, MSECS) munit_assert_int(F->raft.timer, ==, MSECS);

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * Assert that the given I/O request is a RequestVote RPC with the given
 * parameters.
 */
#define __assert_request_vote(REQUEST, SERVER_ID, TERM, LAST_LOG_INDEX,     \
                              LAST_LOG_TERM)                                \
    {                                                                       \
        struct raft_request_vote_args *args = &(REQUEST.request_vote.args); \
                                                                            \
        munit_assert_int(REQUEST.type, ==, RAFT_IO_REQUEST_VOTE);           \
        munit_assert_int(REQUEST.request_vote.id, ==, SERVER_ID);           \
        munit_assert_int(args->term, ==, TERM);                             \
        munit_assert_int(args->last_log_index, ==, LAST_LOG_INDEX);         \
        munit_assert_int(args->last_log_term, ==, LAST_LOG_TERM);           \
    }

/**
 * Assert that the given I/O request is an AppendEntries RPC with the given
 * parameters and no entries.
 */
#define __assert_heartbeat(REQUEST, SERVER_ID, TERM, PREV_LOG_INDEX, \
                           PREV_LOG_TERM)                            \
    {                                                                \
        struct raft_append_entries_args *args =                      \
            &(REQUEST.append_entries.args);                          \
                                                                     \
        munit_assert_int(REQUEST.type, ==, RAFT_IO_APPEND_ENTRIES);  \
        munit_assert_int(REQUEST.request_vote.id, ==, SERVER_ID);    \
        munit_assert_int(args->term, ==, TERM);                      \
        munit_assert_int(args->prev_log_index, ==, PREV_LOG_INDEX);  \
        munit_assert_int(args->prev_log_term, ==, PREV_LOG_TERM);    \
                                                                     \
        munit_assert_ptr_null(args->entries);                        \
        munit_assert_int(args->n, ==, 0);                            \
    }

/**
 * raft_tick
 */

/* Internal timers are updated according to the given time delta. */
static MunitResult test_updates_timers(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    __tick(f, 100);
    __assert_timer(f, 100);

    __tick(f, 100);
    __assert_timer(f, 200);

    return MUNIT_OK;
}

/* If there's only a single voting server and that's us, switch to leader
   state. */
static MunitResult test_self_elect(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    __tick(f, 100);
    __assert_state(f, RAFT_STATE_LEADER);

    return MUNIT_OK;
}

/* If there's only a single voting server, but it's not us, stay follower. */
static MunitResult test_one_voter_not_us(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 2, 2);

    __tick(f, 100);
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due to an OOM error. */
static MunitResult test_candidate_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    munit_assert_string_equal(
        raft_errmsg(&f->raft),
        "convert to candidate: alloc votes array: out of memory");

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due the disk being full. */
static MunitResult test_candidate_io_err(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    test_io_fault(&f->io, 0, 1);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, RAFT_ERR_NO_SPACE);

    munit_assert_string_equal(
        raft_errmsg(&f->raft),
        "convert to candidate: persist term: no space left on device");

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
static MunitResult test_candidate(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int election_timeout;
    struct test_io_request request;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    election_timeout = f->raft.election_timeout_rand;

    __tick(f, f->raft.election_timeout_rand + 100);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 2);
    munit_assert_int(test_io_get_term(&f->io), ==, 2);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(test_io_get_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    /* We have sent a vote request to the other server. */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE, &request);

    __assert_request_vote(request, 2, 2, 1, 1);

    return MUNIT_OK;
}

/* If the electippon timeout has not elapsed, stay follower. */
static MunitResult test_election_timer_not_expired(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    __tick(f, f->raft.election_timeout_rand - 100);
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
static MunitResult test_not_voter(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 2, 3);

    __tick(f, f->raft.election_timeout_rand + 100);
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has elapsed, send empty
 * AppendEntries RPCs. */
static MunitResult test_heartbeat_timeout_elapsed(const MunitParameter params[],
                                                  void *data)
{
    struct fixture *f = data;
    struct test_io_request request;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Expire the heartbeat timeout */
    __tick(f, f->raft.heartbeat_timeout + 100);

    /* We have sent a heartbeat to our follower */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES, &request);

    __assert_heartbeat(request, 2, 2, 1, 1);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has not elapsed, do nothing. */
static MunitResult test_heartbeat_timeout_not_elapsed(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    size_t n_requests;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Advance timer but not enough to expire the heartbeat timeout */
    __tick(f, f->raft.heartbeat_timeout - 100);

    /* We have sent no heartbeats */
    n_requests = test_io_n_requests(&f->io, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(n_requests, ==, 0);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
static MunitResult test_new_election(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int election_timeout;
    struct test_io_request request;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    test_io_flush(f->raft.io);

    election_timeout = f->raft.election_timeout_rand;

    /* Expire the election timeout */
    __tick(f, f->raft.election_timeout_rand + 100);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 3);
    munit_assert_int(test_io_get_term(&f->io), ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(test_io_get_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    /* We have sent vote requests again */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE, &request);

    __assert_request_vote(request, 2, 3, 1, 1);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay candidate. */
static MunitResult test_during_election(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    size_t n_requests;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    test_io_flush(f->raft.io);

    __tick(f, f->raft.election_timeout_rand - 100);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* No new vote request has been sent */
    n_requests = test_io_n_requests(&f->io, RAFT_IO_REQUEST_VOTE);
    munit_assert_int(n_requests, ==, 0);

    return MUNIT_OK;
}

/* Vote requests are sent only to voting servers. */
static MunitResult test_request_vote_only_to_voters(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct test_io_request request;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    /* We have sent vote requests only to the voting server */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE, &request);

    __assert_request_vote(request, 2, 2, 1, 1);

    return MUNIT_OK;
}

static MunitTest tick_tests[] = {
    {"/updates-timers", test_updates_timers, setup, tear_down, 0, NULL},
    {"/self-elect", test_self_elect, setup, tear_down, 0, NULL},
    {"/one-voter-not-us", test_one_voter_not_us, setup, tear_down, 0, NULL},
    {"/candidate-oom", test_candidate_oom, setup, tear_down, 0, NULL},
    {"/candidate-io-err", test_candidate_io_err, setup, tear_down, 0, NULL},
    {"/candidate", test_candidate, setup, tear_down, 0, NULL},
    {"/election-timer-not-expired", test_election_timer_not_expired, setup,
     tear_down, 0, NULL},
    {"/not-voter", test_not_voter, setup, tear_down, 0, NULL},
    {"/heartbeat-timeout-elapsed", test_heartbeat_timeout_elapsed, setup,
     tear_down, 0, NULL},
    {"/heartbeat-timeout-not-elapsed", test_heartbeat_timeout_not_elapsed,
     setup, tear_down, 0, NULL},
    {"/new-election", test_new_election, setup, tear_down, 0, NULL},
    {"/during-election", test_during_election, setup, tear_down, 0, NULL},
    {"/request-vote-only-to-voters", test_request_vote_only_to_voters, setup,
     tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_tick_suites[] = {
    {"", tick_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
