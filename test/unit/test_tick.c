#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/tick.h"

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
    const char *address = "1";
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_io_setup(params, &f->io, &f->logger);
    test_fsm_setup(params, &f->fsm);

    rv = raft_init(&f->raft, &f->logger, &f->io, &f->fsm, f, id, address);
    munit_assert_int(rv, ==, 0);

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
#define __tick(F, MSECS)                  \
    {                                     \
        int rv;                           \
                                          \
        rv = raft__tick(&F->raft, MSECS); \
        munit_assert_int(rv, ==, 0);      \
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
 * Assert that a RequestVote RPC with the given parameters has been submitted.
 */
#define __assert_request_vote(F, SERVER_ID, TERM, LAST_LOG_INDEX,        \
                              LAST_LOG_TERM)                             \
    {                                                                    \
        struct raft_message *message;                                    \
        struct raft_request_vote *args;                                  \
                                                                         \
        message = raft_io_stub_sending(&F->io, RAFT_IO_REQUEST_VOTE, 0); \
        munit_assert_ptr_not_null(message);                              \
                                                                         \
        args = &message->request_vote;                                   \
        munit_assert_int(message->server_id, ==, SERVER_ID);             \
        munit_assert_int(args->term, ==, TERM);                          \
        munit_assert_int(args->last_log_index, ==, LAST_LOG_INDEX);      \
        munit_assert_int(args->last_log_term, ==, LAST_LOG_TERM);        \
    }

/**
 * Assert that the test I/O implementation has received exactly one
 * AppendEntries RPC with the given parameters and no entries.
 */
#define __assert_heartbeat(F, SERVER_ID, TERM, PREV_LOG_INDEX, PREV_LOG_TERM) \
    {                                                                         \
        struct raft_message *message;                                         \
        struct raft_append_entries *args;                                     \
                                                                              \
        message = raft_io_stub_sending(&F->io, RAFT_IO_APPEND_ENTRIES, 0);    \
        munit_assert_ptr_not_null(message);                                   \
                                                                              \
        args = &message->append_entries;                                      \
                                                                              \
        munit_assert_int(args->term, ==, TERM);                               \
        munit_assert_int(args->prev_log_index, ==, PREV_LOG_INDEX);           \
        munit_assert_int(args->prev_log_term, ==, PREV_LOG_TERM);             \
                                                                              \
        munit_assert_ptr_null(args->entries);                                 \
        munit_assert_int(args->n_entries, ==, 0);                             \
    }

/**
 * raft__tick
 */

/* If we're in the unavailable state, raft__tick is a no-op. */
static MunitResult test_unavailable(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __tick(f, 100);
    __tick(f, 100);

    return MUNIT_OK;
}

/* Internal timers are updated according to the given time delta. */
static MunitResult test_updates_timers(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

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

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

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

    test_bootstrap_and_start(&f->raft, 2, 2, 2);

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

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft__tick(&f->raft, 100);
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

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft__tick(&f->raft, 100);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
static MunitResult test_candidate(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int election_timeout;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    election_timeout = f->raft.election_timeout_rand;

    __tick(f, f->raft.election_timeout_rand + 100);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 2);
    munit_assert_int(raft_io_stub_term(&f->io), ==, 2);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(raft_io_stub_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    __assert_request_vote(f, 2, 2, 1, 1);

    return MUNIT_OK;
}

/* If the electippon timeout has not elapsed, stay follower. */
static MunitResult test_election_timer_not_expired(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __tick(f, f->raft.election_timeout_rand - 100);
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
static MunitResult test_not_voter(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 2, 3);

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

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Expire the heartbeat timeout */
    __tick(f, f->raft.heartbeat_timeout + 100);

    /* We have sent a heartbeat to our follower */
    __assert_heartbeat(f, 2, 2, 1, 1);

    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has not elapsed, do nothing. */
static MunitResult test_heartbeat_timeout_not_elapsed(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct raft_message *message;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Advance timer but not enough to expire the heartbeat timeout */
    __tick(f, f->raft.heartbeat_timeout - 100);

    /* We have sent no heartbeats */
    message = raft_io_stub_sending(&f->io, RAFT_IO_REQUEST_VOTE, 0);
    munit_assert_ptr_null(message);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
static MunitResult test_new_election(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int election_timeout;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    raft_io_stub_flush(f->raft.io);

    election_timeout = f->raft.election_timeout_rand;

    /* Expire the election timeout */
    __tick(f, f->raft.election_timeout_rand + 100);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 3);
    munit_assert_int(raft_io_stub_term(&f->io), ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(raft_io_stub_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    /* We have sent vote requests again */
    __assert_request_vote(f, 2, 3, 1, 1);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay candidate. */
static MunitResult test_during_election(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_message *message;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    raft_io_stub_flush(f->raft.io);

    __tick(f, f->raft.election_timeout_rand - 100);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* No new vote request has been sent */
    message = raft_io_stub_sending(&f->io, RAFT_IO_REQUEST_VOTE, 0);
    munit_assert_ptr_null(message);

    return MUNIT_OK;
}

/* Vote requests are sent only to voting servers. */
static MunitResult test_request_vote_only_to_voters(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    /* We have sent vote requests only to the voting server */
    __assert_request_vote(f, 2, 2, 1, 1);

    return MUNIT_OK;
}

static MunitTest tick_tests[] = {
    {"/unavailable", test_unavailable, setup, tear_down, 0, NULL},
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
