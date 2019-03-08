#include "../../include/raft.h"
#include "../../include/raft/io_stub.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/tick.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(tick);

/**
 * Helpers
 */

struct fixture
{
    RAFT_FIXTURE;
};

/**
 * Setup and tear down
 */

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
 * Tick the raft instance of the given fixture and assert that no error
 * occurred.
 */
#define __tick(F, MSECS)                     \
    {                                        \
        raft_io_stub_advance(&F->io, MSECS); \
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
#define __assert_request_vote(F, SERVER_ID, TERM, LAST_LOG_INDEX,     \
                              LAST_LOG_TERM)                          \
    {                                                                 \
        struct raft_message *messages;                                \
        struct raft_request_vote *args;                               \
        unsigned n;                                                   \
                                                                      \
        raft_io_stub_flush(&F->io);                                   \
        raft_io_stub_sent(&F->io, &messages, &n);                     \
                                                                      \
        munit_assert_int(n, ==, 1);                                   \
        munit_assert_int(messages[0].type, ==, RAFT_IO_REQUEST_VOTE); \
        munit_assert_int(messages[0].server_id, ==, SERVER_ID);       \
                                                                      \
        args = &messages[0].request_vote;                             \
        munit_assert_int(args->term, ==, TERM);                       \
        munit_assert_int(args->last_log_index, ==, LAST_LOG_INDEX);   \
        munit_assert_int(args->last_log_term, ==, LAST_LOG_TERM);     \
    }

/**
 * Assert that the test I/O implementation has received exactly one
 * AppendEntries RPC with the given parameters and no entries.
 */
#define __assert_heartbeat(F, SERVER_ID, TERM, PREV_LOG_INDEX, PREV_LOG_TERM) \
    {                                                                         \
        struct raft_message *messages;                                        \
        struct raft_append_entries *args;                                     \
        unsigned n;                                                           \
                                                                              \
        raft_io_stub_flush(&F->io);                                           \
        raft_io_stub_sent(&F->io, &messages, &n);                             \
                                                                              \
        munit_assert_int(n, ==, 1);                                           \
        munit_assert_int(messages[0].type, ==, RAFT_IO_APPEND_ENTRIES);       \
                                                                              \
        args = &messages[0].append_entries;                                   \
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

TEST_SUITE(elapse);
TEST_SETUP(elapse, setup);
TEST_TEAR_DOWN(elapse, tear_down);

TEST_GROUP(elapse, error);
TEST_GROUP(elapse, success);

/* If we're in the unavailable state, raft__tick is a no-op. */
TEST_CASE(elapse, success, unavailable, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_start(&f->raft);
    munit_assert_int(rv, ==, 0);

    __tick(f, 100);
    __tick(f, 100);

    return MUNIT_OK;
}

/* Internal timers are updated according to the given time delta. */
TEST_CASE(elapse, success, update_timer, NULL)
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
TEST_CASE(elapse, success, self_elect, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

    __tick(f, 100);
    __assert_state(f, RAFT_STATE_LEADER);

    return MUNIT_OK;
}

/* If there's only a single voting server, but it's not us, stay follower. */
TEST_CASE(elapse, success, voter_not_us, NULL)
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
TEST_CASE(elapse, error, candidate_oom, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    raft_io_stub_set_time(&f->io, 100);

    rv = raft__tick(&f->raft);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due the disk being full. */
TEST_CASE(elapse, error, candidate_io_err, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 1, 1, 1);

    raft_io_stub_fault(&f->io, 0, 1);
    raft_io_stub_set_time(&f->io, 100);

    rv = raft__tick(&f->raft);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
TEST_CASE(elapse, success, candidate, NULL)
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
TEST_CASE(elapse, success, timer_not_expired, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __tick(f, f->raft.election_timeout_rand - 100);
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
TEST_CASE(elapse, success, not_voter, NULL)
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
TEST_CASE(elapse, success, heartbeat, NULL)
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
TEST_CASE(elapse, success, no_heartbeat, NULL)
{
    struct fixture *f = data;
    struct raft_message *messages;
    unsigned n;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Advance timer but not enough to expire the heartbeat timeout */
    __tick(f, f->raft.heartbeat_timeout - 100);

    /* We have sent no heartbeats */
    raft_io_stub_flush(&f->io);
    raft_io_stub_sent(&f->io, &messages, &n);
    munit_assert_int(n, ==, 0);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
TEST_CASE(elapse, success, new_election, NULL)
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
TEST_CASE(elapse, success, during_election, NULL)
{
    struct fixture *f = data;
    struct raft_message *messages;
    unsigned n;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Become candidate */
    __tick(f, f->raft.election_timeout_rand + 100);

    raft_io_stub_flush(f->raft.io);

    __tick(f, f->raft.election_timeout_rand - 100);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    /* No new vote request has been sent */
    raft_io_stub_flush(&f->io);
    raft_io_stub_sent(&f->io, &messages, &n);
    munit_assert_int(n, ==, 0);

    return MUNIT_OK;
}

/* Vote requests are sent only to voting servers. */
TEST_CASE(elapse, success, request_vote_only_to_voters, NULL)
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
