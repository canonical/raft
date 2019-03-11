#include <stdio.h>

#include "../../include/raft.h"
#include "../../include/raft/io_stub.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/rpc_request_vote.h"
#include "../../src/tick.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/logger.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(rpc_request_vote);

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
 * Call raft_rpc__recv_request_vote with the given parameters and check that no
 * error occurs.
 */
#define __recv_request_vote(F, TERM, CANDIDATE_ID, LAST_LOG_INDEX,        \
                            LAST_LOG_TERM)                                \
    {                                                                     \
        int rv;                                                           \
        struct raft_request_vote args;                                    \
        char address[4];                                                  \
                                                                          \
        sprintf(address, "%d", CANDIDATE_ID);                             \
                                                                          \
        args.term = TERM;                                                 \
        args.candidate_id = CANDIDATE_ID;                                 \
        args.last_log_index = LAST_LOG_INDEX;                             \
        args.last_log_term = LAST_LOG_TERM;                               \
                                                                          \
        rv = raft_rpc__recv_request_vote(&F->raft, CANDIDATE_ID, address, \
                                         &args);                          \
        munit_assert_int(rv, ==, 0);                                      \
    }

/**
 * Call raft_rpc__recv_request_vote_result with the given parameters and check
 * that no error occurs.
 */
#define __recv_request_vote_result(F, VOTER_ID, TERM, GRANTED)               \
    {                                                                        \
        struct raft_request_vote_result result;                              \
        char address[4];                                                     \
        int rv;                                                              \
                                                                             \
        sprintf(address, "%d", VOTER_ID);                                    \
                                                                             \
        result.term = TERM;                                                  \
        result.vote_granted = GRANTED;                                       \
        rv = raft_rpc__recv_request_vote_result(&F->raft, VOTER_ID, address, \
                                                &result);                    \
        munit_assert_int(rv, ==, 0);                                         \
    }

/**
 * Add a server to the configuration of the raft instance of the given fixture.
 */
#define __configuration_add(F, ID, ADDRESS, VOTING)                      \
    {                                                                    \
        int rv;                                                          \
        rv = raft_configuration_add(&F->raft.configuration, ID, ADDRESS, \
                                    VOTING);                             \
        munit_assert_int(rv, ==, 0);                                     \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * Assert that the I/O queue has exactly one pending RAFT_IO_REQUEST_VOTE_RESULT
 * request, with the given parameters.
 */
#define __assert_request_vote_result(F, TERM, GRANTED)       \
    {                                                        \
        struct raft_message *messages;                       \
        struct raft_request_vote_result *result;             \
        unsigned n;                                          \
                                                             \
        raft_io_stub_flush(&F->io);                          \
        raft_io_stub_sent(&F->io, &messages, &n);            \
        munit_assert_int(n, ==, 1);                          \
                                                             \
        result = &messages[0].request_vote_result;           \
        munit_assert_int(result->term, ==, TERM);            \
        munit_assert_int(result->vote_granted, ==, GRANTED); \
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
        munit_assert_int(n, ==, 1);                                           \
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
 * Receive a RequestVote message.
 */

TEST_SUITE(request);

TEST_SETUP(request, setup);
TEST_TEAR_DOWN(request, tear_down);

TEST_GROUP(request, error);
TEST_GROUP(request, success);

/* If the server's current term is higher than the one in the request, the vote
 * is not granted. */
TEST_CASE(request, error, higher_term, NULL)
{
    struct fixture *f = data;
    return MUNIT_OK;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3). */
    test_io_set_term_and_vote(&f->io, 3, 0);

    test_start(&f->raft);

    /* Handle a RequestVote RPC containing a lower term. */
    __recv_request_vote(f, 2, 2, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 3, false);

    return MUNIT_OK;
}

/* If the server already has a leader, the vote is not granted (even if the
   request has a higher term). */
TEST_CASE(request, error, has_leader, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

    /* Receive a valid AppendEntries RPC to update our leader to server 2. */
    test_receive_heartbeat(&f->raft, 2);

    munit_assert_int(f->raft.follower_state.current_leader_id, ==, 2);

    /* Receive a vote request from server 3, with a higher term than ours. */
    __recv_request_vote(f, f->raft.current_term + 1, 3, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 1, false);

    return MUNIT_OK;
}

/* If we are not a voting server, the vote is not granted. */
TEST_CASE(request, error, non_voting, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 2, 2);

    __recv_request_vote(f, f->raft.current_term + 1, 2,
                        log__last_index(&f->raft.log),
                        log__last_term(&f->raft.log));

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    return MUNIT_OK;
}

/* If we have already voted, vote is not granted. */
TEST_CASE(request, error, already_voted, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

    /* Grant vote to server 2 */
    __recv_request_vote(f, f->raft.current_term + 1, 2,
                        log__last_index(&f->raft.log),
                        log__last_term(&f->raft.log));

    munit_assert_int(f->raft.voted_for, ==, 2);

    raft_io_stub_flush(f->raft.io);

    /* Refuse vote to server 3 */
    __recv_request_vote(f, f->raft.current_term, 3,
                        log__last_index(&f->raft.log),
                        log__last_term(&f->raft.log));

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
TEST_CASE(request, error, dupe_vote, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Grant vote */
    __recv_request_vote(f, f->raft.current_term + 1, 2,
                        log__last_index(&f->raft.log),
                        log__last_term(&f->raft.log));

    munit_assert_int(f->raft.voted_for, ==, 2);

    raft_io_stub_flush(f->raft.io);

    /* Grant again */
    __recv_request_vote(f, f->raft.current_term, 2,
                        log__last_index(&f->raft.log),
                        log__last_term(&f->raft.log));

    /* The request is successful */
    __assert_request_vote_result(f, 2, true);

    return MUNIT_OK;
}

/* If server has an empty log, the vote is granted. */
TEST_CASE(request, error, empty_log, NULL)
{
    struct fixture *f = data;

    (void)params;

    __configuration_add(f, 1, "1", true);
    __configuration_add(f, 2, "2", true);

    f->raft.state = RAFT_FOLLOWER;

    __recv_request_vote(f, f->raft.current_term + 1, 2, 1, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 1, true);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
TEST_CASE(request, error, last_term_lower, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_request_vote(f, 1, 2, 0, 0);

    /* The request is successful */
    __assert_request_vote_result(f, 1, false);

    return MUNIT_OK;
}

/* If the requester last log term is higher than ours, the vote is granted. */
TEST_CASE(request, success, last_term_higher, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3), although the term of our last log entry is still
     * 1. */
    test_io_set_term_and_vote(f->raft.io, 3, 0);

    test_start(&f->raft);

    /* Receive a vote request from a server that has the same term 3, but whose
     * last log entry term is 2 (i.e. we lost an entry that was committed in
     * term 2). */
    __recv_request_vote(f, 3, 2, 2, 2);

    /* The request is successful */
    __assert_request_vote_result(f, 3, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher than ours, the vote is
 * granted. */
TEST_CASE(request, success, last_idx_higher, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3), although the term of our last log entry is still
     * 1. */
    test_io_set_term_and_vote(f->raft.io, 3, 0);

    test_start(&f->raft);

    /* Receive a vote request from a server that has the same term 3, but whose
     * last log entry index is 2 (i.e. we lost an entry that was committed in
     * term 1). */
    __recv_request_vote(f, 3, 2, 2, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 3, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same as ours, the vote is
 * granted. */
TEST_CASE(request, success, last_idx_same_index, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_request_vote(f, 2, 2, 1, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 2, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower than ours, the vote is not
 * granted. */
TEST_CASE(request, error, last_idx_lower_index, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 and commit a new entry (for instance we
     * received an AppendEntries request and then crashed while we were
     * candidate for term 2) */
    test_io_set_term_and_vote(&f->io, 2, 0);

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 1;
    entry.buf.base = raft_malloc(8);
    entry.buf.len = 8;

    test_io_append_entry(f->raft.io, &entry);

    test_start(&f->raft);

    munit_assert_int(log__last_index(&f->raft.log), ==, 2);

    /* Receive a vote request from a server that does not have this new
     * entry. */
    __recv_request_vote(f, 2, 2, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    raft_free(entry.buf.base);

    return MUNIT_OK;
}

/**
 * raft_rpc__recv_request_vote_result
 */

TEST_SUITE(response);

TEST_SETUP(response, setup);
TEST_TEAR_DOWN(response, tear_down);

TEST_GROUP(response, error);
TEST_GROUP(response, success);

static char *res_oom_heap_fault_delay[] = {"0", NULL};
static char *res_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum res_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, res_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, res_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST_CASE(response, error, oom, res_oom_params)
{
    struct fixture *f = data;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    result.term = 2;
    result.vote_granted = 1;

    test_heap_fault_enable(&f->heap);

    rv = raft_rpc__recv_request_vote_result(&f->raft, 2, "2", &result);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote and the
 * quorum is reached, it becomes leader. */
TEST_CASE(response, success, quorum, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __recv_request_vote_result(f, 2, 2, true);

    /* We are leader */
    __assert_state(f, RAFT_LEADER);

    munit_assert_ptr_not_null(f->raft.leader_state.replication);

    /* The next_index array is initialized */
    munit_assert_int(f->raft.leader_state.replication[0].next_index, ==, 2);
    munit_assert_int(f->raft.leader_state.replication[1].next_index, ==, 2);

    /* The match_index array is initialized */
    munit_assert_int(f->raft.leader_state.replication[0].match_index, ==, 0);
    munit_assert_int(f->raft.leader_state.replication[1].match_index, ==, 0);

    /* We have sent heartbeats */
    __assert_heartbeat(f, 2, 2, 1, 1);

    raft_io_stub_flush(&f->io);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it statys candidate. */
TEST_CASE(response, success, no_quorum, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 5, 1, 5);
    test_become_candidate(&f->raft);

    __recv_request_vote_result(f, 2, 2, true);

    /* We are still candidate, since majority requires 3 votes, but we have
     * 2. */
    __assert_state(f, RAFT_CANDIDATE);

    return MUNIT_OK;
}

/* If the server is not in candidate state the response gets discarded. */
TEST_CASE(response, success, not_candidate, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_request_vote_result(f, 2, 2, true);

    __assert_state(f, RAFT_FOLLOWER);

    return MUNIT_OK;
}

/* If the server receives a response contaning an higher term than its own, it
   converts to follower. */
TEST_CASE(response, success, step_down, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __recv_request_vote_result(f, 2, 3, false);

    /* The term has been saved to stable store and incremented. */
    munit_assert_int(f->raft.current_term, ==, 3);
    munit_assert_int(raft_io_stub_term(&f->io), ==, 3);

    /* The vote has been reset both in stable store and in the cache. */
    munit_assert_int(raft_io_stub_vote(&f->io), ==, 0);
    munit_assert_int(f->raft.voted_for, ==, 0);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.timer, ==, 0);

    /* We are follower */
    munit_assert_int(f->raft.state, ==, RAFT_FOLLOWER);

    /* No leader is set. */
    munit_assert_int(f->raft.follower_state.current_leader_id, ==, 0);

    return MUNIT_OK;
}

/* The server receives a response contaning an higher term than its own, it
 * tries to convert to follower, but an I/O error occcurs. */
TEST_CASE(response, error, io, NULL)
{
    struct fixture *f = data;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    result.term = 3;
    result.vote_granted = 0;

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_rpc__recv_request_vote_result(&f->raft, 2, "2", &result);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* If a candidate server receives a response indicating that the vote was not
 * granted, nothing happens. */
TEST_CASE(response, success, not_granted, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    /* The receiver does not grant the vote (e.g. it has already voted for
       someone else). */
    __recv_request_vote_result(f, 2, 2, false);

    /* We are still candidate */
    __assert_state(f, RAFT_CANDIDATE);

    return MUNIT_OK;
}
