#include <stdio.h>

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
 * Call raft_handle_request_vote with the given parameters and check that no
 * error occurs.
 */
#define __handle_request_vote(F, TERM, CANDIDATE_ID, LAST_LOG_INDEX,           \
                              LAST_LOG_TERM)                                   \
    {                                                                          \
        int rv;                                                                \
        struct raft_request_vote_args args;                                    \
        char address[4];                                                       \
                                                                               \
        sprintf(address, "%d", CANDIDATE_ID);                                  \
                                                                               \
        args.term = TERM;                                                      \
        args.candidate_id = CANDIDATE_ID;                                      \
        args.last_log_index = LAST_LOG_INDEX;                                  \
        args.last_log_term = LAST_LOG_TERM;                                    \
                                                                               \
        rv = raft_handle_request_vote(&F->raft, CANDIDATE_ID, address, &args); \
        munit_assert_int(rv, ==, 0);                                           \
    }

/**
 * Call raft_handle_request_vote_response with the given parameters and check
 * that no error occurs.
 */
#define __handle_request_vote_response(F, VOTER_ID, TERM, GRANTED)          \
    {                                                                       \
        struct raft_request_vote_result result;                             \
        char address[4];                                                    \
        int rv;                                                             \
                                                                            \
        sprintf(address, "%d", VOTER_ID);                                   \
                                                                            \
        result.term = TERM;                                                 \
        result.vote_granted = GRANTED;                                      \
        rv = raft_handle_request_vote_response(&F->raft, VOTER_ID, address, \
                                               &result);                    \
        munit_assert_int(rv, ==, 0);                                        \
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
#define __assert_request_vote_result(F, TERM, GRANTED)               \
    {                                                                \
        struct test_io_request request;                              \
        struct raft_request_vote_result *result;                     \
                                                                     \
        test_io_get_one_request(&F->io, RAFT_IO_REQUEST_VOTE_RESULT, \
                                &request);                           \
        result = &request.request_vote_response.result;              \
        munit_assert_int(result->term, ==, TERM);                    \
        munit_assert_int(result->vote_granted, ==, GRANTED);         \
    }

/**
 * Assert that the test I/O implementation has received exactly one
 * AppendEntries RPC with the given parameters and no entries.
 */
#define __assert_heartbeat(F, SERVER_ID, TERM, PREV_LOG_INDEX, PREV_LOG_TERM) \
    {                                                                         \
        struct test_io_request request;                                       \
        struct raft_append_entries_args *args;                                \
                                                                              \
        test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES, &request);    \
        args = &request.append_entries.args;                                  \
                                                                              \
        munit_assert_int(request.type, ==, RAFT_IO_APPEND_ENTRIES);           \
        munit_assert_int(request.request_vote.id, ==, SERVER_ID);             \
        munit_assert_int(args->term, ==, TERM);                               \
        munit_assert_int(args->prev_log_index, ==, PREV_LOG_INDEX);           \
        munit_assert_int(args->prev_log_term, ==, PREV_LOG_TERM);             \
                                                                              \
        munit_assert_ptr_null(args->entries);                                 \
        munit_assert_int(args->n, ==, 0);                                     \
    }

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
 * raft_handle_request_vote
 */

/* If the server's current term is higher than the one in the request, the vote
 * is not granted. */
static MunitResult test_req_higher_term(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3). */
    test_io_write_term_and_vote(&f->io, 3, 0);

    test_load(&f->raft);

    /* Handle a RequestVote RPC containing a lower term. */
    __handle_request_vote(f, 2, 2, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 3, false);

    return MUNIT_OK;
}

/* If the server already has a leader, the vote is not granted (even if the
   request has a higher term). */
static MunitResult test_req_has_leader(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    /* Receive a valid AppendEntries RPC to update our leader to server 2. */
    test_receive_heartbeat(&f->raft, 2);

    munit_assert_int(f->raft.follower_state.current_leader_id, ==, 2);

    /* Receive a vote request from server 3, with a higher term than ours. */
    __handle_request_vote(f, f->raft.current_term + 1, 3, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 1, false);

    return MUNIT_OK;
}

/* If we are not a voting server, the vote is not granted. */
static MunitResult test_req_non_voting(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 2, 2);

    __handle_request_vote(f, f->raft.current_term + 1, 2,
                          raft_log__last_index(&f->raft.log),
                          raft_log__last_term(&f->raft.log));

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    return MUNIT_OK;
}

/* If we have already voted, vote is not granted. */
static MunitResult test_req_already_voted(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    /* Grant vote to server 2 */
    __handle_request_vote(f, f->raft.current_term + 1, 2,
                          raft_log__last_index(&f->raft.log),
                          raft_log__last_term(&f->raft.log));

    munit_assert_int(f->raft.voted_for, ==, 2);

    test_io_flush(f->raft.io);

    /* Refuse vote to server 3 */
    __handle_request_vote(f, f->raft.current_term, 3,
                          raft_log__last_index(&f->raft.log),
                          raft_log__last_term(&f->raft.log));

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
static MunitResult test_req_dupe_vote(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Grant vote */
    __handle_request_vote(f, f->raft.current_term + 1, 2,
                          raft_log__last_index(&f->raft.log),
                          raft_log__last_term(&f->raft.log));

    munit_assert_int(f->raft.voted_for, ==, 2);

    test_io_flush(f->raft.io);

    /* Grant again */
    __handle_request_vote(f, f->raft.current_term, 2,
                          raft_log__last_index(&f->raft.log),
                          raft_log__last_term(&f->raft.log));

    /* The request is successful */
    __assert_request_vote_result(f, 2, true);

    return MUNIT_OK;
}

/* If server has an empty log, the vote is granted. */
static MunitResult test_req_empty_log(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __configuration_add(f, 1, "1", true);
    __configuration_add(f, 2, "2", true);

    __handle_request_vote(f, f->raft.current_term + 1, 2, 1, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 1, true);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
 * granted. */
static MunitResult test_req_last_term_lower(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    __handle_request_vote(f, 1, 2, 0, 0);

    /* The request is successful */
    __assert_request_vote_result(f, 1, false);

    return MUNIT_OK;
}

/* If the requester last log term is higher than ours, the vote is granted. */
static MunitResult test_req_last_term_higher(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3), although the term of our last log entry is still
     * 1. */
    rv = f->raft.io->write_term(f->raft.io, 3);
    munit_assert_int(rv, ==, 0);
    f->raft.current_term = 3;

    /* Receive a vote request from a server that has the same term 3, but whose
     * last log entry term is 2 (i.e. we lost an entry that was committed in
     * term 2). */
    __handle_request_vote(f, 3, 2, 2, 2);

    /* The request is successful */
    __assert_request_vote_result(f, 3, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher than ours, the vote is
 * granted. */
static MunitResult test_req_last_idx_higher(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3), although the term of our last log entry is still
     * 1. */
    rv = f->raft.io->write_term(f->raft.io, 3);
    munit_assert_int(rv, ==, 0);
    f->raft.current_term = 3;

    /* Receive a vote request from a server that has the same term 3, but whose
     * last log entry index is 2 (i.e. we lost an entry that was committed in
     * term 1). */
    __handle_request_vote(f, 3, 2, 2, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 3, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same as ours, the vote is
 * granted. */
static MunitResult test_req_last_idx_same_index(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    __handle_request_vote(f, 2, 2, 1, 1);

    /* The request is successful */
    __assert_request_vote_result(f, 2, true);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower than ours, the vote is not
 * granted. */
static MunitResult test_req_last_idx_lower_index(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Artificially bump to term 3 and commit a new entry (for instance we
     * received an AppendEntries request and then crashed while we were
     * candidate for term 2) */
    rv = f->raft.io->write_term(f->raft.io, 2);
    munit_assert_int(rv, ==, 0);
    f->raft.current_term = 2;

    entry.type = RAFT_LOG_COMMAND;
    entry.term = 1;
    entry.buf.base = NULL;
    entry.buf.len = 0;

    test_io_write_entry(f->raft.io, &entry);

    memset(&buf, 0, sizeof buf);
    raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &buf, NULL);

    munit_assert_int(raft_log__last_index(&f->raft.log), ==, 2);

    /* Receive a vote request from a server that does not have this new
     * entry. */
    __handle_request_vote(f, 2, 2, 1, 1);

    /* The request is unsuccessful */
    __assert_request_vote_result(f, 2, false);

    return MUNIT_OK;
}

static MunitTest req_tests[] = {
    {"/higher-term", test_req_higher_term, setup, tear_down, 0, NULL},
    {"/has-leader", test_req_has_leader, setup, tear_down, 0, NULL},
    {"/non-voting", test_req_non_voting, setup, tear_down, 0, NULL},
    {"/already-voted", test_req_already_voted, setup, tear_down, 0, NULL},
    {"/dupe-vote", test_req_dupe_vote, setup, tear_down, 0, NULL},
    {"/empty-log", test_req_empty_log, setup, tear_down, 0, NULL},
    {"/last-term-lower", test_req_last_term_lower, setup, tear_down, 0, NULL},
    {"/last-term-higher", test_req_last_term_higher, setup, tear_down, 0, NULL},
    {"/last-index-higher", test_req_last_idx_higher, setup, tear_down, 0, NULL},
    {"/last-index-same", test_req_last_idx_same_index, setup, tear_down, 0,
     NULL},
    {"/last-index-lower", test_req_last_idx_lower_index, setup, tear_down, 0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_handle_request_vote_response
 */

static char *res_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *res_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum res_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, res_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, res_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
static MunitResult test_res_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    result.term = 2;
    result.vote_granted = 1;

    test_heap_fault_enable(&f->heap);

    rv = raft_handle_request_vote_response(&f->raft, 2, "2", &result);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote and the
 * quorum is reached, it becomes leader. */
static MunitResult test_res_quorum(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __handle_request_vote_response(f, 2, 2, true);

    /* We are leader */
    __assert_state(f, RAFT_STATE_LEADER);

    /* The next_index array is initialized */
    munit_assert_ptr_not_null(f->raft.leader_state.next_index);
    munit_assert_int(f->raft.leader_state.next_index[0], ==, 2);
    munit_assert_int(f->raft.leader_state.next_index[1], ==, 2);

    /* The match_index array is initialized */
    munit_assert_ptr_not_null(f->raft.leader_state.match_index);
    munit_assert_int(f->raft.leader_state.match_index[0], ==, 0);
    munit_assert_int(f->raft.leader_state.match_index[1], ==, 0);

    /* We have sent heartbeats */
    __assert_heartbeat(f, 2, 2, 1, 1);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it statys candidate. */
static MunitResult test_res_no_quorum(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 5, 1, 5);
    test_become_candidate(&f->raft);

    __handle_request_vote_response(f, 2, 2, true);

    /* We are still candidate, since majority requires 3 votes, but we have
     * 2. */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    return MUNIT_OK;
}

/* If the server is not in candidate state the response gets discarded. */
static MunitResult test_res_not_candidate(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    __handle_request_vote_response(f, 2, 2, true);

    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the server receives a response contaning an higher term than its own, it
   converts to follower. */
static MunitResult test_res_step_down(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __handle_request_vote_response(f, 2, 3, false);

    /* The term has been saved to stable store and incremented. */
    munit_assert_int(f->raft.current_term, ==, 3);
    munit_assert_int(test_io_get_term(&f->io), ==, 3);

    /* The vote has been reset both in stable store and in the cache. */
    munit_assert_int(test_io_get_vote(&f->io), ==, 0);
    munit_assert_int(f->raft.voted_for, ==, 0);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.timer, ==, 0);

    /* We are follower */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    /* No leader is set. */
    munit_assert_int(f->raft.follower_state.current_leader_id, ==, 0);

    return MUNIT_OK;
}

/* The server receives a response contaning an higher term than its own, it
 * tries to convert to follower, but an I/O error occcurs. */
static MunitResult test_res_io_err(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    result.term = 3;
    result.vote_granted = 0;

    test_io_fault(&f->io, 0, 1);

    rv = raft_handle_request_vote_response(&f->raft, 2, "2", &result);
    munit_assert_int(rv, ==, RAFT_ERR_NO_SPACE);

    munit_assert_string_equal(
        raft_errmsg(&f->raft),
        "convert to follower: persist new term: no space left on device");

    return MUNIT_OK;
}

/* If a candidate server receives a response indicating that the vote was not
 * granted, nothing happens. */
static MunitResult test_res_not_granted(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    /* The receiver does not grant the vote (e.g. it has already voted for
       someone else). */
    __handle_request_vote_response(f, 2, 2, false);

    /* We are still candidate */
    __assert_state(f, RAFT_STATE_CANDIDATE);

    return MUNIT_OK;
}

static MunitTest res_tests[] = {
    {"/oom", test_res_oom, setup, tear_down, 0, res_oom_params},
    {"/quorum", test_res_quorum, setup, tear_down, 0, NULL},
    {"/no-quorum", test_res_no_quorum, setup, tear_down, 0, NULL},
    {"/not-candidate", test_res_not_candidate, setup, tear_down, 0, NULL},
    {"/step-down", test_res_step_down, setup, tear_down, 0, NULL},
    {"/io-err", test_res_io_err, setup, tear_down, 0, NULL},
    {"/not-granted", test_res_not_granted, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_rpc_request_vote_suites[] = {
    {"/req", req_tests, NULL, 1, 0},
    {"/res", res_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
