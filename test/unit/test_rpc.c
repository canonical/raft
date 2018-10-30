#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/io.h"
#include "../../src/log.h"

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
    struct raft raft;
};

static struct raft_entry *__create_entries_batch()
{
    void *batch;
    struct raft_entry *entries;

    batch = raft_malloc(8 +  /*Number of entries in the batch, little endian */
                        16 + /* Header data of the first entry */
                        8 /* Payload data of the first entry */);
    munit_assert_ptr_not_null(batch);

    entries = raft_malloc(sizeof *entries);
    entries[0].term = 1;
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].buf.base = batch + 8 + 16;
    entries[0].buf.len = 8;
    entries[0].batch = batch;

    return entries;
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

    raft_init(&f->raft, &f->io, f, id);

    raft_set_logger(&f->raft, &f->logger);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

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
static MunitResult test_refuse_vote_if_higher_term(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_io_bootstrap(&f->io, 2, 1, 2);

    /* Artificially bump to term 3 (for instance we crashed while we were
     * candidate for term 3). */
    test_io_write_term_and_vote(&f->io, 3, 0);

    test_load(&f->raft);

    /* Handle a RequestVote RPC containing a lower term. */
    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.candidate_id = server->id;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 3);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the server already has a leader, the vote is not granted (even if the
   request has a higher term). */
static MunitResult test_refuse_if_has_leader(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    server = raft_configuration__get(&f->raft.configuration, 3);

    /* Receive a valid AppendEntries RPC to update our leader to server 2. */
    test_receive_heartbeat(&f->raft, 2);

    munit_assert_ptr_not_null(f->raft.follower_state.current_leader);
    munit_assert_int(f->raft.follower_state.current_leader->id, ==, 2);

    /* Receive a vote request from server 3, with a higher term than ours. */
    args.term = f->raft.current_term + 1;
    args.candidate_id = 3;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 1);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If we are not a voting server, the vote is not granted. */
static MunitResult test_refuse_if_non_voting(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 2, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = f->raft.current_term + 1;
    args.candidate_id = server->id;
    args.last_log_index = raft_log__last_index(&f->raft.log);
    args.last_log_term = raft_log__last_term(&f->raft.log);

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 2);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If we have already voted, vote is not granted. */
static MunitResult test_refuse_if_already_voted(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    const struct raft_server *server1;
    const struct raft_server *server2;
    struct raft_request_vote_args args1;
    struct raft_request_vote_args args2;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    server1 = raft_configuration__get(&f->raft.configuration, 2);
    server2 = raft_configuration__get(&f->raft.configuration, 3);

    /* Grant vote to server1 */
    args1.term = f->raft.current_term + 1;
    args1.candidate_id = server1->id;
    args1.last_log_index = raft_log__last_index(&f->raft.log);
    args1.last_log_term = raft_log__last_term(&f->raft.log);

    rv = raft_handle_request_vote(&f->raft, server1, &args1);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.voted_for, ==, 2);

    test_io_flush(f->raft.io);

    /* Refuse vote to server2 */
    args2.term = f->raft.current_term;
    args2.candidate_id = server2->id;
    args1.last_log_index = raft_log__last_index(&f->raft.log);
    args1.last_log_term = raft_log__last_term(&f->raft.log);

    rv = raft_handle_request_vote(&f->raft, server2, &args2);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 2);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If we have already voted and the same candidate requests the vote again, the
 * vote is granted. */
static MunitResult test_duplicate_vote(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    /* Grant vote */
    args.term = f->raft.current_term + 1;
    args.candidate_id = server->id;
    args.last_log_index = raft_log__last_index(&f->raft.log);
    args.last_log_term = raft_log__last_term(&f->raft.log);

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.voted_for, ==, 2);

    test_io_flush(f->raft.io);

    /* Grant again */
    args.term = f->raft.current_term;
    args.candidate_id = server->id;
    args.last_log_index = raft_log__last_index(&f->raft.log);
    args.last_log_term = raft_log__last_term(&f->raft.log);

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 2);
    munit_assert_true(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If server has an empty log, the vote is granted. */
static MunitResult test_grant_if_empty_log(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    rv = raft_configuration_add(&f->raft.configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_configuration_add(&f->raft.configuration, 2, "2", true);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = f->raft.current_term + 1;
    args.candidate_id = server->id;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 1);
    munit_assert_true(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the requester last log entry term is lower than ours, the vote is not
   granted. */
static MunitResult test_last_term_lower(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.candidate_id = 2;
    args.last_log_index = 0;
    args.last_log_term = 0;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 1);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the requester last log term is higher than ours, the vote is granted. */
static MunitResult test_last_term_higher(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
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
    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 3;
    args.candidate_id = 2;
    args.last_log_index = 2;
    args.last_log_term = 2;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 3);
    munit_assert_true(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the requester last log entry index is higher than ours, the vote is
   granted. */
static MunitResult test_last_idx_higher(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
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
    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 3;
    args.candidate_id = 2;
    args.last_log_index = 2;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 3);
    munit_assert_true(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the requester last log entry index is the same as ours, the vote is
 * granted. */
static MunitResult test_last_idx_same_index(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.candidate_id = 2;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is successful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 2);
    munit_assert_true(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

/* If the requester last log entry index is the lower than ours, the vote is not
 * granted. */
static MunitResult test_last_idx_lower_index(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_buffer buf;
    const struct raft_server *server;
    struct raft_request_vote_args args;
    struct test_io_request event;
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
    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.candidate_id = 2;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_handle_request_vote(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_REQUEST_VOTE_RESULT,
                            &event);
    munit_assert_int(event.request_vote_response.result.term, ==, 2);
    munit_assert_false(event.request_vote_response.result.vote_granted);

    return MUNIT_OK;
}

static MunitTest request_vote_tests[] = {
    {"/higher", test_refuse_vote_if_higher_term, setup, tear_down, 0, NULL},
    {"/has-leader", test_refuse_if_has_leader, setup, tear_down, 0, NULL},
    {"/empty-log", test_grant_if_empty_log, setup, tear_down, 0, NULL},
    {"/non-voting", test_refuse_if_non_voting, setup, tear_down, 0, NULL},
    {"/already-voted", test_refuse_if_already_voted, setup, tear_down, 0, NULL},
    {"/duplicate-vote", test_duplicate_vote, setup, tear_down, 0, NULL},
    {"/last-term-lower", test_last_term_lower, setup, tear_down, 0, NULL},
    {"/last-term-higher", test_last_term_higher, setup, tear_down, 0, NULL},
    {"/last-index-higher", test_last_idx_higher, setup, tear_down, 0, NULL},
    {"/last-index-same", test_last_idx_same_index, setup, tear_down, 0, NULL},
    {"/last-index-lower", test_last_idx_lower_index, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_handle_request_vote_response
 */

/* If a candidate receives a vote request response granting the vote and the
 * quorum is reached, it becomes leader. */
static MunitResult test_votes_quorum_reached(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    /* We are leader */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    /* The next_index array is initialized */
    munit_assert_ptr_not_null(f->raft.leader_state.next_index);
    munit_assert_int(f->raft.leader_state.next_index[0], ==, 2);
    munit_assert_int(f->raft.leader_state.next_index[1], ==, 2);

    /* The match_index array is initialized */
    munit_assert_ptr_not_null(f->raft.leader_state.match_index);
    munit_assert_int(f->raft.leader_state.match_index[0], ==, 0);
    munit_assert_int(f->raft.leader_state.match_index[1], ==, 0);

    /* We have sent heartbeats */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES, &event);
    munit_assert_int(event.append_entries.server.id, ==, 2);
    munit_assert_int(event.append_entries.args.term, ==, 2);
    munit_assert_int(event.append_entries.args.prev_log_index, ==, 1);
    munit_assert_int(event.append_entries.args.prev_log_term, ==, 1);
    munit_assert_ptr_null(event.append_entries.args.entries);
    munit_assert_int(event.append_entries.args.n, ==, 0);

    test_io_flush(&f->io);

    return MUNIT_OK;
}

/* If a candidate receives a vote request response granting the vote but the
 * quorum is not reached, it statys candidate. */
static MunitResult test_votes_quorum_not_reached(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 5, 1, 5);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    /* We are still candidate, since majority requires 3 votes, but we have
     * 2. */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_CANDIDATE);

    return MUNIT_OK;
}

/* If the server is not in candidate state the response gets discarded. */
static MunitResult test_not_candidate(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* If the server receives a response contaning an higher term than its own, it
   converts to follower. */
static MunitResult test_rv_response_step_down(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 3;
    result.vote_granted = 0;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

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
    munit_assert_ptr_null(f->raft.follower_state.current_leader);

    return MUNIT_OK;
}

/* If a candidate server receives a response indicating that the vote was not
 *  granted, nothing happens. */
static MunitResult test_vote_not_granted(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* The receiver does not grant the vote (e.g. it has already voted for
       someone else). */
    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 0;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    /* We are still candidate */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_CANDIDATE);

    return MUNIT_OK;
}

static MunitTest request_vote_response_tests[] = {
    {"/votes-quorum", test_votes_quorum_reached, setup, tear_down, 0, NULL},
    {"/no-quorum", test_votes_quorum_not_reached, setup, tear_down, 0, NULL},
    {"/not-candidate", test_not_candidate, setup, tear_down, 0, NULL},
    {"/higher-term", test_rv_response_step_down, setup, tear_down, 0, NULL},
    {"/not-granted", test_vote_not_granted, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_handle_append_entries
 */

/* If the term in the request is stale, the server rejects it. */
static MunitResult test_ae_stale_term(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_args args;
    struct raft_append_entries_result result;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate, this will bump our term. */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 0;
    args.prev_log_term = 0;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    /* Receive a valid AppendEntries RPC to update our leader. */
    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES_RESULT,
                            &event);

    result = event.append_entries_response.result;
    munit_assert_int(result.term, ==, 2);
    munit_assert_false(result.success);
    munit_assert_int(result.last_log_index, ==, 1);

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning an higher term as its
 * own, it it steps down to follower and accept the request . */
static MunitResult test_ae_higher_term(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_args args;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 3;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* We have stepped down to follower. */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    /* We have updated our leader. */
    munit_assert_int(f->raft.follower_state.current_leader->id, ==, server->id);

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning the same term as its
 * own, it it steps down to follower and accept the request . */
static MunitResult test_ae_candidate_step_down(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_args args;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* We have stepped down to follower. */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    /* We have updated our leader. */
    munit_assert_int(f->raft.follower_state.current_leader->id, ==, server->id);

    return MUNIT_OK;
}

/* If server's log is shorter than prevLogIndex, the request is rejected . */
static MunitResult test_missing_log_entries(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_args args;
    struct raft_append_entries_result result;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 2;
    args.prev_log_term = 1;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request is unsuccessful */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES_RESULT,
                            &event);

    result = event.append_entries_response.result;
    munit_assert_int(result.term, ==, 1);
    munit_assert_false(result.success);
    munit_assert_int(result.last_log_index, ==, 1);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is lower or equal than server's commit
 * index, then an error is returned . */
static MunitResult test_prev_index_conflict(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_args args;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 2;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, RAFT_ERR_SHUTDOWN);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is greater than server's commit commit
 * index (i.e. this is a normal inconsistency), we reject the request. */
static MunitResult test_prev_log_term_mismatch(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    struct raft_entry entries[2];
    const struct raft_server *server;
    struct raft_append_entries_args args;
    struct raft_append_entries_result result;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Append two uncommitted entries. */
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = 1;
    entries[0].buf.base = NULL;
    entries[0].buf.len = 0;

    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].term = 1;
    entries[1].buf.base = NULL;
    entries[1].buf.len = 0;

    test_io_write_entry(f->raft.io, &entries[0]);
    test_io_write_entry(f->raft.io, &entries[1]);

    memset(&buf, 0, sizeof buf);

    raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &buf, NULL);
    raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &buf, NULL);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 2;
    args.prev_log_term = 2;
    args.entries = NULL;
    args.n = 0;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* The request gets rejected. */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES_RESULT,
                            &event);

    result = event.append_entries_response.result;
    munit_assert_int(result.term, ==, 1);
    munit_assert_false(result.success);
    munit_assert_int(result.last_log_index, ==, 3);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. */
static MunitResult test_submit_write_log_io_request(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct raft_entry *entries = __create_entries_batch();
    const struct raft_server *server;
    struct raft_append_entries_args args;
    struct test_io_request request;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entries;
    args.n = 1;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* A write request has been submitted. */
    test_io_get_one_request(&f->io, RAFT_IO_WRITE_LOG, &request);

    munit_assert_int(request.write_log.n, ==, 1);
    munit_assert_int(request.write_log.entries[0].type, ==, RAFT_LOG_COMMAND);

    /* We saved the details about the pending write request. */
    munit_assert_int(f->raft.io_queue.requests[0].leader_id, ==, server->id);

    test_io_flush(f->raft.io);
    raft_handle_io(&f->raft, 0, 0);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
static MunitResult test_skip_entries_already_appended(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    const struct raft_server *leader;
    struct raft_append_entries_args args;
    struct test_io_request request;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    int rv;

    (void)params;

    munit_assert_ptr_not_null(buf1);
    munit_assert_ptr_not_null(buf2);

    *buf1 = 1;
    *buf2 = 2;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = 1;
    entries[0].buf.base = buf1;
    entries[0].buf.len = 1;

    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].term = 1;
    entries[1].buf.base = buf2;
    entries[1].buf.len = 1;

    /* Append the first entry to our log. */
    test_io_write_entry(f->raft.io, &entries[0]);
    rv = raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &entries[0].buf, NULL);
    munit_assert_int(rv, ==, 0);

    /* Handle an AppendEntries RPC containing both the entry that we already
     * have and an additional one. */
    leader = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = leader->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entries;
    args.n = 2;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, leader, &args);
    munit_assert_int(rv, ==, 0);

    /* A write request has been submitted, only for the second entry. */
    test_io_get_one_request(&f->io, RAFT_IO_WRITE_LOG, &request);

    munit_assert_int(request.write_log.n, ==, 1);
    munit_assert_int(request.write_log.entries[0].type, ==, RAFT_LOG_COMMAND);
    munit_assert_int(*(uint8_t *)request.write_log.entries[0].buf.base, ==, 2);

    /* We saved the details about the pending write request. */
    munit_assert_int(f->raft.io_queue.requests[0].leader_id, ==, leader->id);

    test_io_flush(f->raft.io);
    raft_handle_io(&f->raft, 0, 0);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log but they have a different term, they will be
 * replaced. */
static MunitResult test_truncate_local_log(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    const struct raft_server *leader;
    struct raft_append_entries_args args;
    struct test_io_request request;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    uint8_t *buf3 = raft_malloc(1);
    int rv;

    *buf1 = 1;
    *buf2 = 2;
    *buf3 = 3;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Append an additional entry to our log. */
    entry.type = RAFT_LOG_COMMAND;
    entry.term = 1;
    entry.buf.base = buf1;
    entry.buf.len = 1;

    test_io_write_entry(f->raft.io, &entry);
    rv = raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &entry.buf, NULL);
    munit_assert_int(rv, ==, 0);

    /* Include two new entries with a different term in the request */
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = 2;
    entries[0].buf.base = buf2;
    entries[0].buf.len = 1;
    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].term = 2;
    entries[1].buf.base = buf3;
    entries[1].buf.len = 1;

    leader = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.leader_id = leader->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entries;
    args.n = 2;
    args.leader_commit = 1;

    rv = raft_handle_append_entries(&f->raft, leader, &args);
    munit_assert_int(rv, ==, 0);

    /* A write request has been submitted, only for both the two new entries. */
    test_io_get_one_request(&f->io, RAFT_IO_WRITE_LOG, &request);

    munit_assert_int(request.write_log.n, ==, 2);
    munit_assert_int(*(uint8_t*)request.write_log.entries[0].buf.base, ==, 2);
    munit_assert_int(*(uint8_t*)request.write_log.entries[1].buf.base, ==, 3);

    /* We saved the details about the pending write request. */
    munit_assert_int(f->raft.io_queue.requests[0].leader_id, ==, leader->id);

    test_io_flush(f->raft.io);
    raft_handle_io(&f->raft, 0, 0);

    return MUNIT_OK;
}

/* If any of the new entry has the same index of an existing entry in our log,
 *  but different term, and that entry index is already committed, we bail out
 *  with an error. */
static MunitResult test_committed_index_conflict(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    const struct raft_server *server;
    struct raft_append_entries_args args;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    uint8_t *buf3 = raft_malloc(1);
    int rv;

    *buf1 = 1;
    *buf2 = 2;
    *buf3 = 3;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Append an additional entry to our log, with index 2 and term 1. */
    entry.type = RAFT_LOG_COMMAND;
    entry.term = 1;
    entry.buf.base = buf1;
    entry.buf.len = 1;

    test_io_write_entry(f->raft.io, &entry);
    rv = raft_log__append(&f->raft.log, 1, RAFT_LOG_COMMAND, &entry.buf, NULL);
    munit_assert_int(rv, ==, 0);

    /* Bump the commit index. */
    f->raft.commit_index = 2;

    /* Include two new entries with a different term in the request */
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = 2;
    entries[0].buf.base = &buf2;
    entries[0].buf.len = 1;
    entries[1].type = RAFT_LOG_COMMAND;
    entries[1].term = 2;
    entries[1].buf.base = &buf3;
    entries[1].buf.len = 1;

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 2;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entries;
    args.n = 2;
    args.leader_commit = 1;

    /* We return a shutdown error. */
    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, RAFT_ERR_SHUTDOWN);

    /* TODO: should the code itself perform this cleanup? */
    raft_free(buf2);
    raft_free(buf3);
    raft_free(entries);

    return MUNIT_OK;
}

static MunitTest append_entries_tests[] = {
    {"/stale-term", test_ae_stale_term, setup, tear_down, 0, NULL},
    {"/higher-term", test_ae_higher_term, setup, tear_down, 0, NULL},
    {"/same-term", test_ae_candidate_step_down, setup, tear_down, 0, NULL},
    {"/missing-entries", test_missing_log_entries, setup, tear_down, 0, NULL},
    {"/prev-conflict", test_prev_index_conflict, setup, tear_down, 0, NULL},
    {"/mismatch", test_prev_log_term_mismatch, setup, tear_down, 0, NULL},
    {"/write-log", test_submit_write_log_io_request, setup, tear_down, 0, NULL},
    {"/skip", test_skip_entries_already_appended, setup, tear_down, 0, NULL},
    {"/truncate", test_truncate_local_log, setup, tear_down, 0, NULL},
    {"/conflict", test_committed_index_conflict, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_handle_append_entries_response
 */

/* If the server handling the response is not the leader, the result
 * is ignored. */
static MunitResult test_not_leader(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_append_entries_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    result.term = 1;
    result.success = 1;
    result.last_log_index = 1;

    rv = raft_handle_append_entries_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
static MunitResult test_ae_response_ignore(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result request_vote_result;
    struct raft_append_entries_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    /* Become Leader */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    request_vote_result.term = 2;
    request_vote_result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server,
                                           &request_vote_result);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    /* Receive an append entries response with a stale term. */
    result.term = 1;
    result.success = 1;
    result.last_log_index = 2;

    rv = raft_handle_append_entries_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
static MunitResult test_ae_response_step_down(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result request_vote_result;
    struct raft_append_entries_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    /* Become Leader */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    request_vote_result.term = 2;
    request_vote_result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server,
                                           &request_vote_result);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    /* Receive an append entries response with a newer term. */
    result.term = 3;
    result.success = 0;
    result.last_log_index = 2;

    rv = raft_handle_append_entries_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the response fails because a log mismatch, the nextIndex for the server is
 * updated and the relevant older entries are resent. */
static MunitResult test_retry_upon_log_mismatch(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result request_vote_result;
    struct raft_append_entries_result result;
    struct test_io_request event;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 2);

    /* Become Leader */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    request_vote_result.term = 2;
    request_vote_result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server,
                                           &request_vote_result);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    test_io_flush(&f->io);

    /* Receive an unsuccessful append entries response reporting that the peer's
     * last log entry has index 0 (peer's log is empty. */
    result.term = 2;
    result.success = 0;
    result.last_log_index = 0;

    rv = raft_handle_append_entries_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    /* We have resent entry 1. */
    test_io_get_one_request(&f->io, RAFT_IO_APPEND_ENTRIES, &event);

    munit_assert_int(event.append_entries.args.n, ==, 1);

    test_io_flush(f->raft.io);

    return MUNIT_OK;
}

/* If a majority of servers has replicated an entry, commit it. */
static MunitResult test_commit_if_quorum_replicated(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct test_io_request request;
    struct raft_buffer buf;
    const struct raft_server *server;
    struct raft_request_vote_result request_vote_result;
    struct raft_append_entries_result result;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    server = raft_configuration__get(&f->raft.configuration, 2);

    /* Become Leader */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    server = raft_configuration__get(&f->raft.configuration, 2);
    request_vote_result.term = 2;
    request_vote_result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server,
                                           &request_vote_result);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    test_io_flush(&f->io);

    raft_handle_io(&f->raft, 0, 0);
    raft_handle_io(&f->raft, 1, 0);

    /* Append an entry to our log and handle the associated successful write. */
    buf.base = NULL;
    buf.len = 0;

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, 0);

    test_io_get_one_request(f->raft.io, RAFT_IO_WRITE_LOG, &request);
    test_io_flush(f->raft.io);

    raft_handle_io(&f->raft, 0, 0);
    raft_handle_io(&f->raft, 1, 0);
    raft_handle_io(&f->raft, 2, 0);

    /* Receive a successful append entries response reporting that the peer
     * has replicated that entry. */
    result.term = 2;
    result.success = 1;
    result.last_log_index = 2;

    rv = raft_handle_append_entries_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 2);

    return MUNIT_OK;
}

static MunitTest append_entries_response_tests[] = {
    {"/not-leader", test_not_leader, setup, tear_down, 0, NULL},
    {"/ignore", test_ae_response_ignore, setup, tear_down, 0, NULL},
    {"/step-down", test_ae_response_step_down, setup, tear_down, 0, NULL},
    {"/retry", test_retry_upon_log_mismatch, setup, tear_down, 0, NULL},
    {"/commit", test_commit_if_quorum_replicated, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_rpc_suites[] = {
    {"/request-vote", request_vote_tests, NULL, 1, 0},
    {"/request-vote-response", request_vote_response_tests, NULL, 1, 0},
    {"/append-entries", append_entries_tests, NULL, 1, 0},
    {"/append_entries_response", append_entries_response_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
