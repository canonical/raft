#include "../../include/raft.h"

#include "../../src/configuration.h"
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
 * raft_tick
 */

/* Internal timers are updated according to the given time delta. */
static MunitResult test_updates_timers(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.timer, ==, 100);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.timer, ==, 200);

    return MUNIT_OK;
}

/* If there's only a single voting server and that's us, switch to leader
   state. */
static MunitResult test_self_elect(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_LEADER);

    return MUNIT_OK;
}

/* If there's only a single voting, but it's not us, stay follower. */
static MunitResult test_one_voter_not_us(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 2, 2);

    rv = raft_tick(&f->raft, f->raft.election_timeout_rand - 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due to an OOM error. */
static MunitResult test_to_candidate_oom(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    munit_assert_string_equal(f->raft.ctx.errmsg,
                              "failed to convert to candidate");

    return MUNIT_OK;
}

/* There's only a single voting server and that's us, but we fail to convert to
   candidate due the disk being full. */
static MunitResult test_to_candidate_io_err(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 1, 1, 1);

    test_io_fault(&f->io, 0, 1);

    rv = raft_tick(&f->raft, 100);
    munit_assert_int(rv, ==, RAFT_ERR_NO_SPACE);

    munit_assert_string_equal(f->raft.ctx.errmsg,
                              "failed to convert to candidate");

    return MUNIT_OK;
}

/* If the election timeout expires, the follower is a voting server, and it
 * hasn't voted yet in this term, then become candidate and start a new
 * election. */
static MunitResult test_to_candidate(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int election_timeout;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    election_timeout = f->raft.election_timeout_rand;

    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 2);
    munit_assert_int(test_io_get_term(&f->io), ==, 2);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(test_io_get_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are candidate */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    /* We have sent vote requests */
    test_io_get_requests(&f->io, RAFT_IO_REQUEST_VOTE, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 1);
    munit_assert_int(events[0].request_vote.server.id, ==, 2);
    munit_assert_int(events[0].request_vote.args.term, ==, 2);
    munit_assert_int(events[0].request_vote.args.last_log_index, ==, 1);
    munit_assert_int(events[0].request_vote.args.last_log_term, ==, 1);

    free(events);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay follower. */
static MunitResult test_follower_election_timer_not_expired(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    rv = raft_tick(&f->raft, f->raft.election_timeout_rand - 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If the election timeout has elapsed, but we're not voters, stay follower. */
static MunitResult test_follower_not_voter(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 2, 3);

    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has elapsed, send empty
 * AppendEntries RPCs. */
static MunitResult test_heartbeat_timeout_elapsed(const MunitParameter params[],
                                                  void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* Win election */
    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    /* Expire the heartbeat timeout */
    rv = raft_tick(&f->raft, f->raft.heartbeat_timeout + 100);
    munit_assert_int(rv, ==, 0);

    /* We have sent heartbeats */
    test_io_get_requests(&f->io, RAFT_IO_APPEND_ENTRIES, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 1);
    munit_assert_int(events[0].append_entries.server.id, ==, 2);
    munit_assert_int(events[0].append_entries.args.term, ==, 2);
    munit_assert_int(events[0].append_entries.args.prev_log_index, ==, 1);
    munit_assert_int(events[0].append_entries.args.prev_log_term, ==, 1);
    munit_assert_ptr_null(events[0].append_entries.args.entries);
    munit_assert_int(events[0].append_entries.args.n, ==, 0);

    free(events);

    test_io_flush(&f->io);

    return MUNIT_OK;
}

/* If we're leader and the heartbeat timeout has not elapsed, do nothing. */
static MunitResult test_heartbeat_timeout_not_elapsed(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;
    struct raft_request_vote_result result;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* Win election */
    server = raft_configuration__get(&f->raft.configuration, 2);
    result.term = 2;
    result.vote_granted = 1;

    rv = raft_handle_request_vote_response(&f->raft, server, &result);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    /* Advance timer but not enough to expire the heartbeat timeout */
    rv = raft_tick(&f->raft, f->raft.heartbeat_timeout - 100);
    munit_assert_int(rv, ==, 0);

    /* We have sent no heartbeats */
    test_io_get_requests(&f->io, RAFT_IO_APPEND_ENTRIES, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 0);

    free(events);

    return MUNIT_OK;
}

/* If we're candidate and the election timeout has elapsed, start a new
 * election. */
static MunitResult test_candidate_new_election(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    int election_timeout;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    election_timeout = f->raft.election_timeout_rand;

    /* Expire the election timeout */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* The term has been incremeted and saved to stable store. */
    munit_assert_int(f->raft.current_term, ==, 3);
    munit_assert_int(test_io_get_term(&f->io), ==, 3);

    /* We have voted for ouselves. */
    munit_assert_int(f->raft.voted_for, ==, 1);
    munit_assert_int(test_io_get_vote(&f->io), ==, 1);

    /* The election timeout has been reset. */
    munit_assert_int(f->raft.election_timeout_rand, !=, election_timeout);

    /* We are still candidate */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_CANDIDATE);

    /* The votes array is initialized */
    munit_assert_ptr_not_null(f->raft.candidate_state.votes);
    munit_assert_true(f->raft.candidate_state.votes[0]);
    munit_assert_false(f->raft.candidate_state.votes[1]);

    /* We have sent vote requests again */
    test_io_get_requests(&f->io, RAFT_IO_REQUEST_VOTE, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 1);
    munit_assert_int(events[0].request_vote.server.id, ==, 2);
    munit_assert_int(events[0].request_vote.args.term, ==, 3);
    munit_assert_int(events[0].request_vote.args.last_log_index, ==, 1);
    munit_assert_int(events[0].request_vote.args.last_log_term, ==, 1);

    free(events);

    return MUNIT_OK;
}

/* If the election timeout has not elapsed, stay candidate. */
static MunitResult test_candidate_election_timer_not_expired(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    test_io_flush(f->raft.io);

    rv = raft_tick(&f->raft, f->raft.election_timeout_rand - 100);
    munit_assert_int(rv, ==, 0);

    /* We are still candidate */
    munit_assert_int(f->raft.state, ==, RAFT_STATE_CANDIDATE);

    /* No new vote request has been sent */
    test_io_get_requests(&f->io, RAFT_IO_REQUEST_VOTE, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 0);

    free(events);

    return MUNIT_OK;
}

/* Vote requests are sent only to voting servers. */
static MunitResult test_request_vote_only_to_voters(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    struct test_io_request *events;
    size_t n_events;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);

    /* Become candidate */
    rv = raft_tick(&f->raft, f->raft.election_timeout_rand + 100);
    munit_assert_int(rv, ==, 0);

    /* We have sent vote requests only to the voting server */
    test_io_get_requests(&f->io, RAFT_IO_REQUEST_VOTE, &events,
                       &n_events);
    munit_assert_int(n_events, ==, 1);
    munit_assert_int(events[0].request_vote.server.id, ==, 2);
    munit_assert_int(events[0].request_vote.args.term, ==, 2);
    munit_assert_int(events[0].request_vote.args.last_log_index, ==, 1);
    munit_assert_int(events[0].request_vote.args.last_log_term, ==, 1);

    free(events);

    return MUNIT_OK;
}

static MunitTest tick_tests[] = {
    {"/updates-timers", test_updates_timers, setup, tear_down, 0, NULL},
    {"/self-elect", test_self_elect, setup, tear_down, 0, NULL},
    {"/one-voter-not-us", test_one_voter_not_us, setup, tear_down, 0, NULL},
    {"/to-candidate-oom", test_to_candidate_oom, setup, tear_down, 0, NULL},
    {"/to-candidate-io-err", test_to_candidate_io_err, setup, tear_down, 0,
     NULL},
    {"/to-candidate", test_to_candidate, setup, tear_down, 0, NULL},
    {"/follower-election-timer-not-expired",
     test_follower_election_timer_not_expired, setup, tear_down, 0, NULL},
    {"/follower_not-voter", test_follower_not_voter, setup, tear_down, 0, NULL},
    {"/leader-heartbeat-timeout-elapsed", test_heartbeat_timeout_elapsed, setup,
     tear_down, 0, NULL},
    {"/leader-heartbeat-timeout-not-elapsed",
     test_heartbeat_timeout_not_elapsed, setup, tear_down, 0, NULL},
    {"/candidate-new-election", test_candidate_new_election, setup, tear_down,
     0, NULL},
    {"/candidate_election-timer-not-expired",
     test_candidate_election_timer_not_expired, setup, tear_down, 0, NULL},
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
