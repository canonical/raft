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

static int __rand()
{
    return munit_rand_uint32();
}

/**
 * Submit a request to append a new RAFT_LOG_COMMAND entry.
 */
#define __accept_entry(F)                    \
    {                                        \
        struct raft_buffer buf;              \
        int rv;                              \
                                             \
        test_fsm_encode_set_x(123, &buf);    \
                                             \
        rv = raft_accept(&F->raft, &buf, 1); \
        munit_assert_int(rv, ==, 0);         \
    }

/**
 * Submit a request to add a new server and check that it returns no error.
 */
#define __add_server(F, ID, ADDRESS)                 \
    {                                                \
        int rv;                                      \
                                                     \
        rv = raft_add_server(&F->raft, ID, ADDRESS); \
        munit_assert_int(rv, ==, 0);                 \
    }

/**
 * Submit a request to promote a server and check that it returns no error.
 */
#define __promote(F, ID)                 \
    {                                    \
        int rv;                          \
                                         \
        rv = raft_promote(&F->raft, ID); \
        munit_assert_int(rv, ==, 0);     \
    }

/**
 * Submit a request to remove a server and check that it returns no error.
 */
#define __remove_server(F, ID)                 \
    {                                          \
        int rv;                                \
                                               \
        rv = raft_remove_server(&F->raft, ID); \
        munit_assert_int(rv, ==, 0);           \
    }

/**
 * Call raft_handle_append_entries with the given parameters and check that no
 * error occurs.
 */
#define __handle_append_entries(F, TERM, LEADER_ID, PREV_LOG_INDEX,           \
                                PREV_LOG_TERM, ENTRIES, N, COMMIT)            \
    {                                                                         \
        struct raft_append_entries_args args;                                 \
        char address[4];                                                      \
        int rv;                                                               \
                                                                              \
        sprintf(address, "%d", LEADER_ID);                                    \
                                                                              \
        args.term = TERM;                                                     \
        args.leader_id = LEADER_ID;                                           \
        args.prev_log_index = PREV_LOG_INDEX;                                 \
        args.prev_log_term = PREV_LOG_TERM;                                   \
        args.entries = ENTRIES;                                               \
        args.n = N;                                                           \
        args.leader_commit = COMMIT;                                          \
                                                                              \
        rv = raft_handle_append_entries(&F->raft, LEADER_ID, address, &args); \
        munit_assert_int(rv, ==, 0);                                          \
    }

/**
 * Call raft_handle_append_entries_response with the given parameters and check
 * that no error occurs.
 */
#define __handle_append_entries_response(F, SERVER_ID, TERM, SUCCESS,          \
                                         LAST_LOG_INDEX)                       \
    {                                                                          \
        char address[4];                                                       \
        struct raft_append_entries_result result;                              \
        int rv;                                                                \
                                                                               \
        sprintf(address, "%d", SERVER_ID);                                     \
                                                                               \
        result.term = TERM;                                                    \
        result.success = SUCCESS;                                              \
        result.last_log_index = LAST_LOG_INDEX;                                \
                                                                               \
        rv = raft_handle_append_entries_response(&F->raft, SERVER_ID, address, \
                                                 &result);                     \
        munit_assert_int(rv, ==, 0);                                           \
    }

/**
 * Complete any outstanding RAFT_IO_WRITE_LOG or RAFT_IO_APPEND_ENTRIES
 * requests, asserting that they match the given numbers, then flush the test
 * I/O queue and notify the raft instance about the I/O having completed.
 */
#define __assert_io(F, N_WRITE_LOG, N_APPEND_ENTRIES)                        \
    {                                                                        \
        struct test_io_request *write_log_requests;                          \
        struct test_io_request *append_entries_requests;                     \
        size_t n_write_log;                                                  \
        size_t n_append_entries;                                             \
        size_t i;                                                            \
        int rv;                                                              \
                                                                             \
        test_io_get_requests(&F->io, RAFT_IO_WRITE_LOG, &write_log_requests, \
                             &n_write_log);                                  \
                                                                             \
        munit_assert_int(n_write_log, ==, N_WRITE_LOG);                      \
                                                                             \
        test_io_get_requests(&F->io, RAFT_IO_APPEND_ENTRIES,                 \
                             &append_entries_requests, &n_append_entries);   \
        munit_assert_int(n_append_entries, ==, N_APPEND_ENTRIES);            \
                                                                             \
        test_io_flush(&F->io);                                               \
                                                                             \
        for (i = 0; i < n_write_log; i++) {                                  \
            rv = raft_handle_io(&F->raft, write_log_requests[i].id, 0);      \
            munit_assert_int(rv, ==, 0);                                     \
        }                                                                    \
                                                                             \
        for (i = 0; i < n_append_entries; i++) {                             \
            rv = raft_handle_io(&F->raft, append_entries_requests[i].id, 0); \
            munit_assert_int(rv, ==, 0);                                     \
        }                                                                    \
                                                                             \
        free(write_log_requests);                                            \
        free(append_entries_requests);                                       \
    }

/**
 * Assert that the state of the current catch up round matches the given values.
 */
#define __assert_catch_up_round(F, PROMOTEED_ID, NUMBER, DURATION)            \
    {                                                                         \
        munit_assert_int(f->raft.leader_state.promotee_id, ==, PROMOTEED_ID); \
        munit_assert_int(F->raft.leader_state.round_number, ==, NUMBER);      \
        munit_assert_int(F->raft.leader_state.round_duration, ==, DURATION);  \
    }

/**
 * Assert the values of the committed and uncommitted configuration indexes.
 */
#define __assert_configuration_indexes(F, COMMITTED, UNCOMMITTED)     \
    {                                                                 \
        munit_assert_int(F->raft.configuration_index, ==, COMMITTED); \
        munit_assert_int(F->raft.configuration_uncommitted_index, ==, \
                         UNCOMMITTED);                                \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * Invoke @raft_tick and check that no errors occur.
 */
#define __tick(F, MSECS)                 \
    {                                    \
        int rv;                          \
                                         \
        rv = raft_tick(&F->raft, MSECS); \
        munit_assert_int(rv, ==, 0);     \
    }

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    const char *address = "1";

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_io_setup(params, &f->io);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id, address);

    raft_set_logger(&f->raft, &f->logger);
    raft_set_rand(&f->raft, __rand);

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
 * raft_accept
 */

/* If the raft instance is not in leader state, an error is returned. */
static MunitResult test_accept_not_leader(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    test_fsm_encode_set_x(123, &buf);

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);
    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "can't accept entries: server is not the leader");

    raft_free(buf.base);

    return MUNIT_OK;
}

static char *accept_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *accept_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum accept_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, accept_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, accept_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
static MunitResult test_accept_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    test_fsm_encode_set_x(123, &buf);

    test_heap_fault_enable(&f->heap);

    /* Reset the request queue, to trigger a failure when attempting to grow
     * it. */
    raft_free(f->raft.io_queue.requests);
    f->raft.io_queue.requests = NULL;
    f->raft.io_queue.size = 0;

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* I/O error. */
static MunitResult test_accept_io_err(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    test_fsm_encode_set_x(123, &buf);

    test_io_fault(&f->io, 0, 1);

    rv = raft_accept(&f->raft, &buf, 1);
    munit_assert_int(rv, ==, RAFT_ERR_SHUTDOWN);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* The new entries are sent to all other servers. */
static MunitResult test_accept_send_entries(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    __accept_entry(f);

    /* A write log and an append entries requests has been submitted. */
    __assert_io(f, 1, 1);

    return MUNIT_OK;
}

static MunitTest accept_tests[] = {
    {"/not-leader", test_accept_not_leader, setup, tear_down, 0, NULL},
    {"/oom", test_accept_oom, setup, tear_down, 0, accept_oom_params},
    {"/io-err", test_accept_io_err, setup, tear_down, 0, NULL},
    {"/send-entries", test_accept_send_entries, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_add_server
 */

/* Trying to add a server on a node which is not the leader results in an
 * error. */
static MunitResult test_add_server_not_leader(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    rv = raft_add_server(&f->raft, 3, "3");
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "server is not the leader");

    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
static MunitResult test_add_server_busy(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    __add_server(f, 3, "3");

    rv = raft_add_server(&f->raft, 4, "4");
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "a configuration change is already in progress");

    return MUNIT_OK;
}

/* Trying to add a server with an ID which is already in use results in an
 * error. */
static MunitResult test_add_server_dup_id(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_add_server(&f->raft, 1, "3");
    munit_assert_int(rv, ==, RAFT_ERR_DUP_SERVER_ID);

    munit_assert_string_equal(
        raft_errmsg(&f->raft),
        "add server to new configuration: server ID already in use");

    return MUNIT_OK;
}

/* Submit a request to add a new non-voting server. */
static MunitResult test_add_server_submit(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    __add_server(f, 3, "3");

    /* A write log and 2 append entries requests (one for each follower) have
     * been submitted. */
    __assert_io(f, 1, 2);

    return MUNIT_OK;
}

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
static MunitResult test_add_server_committed(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    __add_server(f, 3, "3");

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 3);
    server = &f->raft.configuration.servers[2];
    munit_assert_int(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_false(server->voting);

    /* The new configuration is marked as uncommitted. */
    __assert_configuration_indexes(f, 1, 2);

    /* The next/match indexes now include an entry for the new server. */
    munit_assert_int(f->raft.leader_state.next_index[2], ==, 3);
    munit_assert_int(f->raft.leader_state.match_index[2], ==, 0);

    __assert_io(f, 1, 2);

    /* Receive a successful append entries response reporting that one peer
     * has replicated the configuration entry. */
    __handle_append_entries_response(f, 2, 2, true, 2);

    /* The new configuration is marked as committed. */
    __assert_configuration_indexes(f, 2, 0);

    return MUNIT_OK;
}

static MunitTest add_server_tests[] = {
    {"/not-leader", test_add_server_not_leader, setup, tear_down, 0, NULL},
    {"/busy", test_add_server_busy, setup, tear_down, 0, NULL},
    {"/dup-id", test_add_server_dup_id, setup, tear_down, 0, NULL},
    {"/submit", test_add_server_submit, setup, tear_down, 0, NULL},
    {"/committed", test_add_server_committed, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_promote
 */

/* Trying to promote a server on a node which is not the leader results in an
 * error. */
static MunitResult test_promote_not_leader(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);

    rv = raft_promote(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "server is not the leader");

    return MUNIT_OK;
}

/* Trying to promote a server whose ID is unknown results in an
 * error. */
static MunitResult test_promote_bad_id(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_promote(&f->raft, 4);
    munit_assert_int(rv, ==, RAFT_ERR_BAD_SERVER_ID);

    munit_assert_string_equal(raft_errmsg(&f->raft), "server ID is not valid");

    return MUNIT_OK;
}

/* Promoting a server which is already a voting server is a no-op. */
static MunitResult test_promote_already_voting(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_promote(&f->raft, 2);
    munit_assert_int(rv, ==, RAFT_ERR_SERVER_ALREADY_VOTING);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "server is already voting");

    return MUNIT_OK;
}

/* Trying to promote a server while another server is being promoted results in
 * an error. */
static MunitResult test_promote_in_progress(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 4, 1, 2);
    test_become_leader(&f->raft);

    __promote(f, 3);

    rv = raft_promote(&f->raft, 4);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "a configuration change "
                              "is already in progress");

    return MUNIT_OK;
}

/* Promoting a server whose log is already up-to-date results in the relevant
 * configuration change to be submitted immediately. */
static MunitResult test_promote_up_to_date(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Advance the match index of server 3. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    __promote(f, 3);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    server = raft_configuration__get(&f->raft.configuration, 3);
    munit_assert_true(server->voting);

    /* A configuration change request has been submitted. */
    __assert_io(f, 1, 2);

    return MUNIT_OK;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. When the server has caught up, the configuration change request gets
 * submitted. */
static MunitResult test_promote_catch_up(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    __promote(f, 3);

    /* Advance the match index of server 3, by acknowledging the AppendEntries
     * request that the leader has sent to it. */
    __assert_io(f, 0, 1);
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* A configuration change request has been submitted. Let's complete the
     * associated I/O requests. */
    __assert_io(f, 1, 2);

    /* At this point the server has caught up, but the configuration is
       uncommitted. */
    __assert_catch_up_round(f, 0, 0, 0);
    munit_assert_int(f->raft.configuration_uncommitted_index, ==, 2);
    server = raft_configuration__get(&f->raft.configuration, 3);
    munit_assert_true(server->voting);

    /* Simulate the server being promoted notifying that it has appended the new
     * configuration. Since it's considered voting already, it counts for the
     * majority and the entry gets committed. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 2);

    /* The promotion is completed. */
    __assert_catch_up_round(f, 0, 0, 0);
    munit_assert_int(f->raft.configuration_uncommitted_index, ==, 0);

    return MUNIT_OK;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. If new entries are appended after a round is started, a new round is
 * initiated once the former one completes. */
static MunitResult test_promote_new_round(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;
    return 0;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    __promote(f, 3);

    /* Now that the catch-up round started, submit a new entry. */
    __accept_entry(f);

    /* Let more than election_timeout milliseconds elapse. */
    __tick(f, f->raft.election_timeout + 100);

    __assert_catch_up_round(f, 3, 1, f->raft.election_timeout + 100);

    /* Simulate the server being promoted sending an AppendEntries result,
     * acknowledging all entries except the last one. */
    __assert_io(f, 1, 2);
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* The first round has completed and a new one has started. */
    __assert_catch_up_round(f, 3, 2, 0);

    return MUNIT_OK;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. Once a round takes less than election_timeout, a request to append the
 * new configuration is made and eventually committed . */
static MunitResult test_promote_committed(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    const struct raft_entry *entry;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Request to promote server 3 to voting. */
    __promote(f, 3);
    __assert_io(f, 0, 1);

    /* Now that the catch-up round started, submit a new entry. */
    __accept_entry(f);
    __assert_io(f, 1, 2);

    /* Let more than election_timeout milliseconds elapse. */
    __tick(f, f->raft.election_timeout + 100);
    __assert_io(f, 0, 2); /* Heartbeat */

    /* Simulate the server being promoted sending an AppendEntries result,
     * acknowledging all entries except the last one. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* The first round has completed and a new one has started. */
    __assert_catch_up_round(f, 3, 2, 0);

    /* Make a new client request, so even when this second round that just
     * started completes, the server being promoted will still be missing
     * entries. */
    __accept_entry(f);
    __assert_io(f, 1, 2);

    /* Simulate the server being promoted completing the second round within the
     * election timeout. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 3);

    /* No round is pending, but the promotion is still in progress. */
    __assert_catch_up_round(f, 0, 0, 0);
    __assert_configuration_indexes(f, 1, 4);

    /* Notify the leader that the AppendEntries RPC for changing the
     * configuration has been appended by a majority of the cluster. */
    __assert_io(f, 1, 2);
    __handle_append_entries_response(f, 2, f->raft.current_term, true, 4);

    /* The promotion has been completed. */
    __assert_catch_up_round(f, 0, 0, 0);
    __assert_configuration_indexes(f, 4, 0);

    entry = raft_log__get(&f->raft.log, 4);
    munit_assert_int(entry->type, ==, RAFT_LOG_CONFIGURATION);

    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for promoting
 * the new server is committed, the leader configuration gets rolled back and
 * the server being promoted is not considered any more as voting. */
static MunitResult test_promote_step_down(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Advance the match index of server 3. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* Ask to promote server 3, which is already up-to-date (so the
     * configuration change request is submitted immediately. */
    __promote(f, 3);

    __assert_catch_up_round(f, 0, 0, 0);
    __assert_configuration_indexes(f, 1, 2);

    server = raft_configuration__get(&f->raft.configuration, 3);
    munit_assert_true(server->voting);

    __assert_io(f, 1, 2);

    /* Receive an AppendEntries RPC from a leader with a higher term, forcing
     * this leader to step down and to discard the uncommitted configuration
     * change log entry. */
    server = raft_configuration__get(&f->raft.configuration, 2);

    entries = raft_calloc(1, sizeof *entries);
    munit_assert_ptr_not_null(entries);

    entries[0].batch = raft_malloc(1);
    munit_assert_ptr_not_null(entries[0].batch);

    entries[0].buf.base = entries[0].batch;
    entries[0].buf.len = 1;
    entries[0].type = RAFT_LOG_COMMAND;
    entries[0].term = f->raft.current_term + 1;

    __handle_append_entries(f, 3, 2, 1, 1, entries, 1, 1);

    munit_assert_int(f->raft.state, ==, RAFT_STATE_FOLLOWER);

    /* Server 3 is not being considered voting anymore. */
    server = raft_configuration__get(&f->raft.configuration, 3);
    munit_assert_false(server->voting);

    return MUNIT_OK;
}

/* If a follower receives an AppendEntries RPC containing a
 * RAFT_LOG_CONFIGURATION entry which promotes a non-voting server, the
 * configuration change is immediately applied locally, even if the entry is not
 * yet committed. Once the entry is committed, the change becomes permanent.*/
static MunitResult test_promote_follower(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);

    /* Encode the new configuration into a buffer. */
    raft_configuration_init(&configuration);

    rv = raft_configuration__copy(&f->raft.configuration, &configuration);
    munit_assert_int(rv, ==, 0);

    configuration.servers[2].voting = true;

    rv = raft_encode_configuration(&configuration, &buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&configuration);

    entries = raft_calloc(1, sizeof *entries);
    munit_assert_ptr_not_null(entries);

    entries[0].batch = buf.base;

    entries[0].buf = buf;
    entries[0].type = RAFT_LOG_CONFIGURATION;
    entries[0].term = f->raft.current_term;

    __handle_append_entries(f, 1, 2, 1, 1, entries, 1, 1);
    __assert_io(f, 1, 0);

    /* The server being promoted is considered as voting. */
    __assert_configuration_indexes(f, 1, 2);
    munit_assert_true(f->raft.configuration.servers[2].voting);

    /* Receive an empty AppendEntries RPC advancing the commit index. */
    __handle_append_entries(f, 1, 2, 2, 1, NULL, 0, 2);

    /* The change is now persistent. */
    munit_assert_int(f->raft.configuration_index, ==, 2);
    munit_assert_int(f->raft.configuration_uncommitted_index, ==, 0);
    munit_assert_true(f->raft.configuration.servers[2].voting);

    return MUNIT_OK;
}

static MunitTest promote_tests[] = {
    {"/not-leader", test_promote_not_leader, setup, tear_down, 0, NULL},
    {"/bad-id", test_promote_bad_id, setup, tear_down, 0, NULL},
    {"/already-voting", test_promote_already_voting, setup, tear_down, 0, NULL},
    {"/in-progress", test_promote_in_progress, setup, tear_down, 0, NULL},
    {"/up-to-date", test_promote_up_to_date, setup, tear_down, 0, NULL},
    {"/catch-up", test_promote_catch_up, setup, tear_down, 0, NULL},
    {"/new-round", test_promote_new_round, setup, tear_down, 0, NULL},
    {"/committed", test_promote_committed, setup, tear_down, 0, NULL},
    {"/step-down", test_promote_step_down, setup, tear_down, 0, NULL},
    {"/follower", test_promote_follower, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_add_server
 */

/* Trying to remove a server on a node which is not the leader results in an
 * error. */
static MunitResult test_remove_server_not_leader(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);

    rv = raft_remove_server(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "server is not the leader");

    return MUNIT_OK;
}

/* Trying to remove a server while a configuration change is already in progress
 * results in an error. */
static MunitResult test_remove_server_busy(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    __add_server(f, 3, "3");

    rv = raft_remove_server(&f->raft, 2);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "a configuration change is already in progress");

    return MUNIT_OK;
}

/* Trying to remove a server with an unknwon ID results in an error. */
static MunitResult test_remove_server_bad_id(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_remove_server(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_ERR_BAD_SERVER_ID);

    munit_assert_string_equal(raft_errmsg(&f->raft), "server ID is not valid");

    return MUNIT_OK;
}

/* Submit a request to remove a server. */
static MunitResult test_remove_server_submit(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    __remove_server(f, 3);

    /* A write log and 1 append entriy request (for the remaining follower) have
     * been submitted. */
    __assert_io(f, 1, 1);

    return MUNIT_OK;
}

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
static MunitResult test_remove_server_committed(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    __remove_server(f, 3);

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 2);
    server = raft_configuration__get(&f->raft.configuration, 3);
    munit_assert_ptr_null(server);

    /* The new configuration is marked as uncommitted. */
    __assert_configuration_indexes(f, 1, 2);

    __assert_io(f, 1, 1);

    /* Receive a successful append entries response reporting that the remaining
     * follower has replicated the configuration entry. */
    __handle_append_entries_response(f, 2, 2, true, 2);

    /* The new configuration is marked as committed. */
    __assert_configuration_indexes(f, 2, 0);

    return MUNIT_OK;
}

/* A leader gets a request to remove itself. */
static MunitResult test_remove_server_self(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_load(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    __remove_server(f, 1);

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 2);
    server = raft_configuration__get(&f->raft.configuration, 1);
    munit_assert_ptr_null(server);

    /* The new configuration is marked as uncommitted. */
    __assert_configuration_indexes(f, 1, 2);

    __assert_io(f, 1, 2);

    /* Receive a successful append entries response reporting that one of the
     * two followers has replicated the configuration entry. */
    __handle_append_entries_response(f, 2, 2, true, 2);

    /* The new configuration is not yet marked as committed, since a majority
     * was not reached (the leader itself doesn't count. */
    __assert_configuration_indexes(f, 1, 2);

    /* Receive a successful append entries response reporting that the other
     * follower has replicated the configuration entry as well. */
    __handle_append_entries_response(f, 3, 2, true, 2);

    /* The new configuration is marked as committed. */
    __assert_configuration_indexes(f, 2, 0);

    /* We have stepped down. */
    __assert_state(f, RAFT_STATE_FOLLOWER);

    return MUNIT_OK;
}

static MunitTest remove_server_tests[] = {
    {"/not-leader", test_remove_server_not_leader, setup, tear_down, 0, NULL},
    {"/busy", test_remove_server_busy, setup, tear_down, 0, NULL},
    {"/bad-id", test_remove_server_bad_id, setup, tear_down, 0, NULL},
    {"/submit", test_remove_server_submit, setup, tear_down, 0, NULL},
    {"/committed", test_remove_server_committed, setup, tear_down, 0, NULL},
    {"/self", test_remove_server_self, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_client_suites[] = {
    {"/accept", accept_tests, NULL, 1, 0},
    {"/add-server", add_server_tests, NULL, 1, 0},
    {"/promote", promote_tests, NULL, 1, 0},
    {"/remove-server", remove_server_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
