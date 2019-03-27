#include <stdio.h>

#include "../../include/raft.h"
#include "../../include/raft/io_stub.h"

#include "../../src/configuration.h"
#include "../../src/log.h"
#include "../../src/tick.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(client);

/**
 * Submit a request to append a new RAFT_COMMAND entry.
 */
#define propose_entry                                       \
    {                                                       \
        struct raft_buffer buf;                             \
        struct raft_apply *req = munit_malloc(sizeof *req); \
        int rv;                                             \
                                                            \
        test_fsm_encode_set_x(123, &buf);                   \
                                                            \
        req->data = f;                                      \
        rv = raft_apply(&f->raft, req, &buf, 1, apply_cb);  \
        munit_assert_int(rv, ==, 0);                        \
    }

/**
 * Submit a request to add a new server and check that it returns no error.
 */
#define add_server(ID, ADDRESS)                      \
    {                                                \
        int rv;                                      \
                                                     \
        rv = raft_add_server(&f->raft, ID, ADDRESS); \
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
#define __handle_append_entries(F, TERM, LEADER_ID, PREV_LOG_INDEX,      \
                                PREV_LOG_TERM, ENTRIES, N, COMMIT)       \
    {                                                                    \
        struct raft_message message;                              \
        struct raft_append_entries *args;                         \
        char address[4];                                          \
                                                                  \
        sprintf(address, "%d", LEADER_ID);                        \
        message.type = RAFT_IO_APPEND_ENTRIES;                    \
        message.server_id = LEADER_ID;                            \
        message.server_address = address;                         \
                                                                  \
        args = &message.append_entries;                           \
        args->term = TERM;                                        \
        args->leader_id = LEADER_ID;                              \
        args->prev_log_index = PREV_LOG_INDEX;                    \
        args->prev_log_term = PREV_LOG_TERM;                      \
        args->entries = ENTRIES;                                  \
        args->n_entries = N;                                      \
        args->leader_commit = COMMIT;                             \
                                                                  \
        raft_io_stub_deliver(&F->io, &message);                   \
    }

/**
 * Call recv__append_entries_result with the given parameters and check
 * that no error occurs.
 */
#define __handle_append_entries_response(F, SERVER_ID, TERM, SUCCESS, \
                                         LAST_LOG_INDEX)              \
    {                                                                 \
        char address[4];                                              \
        struct raft_message message;                                  \
        struct raft_append_entries_result *result;                    \
                                                                      \
        sprintf(address, "%d", SERVER_ID);                            \
        message.type = RAFT_IO_APPEND_ENTRIES_RESULT;                 \
        message.server_id = SERVER_ID;                                \
        message.server_address = address;                             \
                                                                      \
        result = &message.append_entries_result;                      \
                                                                      \
        result->term = TERM;                                          \
        result->success = SUCCESS;                                    \
        result->last_log_index = LAST_LOG_INDEX;                      \
        raft_io_stub_deliver(&F->io, &message);                       \
    }

/**
 * Complete any outstanding I/O operation requests, asserting that they match
 * the given numbers.
 */
#define __assert_io(F, N_WRITE_LOG, N_APPEND_ENTRIES)                        \
    {                                                                        \
        unsigned n;                                                          \
        unsigned n_append_entries = 0;                                       \
        unsigned i;                                                          \
                                                                             \
        n = raft_io_stub_n_sending(&F->io);                                  \
        for (i = 0; i < n; i++) {                                            \
            struct raft_message *message;                                    \
            raft_io_stub_sending(&F->io, i, &message);                       \
            if (message->type == RAFT_IO_APPEND_ENTRIES) {                   \
                n_append_entries++;                                          \
            }                                                                \
        }                                                                    \
        munit_assert_int(n_append_entries, ==, N_APPEND_ENTRIES);            \
        munit_assert_int(raft_io_stub_n_appending(&F->io), ==, N_WRITE_LOG); \
                                                                             \
        raft_io_stub_flush_all(&F->io);                                      \
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
 * Trigger the tick callback.
 */
#define __tick(F, MSECS)                     \
    {                                        \
        raft_io_stub_advance(&F->io, MSECS); \
    }

/**
 * raft_apply
 */

TEST_SUITE(propose);

struct propose__fixture
{
    RAFT_FIXTURE;
    bool invoked;
    int status;
};

TEST_SETUP(propose)
{
    struct propose__fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    RAFT_SETUP(f);
    f->invoked = false;
    f->status = -1;
    return f;
}

TEST_TEAR_DOWN(propose)
{
    struct propose__fixture *f = data;
    RAFT_TEAR_DOWN(f);
    free(f);
}

static void apply_cb(struct raft_apply *req, int status)
{
    struct propose__fixture *f = req->data;
    f->invoked = true;
    f->status = status;
    free(req);
}

TEST_GROUP(propose, error);
TEST_GROUP(propose, success);

/* If the raft instance is not in leader state, an error is returned. */
TEST_CASE(propose, error, not_leader, NULL)
{
    struct propose__fixture *f = data;
    struct raft_apply req;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    test_fsm_encode_set_x(123, &buf);

    rv = raft_apply(&f->raft, &req, &buf, 1, NULL);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* If the raft instance steps down from leader state, the apply callback fires
 * with an error. */
TEST_CASE(propose, error, leadership_lost, NULL)
{
    struct propose__fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    propose_entry;

    /* Advance timer past the election timeout, forcing a step down */
    raft_io_stub_advance(&f->io, f->raft.election_timeout + 100);

    munit_assert_int(f->status, ==, RAFT_ERR_LEADERSHIP_LOST);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

static char *propose_oom_heap_fault_delay[] = {"0", "1", "2", NULL};
static char *propose_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum propose_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, propose_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, propose_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(propose, error, oom, propose_oom_params)
{
    struct propose__fixture *f = data;
    struct raft_apply req;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    test_fsm_encode_set_x(123, &buf);

    test_heap_fault_enable(&f->heap);

    /* Reset the request queue, to trigger a failure when attempting to grow
     * it. */
    /*raft_free(f->raft.io_queue.requests);
    f->raft.io_queue.requests = NULL;
    f->raft.io_queue.size = 0;*/

    rv = raft_apply(&f->raft, &req, &buf, 1, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* The raft instance is shutdown. */
TEST_CASE(propose, error, shutdown, NULL)
{
    struct propose__fixture *f = data;
    struct raft_apply *req = munit_malloc(sizeof *req);
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    test_fsm_encode_set_x(123, &buf);

    req->data = f;

    rv = raft_apply(&f->raft, req, &buf, 1, apply_cb);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

/* I/O error. */
TEST_CASE(propose, error, io_err, NULL)
{
    struct propose__fixture *f = data;
    struct raft_apply req;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    test_fsm_encode_set_x(123, &buf);

    raft_io_stub_fault(&f->io, 0, 1);

    rv = raft_apply(&f->raft, &req, &buf, 1, NULL);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    raft_free(buf.base);

    return MUNIT_OK;
}

/* The new entries are sent to all other servers. */
TEST_CASE(propose, success, send_entries, NULL)
{
    struct propose__fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    propose_entry;

    /* A write log and an append entries requests has been submitted. */
    __assert_io(f, 1, 1);

    return MUNIT_OK;
}

/**
 * raft_add_server
 */

TEST_SUITE(add_server);

struct add_server__fixture
{
    RAFT_FIXTURE;
};

TEST_SETUP(add_server)
{
    struct add_server__fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    RAFT_SETUP(f);
    return f;
}

TEST_TEAR_DOWN(add_server)
{
    struct add_server__fixture *f = data;
    RAFT_TEAR_DOWN(f);
    free(f);
}

TEST_GROUP(add_server, error);
TEST_GROUP(add_server, success);

/* Trying to add a server on a node which is not the leader results in an
 * error. */
TEST_CASE(add_server, error, not_leader, NULL)
{
    struct add_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    rv = raft_add_server(&f->raft, 3, "3");
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    return MUNIT_OK;
}

/* Trying to add a server while a configuration change is already in progress
 * results in an error. */
TEST_CASE(add_server, error, busy, NULL)
{
    struct add_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    add_server(3, "3");

    rv = raft_add_server(&f->raft, 4, "4");
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* Trying to add a server with an ID which is already in use results in an
 * error. */
TEST_CASE(add_server, error, dup_id, NULL)
{
    struct add_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_add_server(&f->raft, 1, "3");
    munit_assert_int(rv, ==, RAFT_EDUPID);

    return MUNIT_OK;
}

/* Submit a request to add a new non-voting server. */
TEST_CASE(add_server, success, submit, NULL)
{
    struct add_server__fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    add_server(3, "3");

    /* A write log and 2 append entries requests (one for each follower) have
     * been submitted. */
    __assert_io(f, 1, 2);

    return MUNIT_OK;
}

/* After a request to add a new non-voting server is committed, the new
 * configuration is not marked as uncommitted anymore */
TEST_CASE(add_server, success, committed, NULL)
{
    struct add_server__fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    add_server(3, "3");

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 3);
    server = &f->raft.configuration.servers[2];
    munit_assert_int(server->id, ==, 3);
    munit_assert_string_equal(server->address, "3");
    munit_assert_false(server->voting);

    /* The new configuration is marked as uncommitted. */
    __assert_configuration_indexes(f, 1, 2);

    /* The next/match indexes now include an entry for the new server. */
    munit_assert_int(f->raft.leader_state.progress[2].next_index, ==, 3);
    munit_assert_int(f->raft.leader_state.progress[2].match_index, ==, 0);

    __assert_io(f, 1, 2);

    /* Receive a successful append entries response reporting that one peer
     * has replicated the configuration entry. */
    __handle_append_entries_response(f, 2, 2, true, 2);

    /* The new configuration is marked as committed. */
    __assert_configuration_indexes(f, 2, 0);

    return MUNIT_OK;
}

/**
 * raft_promote
 */

TEST_SUITE(promote);

struct promote__fixture
{
    RAFT_FIXTURE;
    struct raft_apply req;
    bool invoked;
    int status;
};

TEST_SETUP(promote)
{
    struct promote__fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    RAFT_SETUP(f);
    f->req.data = f;
    f->invoked = false;
    f->status = -1;
    return f;
}

TEST_TEAR_DOWN(promote)
{
    struct promote__fixture *f = data;
    RAFT_TEAR_DOWN(f);
    free(f);
}

TEST_GROUP(promote, error);
TEST_GROUP(promote, success);

/* Trying to promote a server on a node which is not the leader results in an
 * error. */
TEST_CASE(promote, error, not_leader, NULL)
{
    struct promote__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);

    rv = raft_promote(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    return MUNIT_OK;
}

/* Trying to promote a server whose ID is unknown results in an
 * error. */
TEST_CASE(promote, error, bad_id, NULL)
{
    struct promote__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_promote(&f->raft, 4);
    munit_assert_int(rv, ==, RAFT_EBADID);

    return MUNIT_OK;
}

/* Promoting a server which is already a voting server is a no-op. */
TEST_CASE(promote, error, already_voting, NULL)
{
    struct promote__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_promote(&f->raft, 2);
    munit_assert_int(rv, ==, RAFT_EALREADYVOTING);

    return MUNIT_OK;
}

/* Trying to promote a server while another server is being promoted results in
 * an error. */
TEST_CASE(promote, error, in_progress, NULL)
{
    struct promote__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 4, 1, 2);
    test_become_leader(&f->raft);

    __promote(f, 3);

    rv = raft_promote(&f->raft, 4);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* Promoting a server whose log is already up-to-date results in the relevant
 * configuration change to be submitted immediately. */
TEST_CASE(promote, success, up_to_date, NULL)
{
    struct promote__fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Advance the match index of server 3. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    __promote(f, 3);

    /* Server 3 is being considered as voting, even though the configuration
     * change is not committed yet. */
    server = configuration__get(&f->raft.configuration, 3);
    munit_assert_true(server->voting);

    /* A configuration change request has been submitted. */
    __assert_io(f, 1, 2);

    return MUNIT_OK;
}

/* Promoting a server whose log is not up-to-date results in catch-up rounds to
 * start. When the server has caught up, the configuration change request gets
 * submitted. */
TEST_CASE(promote, success, catch_up, NULL)
{
    struct promote__fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
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
    server = configuration__get(&f->raft.configuration, 3);
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
TEST_CASE(promote, success, new_round, NULL)
{
    struct promote__fixture *f = data;

    (void)params;
    return 0;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    __promote(f, 3);

    /* Now that the catch-up round started, submit a new entry. */
    propose_entry;

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
TEST_CASE(promote, success, committed, NULL)
{
    struct promote__fixture *f = data;
    const struct raft_entry *entry;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Request to promote server 3 to voting. */
    __promote(f, 3);
    __assert_io(f, 0, 1);

    /* Now that the catch-up round started, submit a new entry. */
    propose_entry;
    __assert_io(f, 1, 2);

    /* Let more than election_timeout milliseconds elapse, but track a contact
     * from server 2 in between, to avoid stepping down. */
    __tick(f, f->raft.election_timeout - 100);
    f->raft.leader_state.progress[1].recent_recv = true;
    raft_io_stub_flush_all(&f->io);
    __tick(f, 200);
    __assert_io(f, 0, 2); /* Heartbeat */

    /* Simulate the server being promoted sending an AppendEntries result,
     * acknowledging all entries except the last one. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* The first round has completed and a new one has started. */
    __assert_catch_up_round(f, 3, 2, 0);

    /* Make a new client request, so even when this second round that just
     * started completes, the server being promoted will still be missing
     * entries. */
    propose_entry;
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

    entry = log__get(&f->raft.log, 4);
    munit_assert_int(entry->type, ==, RAFT_CONFIGURATION);

    return MUNIT_OK;
}

/* If leadership is lost before the configuration change log entry for promoting
 * the new server is committed, the leader configuration gets rolled back and
 * the server being promoted is not considered any more as voting. */
TEST_CASE(promote, success, step_down, NULL)
{
    struct promote__fixture *f = data;
    struct raft_entry *entries;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    /* Advance the match index of server 3. */
    __handle_append_entries_response(f, 3, f->raft.current_term, true, 1);

    /* Ask to promote server 3, which is already up-to-date (so the
     * configuration change request is submitted immediately. */
    __promote(f, 3);

    __assert_catch_up_round(f, 0, 0, 0);
    __assert_configuration_indexes(f, 1, 2);

    server = configuration__get(&f->raft.configuration, 3);
    munit_assert_true(server->voting);

    __assert_io(f, 1, 2);

    /* Receive an AppendEntries RPC from a leader with a higher term, forcing
     * this leader to step down and to discard the uncommitted configuration
     * change log entry. */
    server = configuration__get(&f->raft.configuration, 2);

    entries = raft_calloc(1, sizeof *entries);
    munit_assert_ptr_not_null(entries);

    entries[0].batch = raft_malloc(1);
    munit_assert_ptr_not_null(entries[0].batch);

    entries[0].buf.base = entries[0].batch;
    entries[0].buf.len = 1;
    entries[0].type = RAFT_COMMAND;
    entries[0].term = f->raft.current_term + 1;

    __handle_append_entries(f, 3, 2, 1, 1, entries, 1, 1);

    munit_assert_int(f->raft.state, ==, RAFT_FOLLOWER);

    /* Server 3 is not being considered voting anymore. */
    server = configuration__get(&f->raft.configuration, 3);
    munit_assert_false(server->voting);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* If a follower receives an AppendEntries RPC containing a
 * RAFT_CONFIGURATION entry which promotes a non-voting server, the
 * configuration change is immediately applied locally, even if the entry is not
 * yet committed. Once the entry is committed, the change becomes permanent.*/
TEST_CASE(promote, success, follower, NULL)
{
    struct promote__fixture *f = data;
    struct raft_configuration configuration;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);

    /* Encode the new configuration into a buffer. */
    raft_configuration_init(&configuration);

    rv = configuration__copy(&f->raft.configuration, &configuration);
    munit_assert_int(rv, ==, 0);

    configuration.servers[2].voting = true;

    rv = configuration__encode(&configuration, &buf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&configuration);

    entries = raft_calloc(1, sizeof *entries);
    munit_assert_ptr_not_null(entries);

    entries[0].batch = buf.base;

    entries[0].buf = buf;
    entries[0].type = RAFT_CONFIGURATION;
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

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/**
 * raft_remove_server
 */

TEST_SUITE(remove_server);

struct remove_server__fixture
{
    RAFT_FIXTURE;
};

TEST_SETUP(remove_server)
{
    struct remove_server__fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    RAFT_SETUP(f);
    return f;
}

TEST_TEAR_DOWN(remove_server)
{
    struct remove_server__fixture *f = data;
    RAFT_TEAR_DOWN(f);
    free(f);
}

TEST_GROUP(remove_server, error);
TEST_GROUP(remove_server, success);

/* Trying to remove a server on a node which is not the leader results in an
 * error. */
TEST_CASE(remove_server, error, not_leader, NULL)
{
    struct remove_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);

    rv = raft_remove_server(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_ERR_NOT_LEADER);

    return MUNIT_OK;
}

/* Trying to remove a server while a configuration change is already in progress
 * results in an error. */
TEST_CASE(remove_server, error, busy, NULL)
{
    struct remove_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    add_server(3, "3");

    rv = raft_remove_server(&f->raft, 2);
    munit_assert_int(rv, ==, RAFT_ERR_CONFIGURATION_BUSY);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* Trying to remove a server with an unknwon ID results in an error. */
TEST_CASE(remove_server, error, bad_id, NULL)
{
    struct remove_server__fixture *f = data;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    rv = raft_remove_server(&f->raft, 3);
    munit_assert_int(rv, ==, RAFT_EBADID);

    return MUNIT_OK;
}

/* Submit a request to remove a server. */
TEST_CASE(remove_server, success, submit, NULL)
{
    struct remove_server__fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 2);
    test_become_leader(&f->raft);

    __remove_server(f, 3);

    /* A write log and 1 append entriy request (for the remaining follower) have
     * been submitted. */
    __assert_io(f, 1, 1);

    return MUNIT_OK;
}

/* After a request to remove server is committed, the new configuration is not
 * marked as uncommitted anymore */
TEST_CASE(remove_server, success, committed, NULL)
{
    struct remove_server__fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    __remove_server(f, 3);

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 2);
    server = configuration__get(&f->raft.configuration, 3);
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
TEST_CASE(remove_server, success, self, NULL)
{
    struct remove_server__fixture *f = data;
    const struct raft_server *server;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    __remove_server(f, 1);

    /* The new configuration is already effective. */
    munit_assert_int(f->raft.configuration.n, ==, 2);
    server = configuration__get(&f->raft.configuration, 1);
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
    __assert_state(f, RAFT_FOLLOWER);

    return MUNIT_OK;
}
