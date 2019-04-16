#include <stdio.h>

#include "../../include/raft.h"

#include "../../src/configuration.h"
#include "../../src/log.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/io.h"
#include "../lib/raft.h"
#include "../lib/runner.h"

TEST_MODULE(rpc_append_entries);

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
 * Call recv__append_entries with the given parameters and check that
 * no error occurs.
 */
#define __recv_append_entries(F, TERM, LEADER_ID, PREV_LOG_INDEX, \
                              PREV_LOG_TERM, ENTRIES, N, COMMIT)  \
    {                                                             \
        struct raft_message message2;                             \
        struct raft_append_entries *args;                         \
        char address[4];                                          \
                                                                  \
        sprintf(address, "%d", LEADER_ID);                        \
        message2.type = RAFT_IO_APPEND_ENTRIES;                   \
        message2.server_id = LEADER_ID;                           \
        message2.server_address = address;                        \
                                                                  \
        args = &message2.append_entries;                          \
        args->term = TERM;                                        \
        args->leader_id = LEADER_ID;                              \
        args->prev_log_index = PREV_LOG_INDEX;                    \
        args->prev_log_term = PREV_LOG_TERM;                      \
        args->entries = ENTRIES;                                  \
        args->n_entries = N;                                      \
        args->leader_commit = COMMIT;                             \
                                                                  \
        raft_io_stub_deliver(&F->io, &message2);                  \
    }

/**
 * Call recv__append_entries_result with the given parameters and check
 * that no error occurs.
 */
#define __recv_append_entries_result(F, SERVER_ID, TERM, SUCCESS, \
                                     LAST_LOG_INDEX)              \
    {                                                             \
        struct raft_message message2;                             \
        struct raft_append_entries_result *result;                \
        char address[4];                                          \
                                                                  \
        sprintf(address, "%d", SERVER_ID);                        \
        message2.type = RAFT_IO_APPEND_ENTRIES_RESULT;            \
        message2.server_id = SERVER_ID;                           \
        message2.server_address = address;                        \
                                                                  \
        result = &message2.append_entries_result;                 \
        result->term = TERM;                                      \
        result->rejected = SUCCESS ? 0 : 1;                       \
        result->last_log_index = LAST_LOG_INDEX;                  \
                                                                  \
        raft_io_stub_deliver(&F->io, &message2);                  \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * Assert the current leader ID of the raft instance of the given fixture.
 */
#define __assert_current_leader_id(F, ID) \
    munit_assert_int(F->raft.follower_state.current_leader.id, ==, ID);

/**
 * Assert that the test I/O implementation has received exactly one
 * AppendEntries response RPC with the given parameters.
 */
#define __assert_append_entries_response(F, TERM, REJECTED, LAST_LOG_INDEX) \
    {                                                                       \
        struct raft_message *message;                                       \
        struct raft_append_entries_result *result;                          \
                                                                            \
        munit_assert_int(raft_io_stub_n_sending(&F->io), ==, 1);            \
                                                                            \
        raft_io_stub_sending(&F->io, 0, &message);                          \
        munit_assert_int(message->type, ==, RAFT_IO_APPEND_ENTRIES_RESULT); \
                                                                            \
        result = &message->append_entries_result;                           \
        munit_assert_int(result->term, ==, TERM);                           \
        munit_assert_int(result->rejected, ==, REJECTED);                   \
        munit_assert_int(result->last_log_index, ==, LAST_LOG_INDEX);       \
    }

/**
 * raft_handle_append_entries
 */

TEST_SUITE(request);

TEST_SETUP(request, setup);
TEST_TEAR_DOWN(request, tear_down);

/**
 * raft_handle_append_entries_response
 */

TEST_SUITE(response);

TEST_SETUP(response, setup);
TEST_TEAR_DOWN(response, tear_down);

TEST_GROUP(response, error);
TEST_GROUP(response, success);

/* If the response fails because a log mismatch, the nextIndex for the server is
 * updated and the relevant older entries are resent. */
TEST_CASE(response, error, retry, NULL)
{
    struct fixture *f = data;
    struct raft_message *message;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Receive an unsuccessful append entries response reporting that the peer's
     * last log entry has index 0 (peer's log is empty. */
    __recv_append_entries_result(f, 2, 2, false, 0);

    /* We have resent entry 1. */
    munit_assert_int(raft_io_stub_n_sending(&f->io), ==, 1);
    raft_io_stub_sending(&f->io, 0, &message);
    munit_assert_int(message->append_entries.n_entries, ==, 1);

    return MUNIT_OK;
}

/* If after committing an entry the snapshot threshold is hit, a new snapshot is
 * taken. */
TEST_CASE(response, success, snapshot, NULL)
{
    struct fixture *f = data;
    struct raft_apply reqs[2];
    struct raft_buffer buf;
    unsigned i;
    int rv;

    (void)params;

    f->raft.snapshot.threshold = 1;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    /* Append a couple of entries to our log and handle the associated
     * successful write. */
    for (i = 0; i < 2; i++) {
        test_fsm_encode_set_x(i, &buf);
        rv = raft_apply(&f->raft, &reqs[i], &buf, 1, NULL);
        munit_assert_int(rv, ==, 0);
        raft_io_stub_flush_all(f->raft.io);
    }

    /* Receive a successful append entries response reporting that the peer
     * has replicated those entries. */
    __recv_append_entries_result(f, 2, 2, true, 3);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 3);

    /* A snapshot was started */
    munit_assert_int(f->raft.snapshot.pending.index, ==, 3);
    munit_assert_int(f->raft.snapshot.pending.term, ==, 2);

    raft_io_stub_flush_all(f->raft.io);

    munit_assert_int(f->raft.log.snapshot.last_index, ==, 3);
    munit_assert_int(f->raft.log.snapshot.last_term, ==, 2);

    return MUNIT_OK;
}

/* If a follower falls behind the next available log entry, the last snapshot is
 * sent. */
TEST_CASE(response, success, send_snapshot, NULL)
{
    struct fixture *f = data;
    struct raft_apply reqs[2];
    struct raft_buffer buf;
    unsigned i;
    int rv;

    (void)params;

    f->raft.snapshot.threshold = 1;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    /* Append a couple of entries to our log and handle the associated
     * successful write. */
    for (i = 0; i < 2; i++) {
        test_fsm_encode_set_x(i, &buf);
        rv = raft_apply(&f->raft, &reqs[i], &buf, 1, NULL);
        munit_assert_int(rv, ==, 0);
        raft_io_stub_flush_all(f->raft.io);
    }

    /* Receive a successful append entries response reporting that the peer
     * has replicated those entries. */
    __recv_append_entries_result(f, 2, 2, true, 3);

    /* Wait for the resulting snapshot to complete. */
    raft_io_stub_flush_all(f->raft.io);
    munit_assert_int(f->raft.log.snapshot.last_index, ==, 3);
    munit_assert_int(f->raft.log.snapshot.last_term, ==, 2);

    raft_io_stub_advance(&f->io, f->raft.heartbeat_timeout + 1);

    raft_io_stub_flush_all(f->raft.io);
    raft_io_stub_flush_all(f->raft.io);

    return MUNIT_OK;
}
