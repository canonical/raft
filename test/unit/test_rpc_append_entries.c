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
    entries[0].type = RAFT_COMMAND;
    entries[0].buf.base = batch + 8 + 16;
    entries[0].buf.len = 8;
    entries[0].batch = batch;

    return entries;
}

/**
 * Call recv__append_entries with the given parameters and check that
 * no error occurs.
 */
#define __recv_append_entries(F, TERM, LEADER_ID, PREV_LOG_INDEX, \
                              PREV_LOG_TERM, ENTRIES, N, COMMIT)  \
    {                                                             \
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
#define __recv_append_entries_result(F, SERVER_ID, TERM, SUCCESS, \
                                     LAST_LOG_INDEX)              \
    {                                                             \
        struct raft_message message;                              \
        struct raft_append_entries_result *result;                \
        char address[4];                                          \
                                                                  \
        sprintf(address, "%d", SERVER_ID);                        \
        message.type = RAFT_IO_APPEND_ENTRIES_RESULT;             \
        message.server_id = SERVER_ID;                            \
        message.server_address = address;                         \
                                                                  \
        result = &message.append_entries_result;                  \
        result->term = TERM;                                      \
        result->success = SUCCESS;                                \
        result->last_log_index = LAST_LOG_INDEX;                  \
                                                                  \
        raft_io_stub_deliver(&F->io, &message);                   \
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
#define __assert_append_entries_response(F, TERM, SUCCESS, LAST_LOG_INDEX)  \
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
        munit_assert_int(result->success, ==, SUCCESS);                     \
        munit_assert_int(result->last_log_index, ==, LAST_LOG_INDEX);       \
    }

/**
 * raft_handle_append_entries
 */

TEST_SUITE(request);

TEST_SETUP(request, setup);
TEST_TEAR_DOWN(request, tear_down);

TEST_GROUP(request, success);
TEST_GROUP(request, error);

/* If the term in the request is stale, the server rejects it. */
TEST_CASE(request, success, stale_term, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Become candidate, this will bump our term. */
    test_become_candidate(&f->raft);

    __recv_append_entries(f, 1, 2, 0, 0, NULL, 0, 1);

    /* The request is unsuccessful */
    __assert_append_entries_response(f, 2, false, 1);

    return MUNIT_OK;
}

/* Receive the same entry a second time, before the first has been persisted. */
TEST_CASE(request, success, twice, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries1 = __create_entries_batch();
    struct raft_entry *entries2 = __create_entries_batch();
    struct raft_message *message;
    struct raft_append_entries_result *result;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_append_entries(f, 1, 2, 1, 1, entries1, 1, 1);
    __recv_append_entries(f, 1, 2, 1, 1, entries2, 1, 1);

    /* The duplicate entry has been ignored. */
    raft_io_stub_flush(&f->io);

    munit_assert_int(raft_io_stub_n_sending(&f->io), ==, 2);

    raft_io_stub_sending(&f->io, 0, &message);
    result = &message->append_entries_result;
    munit_assert_int(result->success, ==, 1);
    munit_assert_int(result->last_log_index, ==, 2);

    raft_io_stub_sending(&f->io, 1, &message);
    result = &message->append_entries_result;
    munit_assert_int(result->success, ==, 1);
    munit_assert_int(result->last_log_index, ==, 2);

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning an higher term as its
 * own, it it steps down to follower and accept the request . */
TEST_CASE(request, success, higher_term, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __recv_append_entries(f, 3, 2, 1, 1, NULL, 0, 1);

    /* We have stepped down to follower. */
    __assert_state(f, RAFT_FOLLOWER);

    /* We have updated our leader. */
    __assert_current_leader_id(f, 2);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* If a candidate server receives a request contaning the same term as its
 * own, it it steps down to follower and accept the request . */
TEST_CASE(request, success, same_term, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_candidate(&f->raft);

    __recv_append_entries(f, 2, 2, 1, 1, NULL, 0, 1);

    /* We have stepped down to follower. */
    __assert_state(f, RAFT_FOLLOWER);

    /* We have updated our leader. */
    __assert_current_leader_id(f, 2);

    raft_io_stub_flush_all(&f->io);

    return MUNIT_OK;
}

/* If the index and term of the last snapshot on the server match prevLogIndex
 * and prevLogTerm the request is accepted. */
TEST_CASE(request, success, snapshot_match, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries = __create_entries_batch();

    (void)params;

    test_set_initial_snapshot(&f->raft, 2, 4, 1, 1);
    test_start(&f->raft);

    entries[0].term = 2;

    __recv_append_entries(f, 2, 2, 4, 2, entries, 1, 4);

    raft_io_stub_flush(&f->io);

    /* The request got accepted. */
    __assert_append_entries_response(f, 2, true, 5);

    return MUNIT_OK;
}

/* If server's log is shorter than prevLogIndex, the request is rejected . */
TEST_CASE(request, error, missing_entries, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_append_entries(f, 1, 2, 2, 1, NULL, 0, 1);

    /* The request is unsuccessful */
    __assert_append_entries_response(f, 1, false, 1);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from
 * prevLogTerm, and value of prevLogIndex is lower or equal than server's commit
 * index, then an error is returned . */
TEST_CASE(request, error, prev_index_conflict, NULL)
{
    struct fixture *f = data;
    struct raft_message message;
    struct raft_append_entries *args;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = 2;
    message.server_address = "2";

    args = &message.append_entries;
    args->term = 1;
    args->leader_id = 2;
    args->prev_log_index = 1;
    args->prev_log_term = 2;
    args->entries = NULL;
    args->n_entries = 0;
    args->leader_commit = 1;

    raft_io_stub_deliver(&f->io, &message);

    return MUNIT_OK;
}

/* If the term of the last log entry on the server is different from the one
 * prevLogTerm, and value of prevLogIndex is greater than server's commit commit
 * index (i.e. this is a normal inconsistency), we reject the request. */
TEST_CASE(request, error, prev_log_term_mismatch, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    struct raft_entry entries[2];

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Append two uncommitted entries. */
    entries[0].type = RAFT_COMMAND;
    entries[0].term = 1;
    entries[0].buf.base = raft_malloc(8);
    entries[0].buf.len = 8;

    entries[1].type = RAFT_COMMAND;
    entries[1].term = 1;
    entries[1].buf.base = raft_malloc(8);
    entries[1].buf.len = 8;

    test_io_append_entry(f->raft.io, &entries[0]);
    test_io_append_entry(f->raft.io, &entries[1]);

    memset(&buf, 0, sizeof buf);

    log__append(&f->raft.log, 1, RAFT_COMMAND, &buf, NULL);
    log__append(&f->raft.log, 1, RAFT_COMMAND, &buf, NULL);

    __recv_append_entries(f, 1, 2, 2, 2, NULL, 0, 1);

    /* The request gets rejected. */
    __assert_append_entries_response(f, 1, false, 3);

    raft_free(entries[0].buf.base);
    raft_free(entries[1].buf.base);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. */
TEST_CASE(request, success, write_log, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries = __create_entries_batch();

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_append_entries(f, 1, 2, 1, 1, entries, 1, 1);

    /* A write request has been submitted. */
    munit_assert_int(raft_io_stub_n_appending(&f->io), ==, 1);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log, they will be skipped. */
TEST_CASE(request, success, skip, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    const struct raft_entry *appended;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    unsigned n;
    int rv;

    (void)params;

    munit_assert_ptr_not_null(buf1);
    munit_assert_ptr_not_null(buf2);

    *buf1 = 1;
    *buf2 = 2;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    entries[0].type = RAFT_COMMAND;
    entries[0].term = 1;
    entries[0].buf.base = buf1;
    entries[0].buf.len = 1;

    entries[1].type = RAFT_COMMAND;
    entries[1].term = 1;
    entries[1].buf.base = buf2;
    entries[1].buf.len = 1;

    /* Append the first entry to our log. */
    test_io_append_entry(f->raft.io, &entries[0]);
    rv = log__append(&f->raft.log, 1, RAFT_COMMAND, &entries[0].buf, NULL);
    munit_assert_int(rv, ==, 0);

    __recv_append_entries(f, 1, 2, 1, 1, entries, 2, 1);

    /* A write request has been submitted, only for the second entry. */
    munit_assert_int(raft_io_stub_n_appending(&f->io), ==, 1);

    raft_io_stub_appending(&f->io, 0, &appended, &n);
    munit_assert_int(n, ==, 1);
    munit_assert_int(appended[0].type, ==, RAFT_COMMAND);
    munit_assert_int(*(uint8_t *)appended[0].buf.base, ==, 2);

    return MUNIT_OK;
}

/* A write log request is submitted for outstanding log entries. If some entries
 * are already existing in the log but they have a different term, they will be
 * replaced. */
TEST_CASE(request, success, truncate, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    const struct raft_entry *appended;
    unsigned n;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    uint8_t *buf3 = raft_malloc(1);
    int rv;

    *buf1 = 1;
    *buf2 = 2;
    *buf3 = 3;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Append an additional entry to our log. */
    entry.type = RAFT_COMMAND;
    entry.term = 1;
    entry.buf.base = buf1;
    entry.buf.len = 1;

    test_io_append_entry(&f->io, &entry);
    rv = log__append(&f->raft.log, 1, RAFT_COMMAND, &entry.buf, NULL);
    munit_assert_int(rv, ==, 0);

    /* Include two new entries with a different term in the request */
    entries[0].type = RAFT_COMMAND;
    entries[0].term = 2;
    entries[0].buf.base = buf2;
    entries[0].buf.len = 1;
    entries[1].type = RAFT_COMMAND;
    entries[1].term = 2;
    entries[1].buf.base = buf3;
    entries[1].buf.len = 1;

    __recv_append_entries(f, 2, 2, 1, 1, entries, 2, 1);

    /* A write request has been submitted, for both the two new entries. */
    munit_assert_int(raft_io_stub_n_appending(&f->io), ==, 1);

    raft_io_stub_appending(&f->io, 0, &appended, &n);

    munit_assert_int(n, ==, 2);
    munit_assert_int(*(uint8_t *)appended[0].buf.base, ==, 2);
    munit_assert_int(*(uint8_t *)appended[1].buf.base, ==, 3);

    return MUNIT_OK;
}

/* If any of the new entry has the same index of an existing entry in our log,
 * but different term, and that entry index is already committed, we bail out
 * with an error. */
TEST_CASE(request, error, conflict, NULL)
{
    struct fixture *f = data;
    struct raft_entry entry;
    struct raft_entry *entries = raft_malloc(2 * sizeof *entries);
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    uint8_t *buf1 = raft_malloc(1);
    uint8_t *buf2 = raft_malloc(1);
    uint8_t *buf3 = raft_malloc(1);
    int rv;

    *buf1 = 1;
    *buf2 = 2;
    *buf3 = 3;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    /* Append an additional entry to our log, with index 2 and term 1. */
    entry.type = RAFT_COMMAND;
    entry.term = 1;
    entry.buf.base = buf1;
    entry.buf.len = 1;

    test_io_append_entry(f->raft.io, &entry);
    rv = log__append(&f->raft.log, 1, RAFT_COMMAND, &entry.buf, NULL);
    munit_assert_int(rv, ==, 0);

    /* Bump the commit index. */
    f->raft.commit_index = 2;

    /* Include two new entries with a different term in the request */
    entries[0].type = RAFT_COMMAND;
    entries[0].term = 2;
    entries[0].buf.base = &buf2;
    entries[0].buf.len = 1;
    entries[1].type = RAFT_COMMAND;
    entries[1].term = 2;
    entries[1].buf.base = &buf3;
    entries[1].buf.len = 1;

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = 2;
    message.server_address = "2";

    args->term = 2;
    args->leader_id = 2;
    args->prev_log_index = 1;
    args->prev_log_term = 1;
    args->entries = entries;
    args->n_entries = 2;
    args->leader_commit = 1;

    /* We return a shutdown error. */
    raft_io_stub_deliver(&f->io, &message);

    /* TODO: should the code itself perform this cleanup? */
    raft_free(buf2);
    raft_free(buf3);
    raft_free(entries);

    return MUNIT_OK;
}

/**
 * raft_handle_append_entries_response
 */

TEST_SUITE(response);

TEST_SETUP(response, setup);
TEST_TEAR_DOWN(response, tear_down);

TEST_GROUP(response, error);
TEST_GROUP(response, success);

/* If the server handling the response is not the leader, the result
 * is ignored. */
TEST_CASE(response, error, not_leader, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);

    __recv_append_entries_result(f, 2, 1, true, 1);

    return MUNIT_OK;
}

/* If the response has a term which is lower than the server's one, it's
 * ignored. */
TEST_CASE(response, error, ignore, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Receive an append entries response with a stale term. */
    __recv_append_entries_result(f, 2, 1, true, 2);

    return MUNIT_OK;
}

/* If the response has a term which is higher than the server's one, step down
 * to follower. */
TEST_CASE(response, error, step_down, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_bootstrap_and_start(&f->raft, 2, 1, 2);
    test_become_leader(&f->raft);

    /* Receive an append entries response with a newer term. */
    __recv_append_entries_result(f, 2, 3, false, 2);

    __assert_state(f, RAFT_FOLLOWER);

    return MUNIT_OK;
}

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

/* If a majority of servers has replicated an entry, commit it. */
TEST_CASE(response, success, commit, NULL)
{
    struct fixture *f = data;
    struct raft_apply req;
    struct raft_buffer buf;
    int rv;

    (void)params;

    test_bootstrap_and_start(&f->raft, 3, 1, 3);
    test_become_leader(&f->raft);

    /* Append an entry to our log and handle the associated successful write. */
    test_fsm_encode_set_x(123, &buf);

    rv = raft_apply(&f->raft, &req, &buf, 1, NULL);
    munit_assert_int(rv, ==, 0);

    raft_io_stub_flush_all(f->raft.io);

    /* Receive a successful append entries response reporting that the peer
     * has replicated that entry. */
    __recv_append_entries_result(f, 2, 2, true, 2);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 2);

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
