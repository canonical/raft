#include "../../include/raft.h"

#include "../../src/io_queue.h"

#include "../lib/fsm.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"

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

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);

    rv = raft_io_sim_init(&f->raft);
    munit_assert_int(rv, ==, 0);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Push a new request to the I/O queue.
 */
#define __push_io_request(F, ID, REQUEST)            \
    {                                                \
        int rv;                                      \
                                                     \
        rv = raft_io_queue__push(&F->raft, ID);      \
        munit_assert_int(rv, ==, 0);                 \
                                                     \
        *REQUEST = raft_io_queue_get(&F->raft, *ID); \
        munit_assert_ptr_not_null(*REQUEST);         \
    }

/**
 * Submit an I/O request and check that no error occurred.x
 */
#define __submit(F, ID)                        \
    {                                          \
        int rv;                                \
                                               \
        rv = F->raft.io_.submit(&F->raft, ID); \
        munit_assert_int(rv, ==, 0);           \
    }

/**
 * raft_io_sim__submit
 */

static MunitResult test_write_term(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 1;

    __submit(f, request_id);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    munit_assert_int(request->result.read_state.term, ==, 1);
    munit_assert_int(request->result.read_state.voted_for, ==, 0);

    raft_io_queue__pop(&f->raft, request_id);

    return MUNIT_OK;
}

static MunitResult test_write_vote(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 1;

    __submit(f, request_id);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    munit_assert_int(request->result.read_state.term, ==, 0);
    munit_assert_int(request->result.read_state.voted_for, ==, 1);
    munit_assert_int(request->result.read_state.first_index, ==, 0);
    munit_assert_int(request->result.read_state.n_entries, ==, 0);

    raft_io_queue__pop(&f->raft, request_id);

    return MUNIT_OK;
}

static MunitResult test_write_log(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned request_id1;
    unsigned request_id2;
    struct raft_io_request *request1;
    struct raft_io_request *request2;
    struct raft_entry entry;

    (void)params;

    __push_io_request(f, &request_id1, &request1);

    entry.term = 1;
    entry.type = RAFT_LOG_COMMAND;
    entry.buf.base = munit_malloc(1);
    entry.buf.len = 1;

    request1->type = RAFT_IO_WRITE_LOG;
    request1->args.write_log.entries = &entry;
    request1->args.write_log.n = 1;
    request1->cb = NULL;

    __submit(f, request_id1);

    __push_io_request(f, &request_id2, &request2);

    request2->type = RAFT_IO_READ_STATE;

    __submit(f, request_id2);

    /* This the WRITE_LOG request is asynchronous, the entries have not been
     * persited yet. */
    munit_assert_int(request2->result.read_state.first_index, ==, 0);
    munit_assert_int(request2->result.read_state.n_entries, ==, 0);

    raft_io_sim_flush(&f->raft);

    /* The log has now one entry. */
    __submit(f, request_id2);
    munit_assert_int(request2->result.read_state.first_index, ==, 1);
    munit_assert_int(request2->result.read_state.n_entries, ==, 1);

    raft_io_queue__pop(&f->raft, request_id1);
    raft_io_queue__pop(&f->raft, request_id2);

    free(entry.buf.base);

    return MUNIT_OK;
}

static MunitTest submit_tests[] = {
    {"/write-term", test_write_term, setup, tear_down, 0, NULL},
    {"/write-vote", test_write_vote, setup, tear_down, 0, NULL},
    {"/write-log", test_write_log, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_sim_suites[] = {
    {"/submit", submit_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
