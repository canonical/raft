#include "../../include/raft.h"

#include "../../src/io_queue.h"

#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct raft_io_queue queue;
    struct raft_io io;
    unsigned elapsed; /* Milliseconds since last call to __tick */
};

static void __tick(void *p, const unsigned elapsed)
{
    struct fixture *f = p;

    munit_assert_ptr_not_null(f);

    f->elapsed = elapsed;
}

static void __notify(void *p, const unsigned id, const int status)
{
    (void)p;
    (void)id;
    (void)status;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);

    raft_io_queue__init(&f->queue);

    rv = raft_io_stub_init(&f->io);
    munit_assert_int(rv, ==, 0);

    f->io.init(&f->io, &f->queue, f, __tick, __notify);
    f->elapsed = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    f->io.close(&f->io);

    raft_io_queue__close(&f->queue);

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Start the backend.
 */
#define __start(F, MSECS)                \
    {                                    \
        int rv;                          \
                                         \
        rv = F->io.start(&F->io, MSECS); \
        munit_assert_int(rv, ==, 0);     \
    }

/**
 * Advance time.
 */
#define __advance(F, MSECS)                  \
    {                                        \
        raft_io_stub_advance(&F->io, MSECS); \
    }

/**
 * Push a new request to the I/O queue.
 */
#define __push_io_request(F, ID, REQUEST)             \
    {                                                 \
        int rv;                                       \
                                                      \
        rv = raft_io_queue__push(&F->queue, ID);      \
        munit_assert_int(rv, ==, 0);                  \
                                                      \
        *REQUEST = raft_io_queue_get(&F->queue, *ID); \
        munit_assert_ptr_not_null(*REQUEST);          \
    }

/**
 * Submit an I/O request and check that no error occurred.x
 */
#define __submit(F, ID)                \
    {                                  \
        int rv;                        \
                                       \
        rv = F->io.submit(&F->io, ID); \
        munit_assert_int(rv, ==, 0);   \
    }

/**
 * Assert the current state of the raft instance of the given fixture.
 */
#define __assert_state(F, STATE) munit_assert_int(F->raft.state, ==, STATE);

/**
 * raft_io_sim__start
 */

/* When raft_io_stub_advance is called, the tick callback is invoked. */
static MunitResult test_start_advance(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __start(f, 500);
    __advance(f, 100);

    munit_assert_int(f->elapsed, ==, 100);

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/advance", test_start_advance, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_sim__submit
 */

static MunitResult test_bootstrap(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    unsigned request_id;
    struct raft_io_request *request;
    int rv;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_BOOTSTRAP;

    /* Create a configuration and encode it in the request */
    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    rv = raft_encode_configuration(&configuration,
                                   &request->args.bootstrap.conf);
    munit_assert_int(rv, ==, 0);

    raft_configuration_close(&configuration);

    /* Submit the bootstrap request */
    __submit(f, request_id);

    raft_free(request->args.bootstrap.conf.base);

    /* The log has now one entry. */
    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    munit_assert_int(request->result.read_state.n_entries, ==, 1);

    raft_free(request->result.read_state.entries[0].batch);
    raft_free(request->result.read_state.entries);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

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

    raft_io_queue__pop(&f->queue, request_id);

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

    raft_io_queue__pop(&f->queue, request_id);

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
    struct raft_entry *entries;

    (void)params;

    __push_io_request(f, &request_id1, &request1);

    entry.term = 1;
    entry.type = RAFT_LOG_COMMAND;
    entry.buf.base = munit_malloc(1);
    entry.buf.len = 1;

    ((char *)entry.buf.base)[0] = 'x';

    request1->type = RAFT_IO_WRITE_LOG;
    request1->args.write_log.entries = &entry;
    request1->args.write_log.n = 1;

    __submit(f, request_id1);

    __push_io_request(f, &request_id2, &request2);

    request2->type = RAFT_IO_READ_STATE;

    __submit(f, request_id2);

    /* This WRITE_LOG request is asynchronous, the entries have not been
     * persited yet. */
    munit_assert_int(request2->result.read_state.first_index, ==, 0);
    munit_assert_int(request2->result.read_state.n_entries, ==, 0);

    raft_io_stub_flush(&f->io);

    /* The log has now one entry, witch matches the one we wrote. */
    request2->type = RAFT_IO_READ_STATE;

    __submit(f, request_id2);

    munit_assert_int(request2->result.read_state.n_entries, ==, 1);

    entries = request2->result.read_state.entries;
    munit_assert_int(entries[0].buf.len, ==, 1);
    munit_assert_int(((char *)entries[0].buf.base)[0], ==, 'x');
    munit_assert_ptr_not_null(entries[0].batch);

    raft_free(entries[0].batch);
    raft_free(entries);

    raft_io_queue__pop(&f->queue, request_id1);
    raft_io_queue__pop(&f->queue, request_id2);

    free(entry.buf.base);

    return MUNIT_OK;
}

static MunitTest submit_tests[] = {
    {"/bootstrap", test_bootstrap, setup, tear_down, 0, NULL},
    {"/write-term", test_write_term, setup, tear_down, 0, NULL},
    {"/write-vote", test_write_vote, setup, tear_down, 0, NULL},
    {"/write-log", test_write_log, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_stub_suites[] = {
    {"/start", start_tests, NULL, 1, 0},
    {"/submit", submit_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
