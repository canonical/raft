#include "../../include/raft.h"

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_queue.h"

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    char *dir;
    struct uv_loop_s loop;
    struct raft_heap heap;
    struct raft_io_queue queue;
    struct raft_logger logger;
    struct raft_io io;
    unsigned elapsed; /* Milliseconds since last call to __tick */
    struct
    {
        unsigned id;
        int status;
    } notify;
    bool stopped; /* The store has been completely stopped */
};

static void __stop(void *p)
{
    struct fixture *f = p;

    f->stopped = true;
}

static void __tick(void *p, const unsigned elapsed)
{
    struct fixture *f = p;

    munit_assert_ptr_not_null(f);

    f->elapsed = elapsed;
}

static void __notify(void *p, const unsigned id, const int status)
{
    struct fixture *f = p;

    f->notify.id = id;
    f->notify.status = status;
}

#define __MAX_LOOP_RUN 10 /* Max n. of loop iterations upon teardown */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);

    f->dir = test_dir_setup(params);

    rv =
        uv_replace_allocator(raft_malloc, raft_realloc, raft_calloc, raft_free);
    munit_assert_int(rv, ==, 0);

    rv = uv_loop_init(&f->loop);
    munit_assert_int(rv, ==, 0);

    raft_io_queue__init(&f->queue);

    test_logger_setup(params, &f->logger, id);

    rv = raft_io_uv_init(&f->io, &f->loop, f->dir);
    munit_assert_int(rv, ==, 0);

    f->io.init(&f->io, &f->queue, &f->logger, f, __tick, __notify);

    f->elapsed = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    f->io.close(&f->io);

    raft_io_queue__close(&f->queue);
    test_logger_tear_down(&f->logger);

    rv = uv_loop_close(&f->loop);
    munit_assert_int(rv, ==, 0);

    rv = uv_replace_allocator(malloc, realloc, calloc, free);
    munit_assert_int(rv, ==, 0);

    test_dir_tear_down(f->dir);

    test_heap_tear_down(&f->heap);

    free(f);
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
 * Wait until the request with the given ID is completed.
 */
#define __wait(F, ID)                                           \
    {                                                           \
        int i;                                                  \
        int rv;                                                 \
                                                                \
        /* Run the loop until the write request is completed */ \
        for (i = 0; i < 20; i++) {                              \
            rv = uv_run(&F->loop, UV_RUN_ONCE);                 \
            munit_assert_int(rv, ==, 1);                        \
                                                                \
            if (F->notify.id == ID) {                           \
                break;                                          \
            }                                                   \
        }                                                       \
        munit_assert_int(F->notify.id, ==, ID);                 \
        munit_assert_int(F->notify.status, ==, 0);              \
                                                                \
        F->notify.id = 0;                                       \
        F->notify.status = -1;                                  \
    }

/**
 * raft_io_uv_init
 */

static char *init_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *init_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum init_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, init_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, init_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions */
static MunitResult test_init_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    test_heap_fault_enable(&f->heap);

    rv = raft_io_uv_init(&io, &f->loop, f->dir);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* Create data directory if it does not exist */
static MunitTest init_tests[] = {
    {"/oom", test_init_oom, setup, tear_down, 0, init_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__start
 */

/* Once the raft_io_uv instance is started, the tick function gets called
 * periodically. */
static MunitResult test_start_tick(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    rv = f->io.start(&f->io, 50);
    munit_assert_int(rv, ==, 0);

    /* Run the loop and check that the tick callback was called. */
    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 1);

    munit_assert_int(f->elapsed, >=, 25);

    rv = f->io.stop(&f->io, __stop);
    munit_assert_int(rv, ==, 0);

    /* Let the loop run a cycle, so we trigger the close callback of the timer
     * handle. */
    rv = uv_run(&f->loop, UV_RUN_ONCE);
    munit_assert_int(rv, ==, 0);

    return MUNIT_OK;
}

static MunitTest start_tests[] = {
    {"/tick", test_start_tick, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__submit
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

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

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

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 1;

    __submit(f, request_id);

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

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 1;

    __submit(f, request_id);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

static MunitResult test_write_log(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    struct raft_entry entry;
    int rv;

    (void)params;

    rv = f->io.start(&f->io, 50);
    munit_assert_int(rv, ==, 0);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    entry.term = 1;
    entry.type = RAFT_LOG_COMMAND;
    entry.buf.base = munit_malloc(1);
    entry.buf.len = 1;

    ((char *)entry.buf.base)[0] = 'x';

    request->type = RAFT_IO_WRITE_LOG;
    request->args.write_log.entries = &entry;
    request->args.write_log.n = 1;

    __submit(f, request_id);

    __wait(f, request_id);

    raft_io_queue__pop(&f->queue, request_id);

    free(entry.buf.base);

    f->io.stop(&f->io, __stop);
    unsigned i;
    /* Spin a few times to trigger pending callbacks. */
    for (i = 0; i < __MAX_LOOP_RUN; i++) {
        rv = uv_run(&f->loop, UV_RUN_ONCE);
        if (rv == 0) {
            break;
        }
    }

    if (i == __MAX_LOOP_RUN) {
        munit_error("unclean loop");
    }

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

MunitSuite raft_io_uv_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/start", start_tests, NULL, 1, 0},
    {"/submit", submit_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
