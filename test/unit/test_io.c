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
 *
 * raft_io__queue_push
 *
 */

/* When the first request is enqueued, the underlying array gets created. */
static MunitResult test_queue_push_first(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    size_t id;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    rv = raft_io__queue_push(&f->raft, &id);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(id, ==, 0);
    munit_assert_int(f->raft.io_queue.size, ==, 2);

    munit_assert_int(f->raft.io_queue.requests[1].type, ==, RAFT_IO_NULL);

    return MUNIT_OK;
}

/* When a second request is enqueued, there's no need to grow the array. */
static MunitResult test_queue_push_second(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_io_request *request;
    size_t id1;
    size_t id2;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    rv = raft_io__queue_push(&f->raft, &id1);
    munit_assert_int(rv, ==, 0);

    /* Mark the request as in use. */
    request = raft_io__queue_get(&f->raft, id1);
    request->type = RAFT_IO_WRITE_LOG;

    /* Now the queue has 2 slots, one is in use an the other not. */
    munit_assert_int(f->raft.io_queue.requests[0].type, ==, RAFT_IO_WRITE_LOG);
    munit_assert_int(f->raft.io_queue.requests[1].type, ==, RAFT_IO_NULL);

    /* Configure the allocator to fail at the next malloc attempt (which won't
     * be made). */
    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    /* A second push is successful */
    rv = raft_io__queue_push(&f->raft, &id2);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(id2, ==, 1);
    munit_assert_int(f->raft.io_queue.size, ==, 2);

    raft_io__queue_pop(&f->raft, id1);

    return MUNIT_OK;
}

/* When third request is enqueued, we need to grow the array again. */
static MunitResult test_queue_push_third(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    struct raft_io_request *request;
    size_t id1;
    size_t id2;
    size_t id3;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Push two requests and mark them as in use. */
    rv = raft_io__queue_push(&f->raft, &id1);
    munit_assert_int(rv, ==, 0);

    request = raft_io__queue_get(&f->raft, id1);
    request->type = RAFT_IO_WRITE_LOG;

    rv = raft_io__queue_push(&f->raft, &id2);
    munit_assert_int(rv, ==, 0);

    request = raft_io__queue_get(&f->raft, id2);
    request->type = RAFT_IO_WRITE_LOG;

    /* Push a third request. */
    rv = raft_io__queue_push(&f->raft, &id3);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(id3, ==, 2);
    munit_assert_int(f->raft.io_queue.size, ==, 6);

    raft_io__queue_pop(&f->raft, id1);
    raft_io__queue_pop(&f->raft, id2);

    return MUNIT_OK;
}

/* Test out of memory conditions. */
static MunitResult test_queue_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    size_t id;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_io__queue_push(&f->raft, &id);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static MunitTest queue_push_tests[] = {
    {"/first", test_queue_push_first, setup, tear_down, 0, NULL},
    {"/second", test_queue_push_second, setup, tear_down, 0, NULL},
    {"/third", test_queue_push_third, setup, tear_down, 0, NULL},
    {"/oom", test_queue_oom, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * raft_io__queue_push
 *
 */

/* When a request is popped, it's slot is marked as free. */
static MunitResult test_queue_pop(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_io_request *request;
    size_t id;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    rv = raft_io__queue_push(&f->raft, &id);
    munit_assert_int(rv, ==, 0);

    request = raft_io__queue_get(&f->raft, id);
    request->type = RAFT_IO_WRITE_LOG;

    raft_io__queue_pop(&f->raft, id);

    /* The size of the queue is still 2, but both requests types are null. */
    munit_assert_int(f->raft.io_queue.size, ==, 2);
    munit_assert_int(f->raft.io_queue.requests[0].type, ==, RAFT_IO_NULL);
    munit_assert_int(f->raft.io_queue.requests[1].type, ==, RAFT_IO_NULL);

    return MUNIT_OK;
}

static MunitTest queue_pop_tests[] = {
    {"/", test_queue_pop, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * raft_handle_write_log
 *
 */

/* Once the log write is complete, the commit index is updated to match the
 * leader one. */
static MunitResult test_update_commit(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct test_io_request event;
    struct raft_entry *entry = raft_malloc(sizeof *entry);
    const struct raft_server *server;
    struct raft_append_entries_args args;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Include a log entry in the request */
    entry->type = RAFT_LOG_COMMAND;
    entry->term = 1;
    entry->buf.base = NULL;
    entry->buf.len = 0;

    server = raft_configuration__get(&f->raft.configuration, 2);

    args.term = 1;
    args.leader_id = server->id;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entry;
    args.n = 1;
    args.leader_commit = 2;

    rv = raft_handle_append_entries(&f->raft, server, &args);
    munit_assert_int(rv, ==, 0);

    /* Notify the raft instance about the completed write. */
    test_io_get_one_request(f->raft.io, RAFT_IO_WRITE_LOG, &event);
    test_io_flush(f->raft.io);

    raft_handle_io(&f->raft, 0, event.id);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 2);

    return MUNIT_OK;
}

static MunitTest handle_write_log_tests[] = {
    {"/update-commit", test_update_commit, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_suites[] = {
    {"/queue-push", queue_push_tests, NULL, 1, 0},
    {"/queue-pop", queue_pop_tests, NULL, 1, 0},
    {"/write_log", handle_write_log_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
