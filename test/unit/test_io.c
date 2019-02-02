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

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    uint64_t id = 1;
    const char *address = "1";

    (void)user_data;

    test_heap_setup(params, &f->heap);

    test_logger_setup(params, &f->logger, id);

    test_io_setup(params, &f->io, &f->logger);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->logger, &f->io, &f->fsm, f, id, address);

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
 * raft_handle_io
 */

/* Once the log write is complete, the commit index is updated to match the
 * leader one. */
static MunitResult test_handle_update_commit(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    struct test_io_request event;
    struct raft_entry *entry = raft_malloc(sizeof *entry);
    struct raft_append_entries args;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    /* Include a log entry in the request */
    entry->type = RAFT_LOG_COMMAND;
    entry->term = 1;

    test_fsm_encode_set_x(123, &entry->buf);

    args.term = 1;
    args.leader_id = 2;
    args.prev_log_index = 1;
    args.prev_log_term = 1;
    args.entries = entry;
    args.n_entries = 1;
    args.leader_commit = 2;

    rv = raft_handle_append_entries(&f->raft, 2, "2", &args);
    munit_assert_int(rv, ==, 0);

    /* Notify the raft instance about the completed write. */
    test_io_get_one_request(f->raft.io, RAFT_IO_WRITE_LOG, &event);
    test_io_flush(f->raft.io);

    //rv = raft_handle_io(&f->raft, 0, event.id);
    munit_assert_int(rv, ==, 0);

    /* The commit index has been bumped. */
    munit_assert_int(f->raft.commit_index, ==, 2);

    return MUNIT_OK;
}

static MunitTest handle_tests[] = {
    {"/update-commit", test_handle_update_commit, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_suites[] = {
    {"/handle", handle_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
