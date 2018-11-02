#include "../../include/raft.h"

#include "../../src/election.h"

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
 * raft_election__vote
 */

/* The server requesting the vote has a new term than us. */
static MunitResult test_vote_newer_term(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_request_vote_args args;
    bool granted;
    int rv;

    (void)params;

    test_bootstrap_and_load(&f->raft, 2, 1, 2);

    args.term = 2;
    args.candidate_id = 2;
    args.last_log_index = 1;
    args.last_log_term = 1;

    rv = raft_election__vote(&f->raft, &args, &granted);
    munit_assert_int(rv, ==, 0);

    munit_assert_true(granted);

    return MUNIT_OK;
}

static MunitTest vote_tests[] = {
    {"/newer-term", test_vote_newer_term, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Suite
 */
MunitSuite raft_election_suites[] = {
    {"/vote", vote_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
