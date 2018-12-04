#include "../../include/raft.h"

#include "../../src/io_queue.h"

#include "../lib/fs.h"
#include "../lib/fsm.h"
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
    struct raft_logger logger;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
};

/**
 * Push a new request to the I/O queue.
 */
#define __push_io_request(F, ID, REQUEST)             \
    {                                                 \
        int rv;                                       \
                                                      \
        rv = raft_io_queue__push_(&F->raft, ID);      \
        munit_assert_int(rv, ==, 0);                  \
                                                      \
        *REQUEST = raft_io_queue_get_(&F->raft, *ID); \
        munit_assert_ptr_not_null(*REQUEST);          \
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

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    const uint64_t id = 1;
    int rv;

    (void)user_data;

    f->dir = test_dir_setup(params);

    rv = uv_loop_init(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, id);
    test_fsm_setup(params, &f->fsm);

    raft_init(&f->raft, &f->io, &f->fsm, f, id);

    rv = raft_io_uv_init(&f->raft, &f->loop, f->dir);
    munit_assert_int(rv, ==, 0);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    raft_close(&f->raft);

    test_fsm_tear_down(&f->fsm);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    rv = uv_loop_close(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_dir_tear_down(f->dir);

    free(f);
}

/**
 * raft_uv_init
 */

static MunitResult test_init_dir_too_long(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    char dir[1024];

    memset(dir, 'a', sizeof dir - 1);
    dir[sizeof dir - 1] = 0;

    rv = raft_io_uv_init(&f->raft, &f->loop, dir);
    munit_assert_int(rv, ==, RAFT_ERR_PATH_TOO_LONG);

    munit_assert_string_equal(raft_errmsg(&f->raft),
                              "file system path is too long");

    return MUNIT_OK;
}

static MunitTest init_tests[] = {
    {"/dir-too-long", test_init_dir_too_long, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_submit
 */

static MunitResult test_submit_read_state(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    raft_io_queue__pop_(&f->raft, request_id);

    return MUNIT_OK;
}

static MunitTest submit_tests[] = {
    {"/read-state", test_submit_read_state, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/submit", submit_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
