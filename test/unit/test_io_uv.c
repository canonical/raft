#include "../../include/raft.h"

#include "../../src/io_queue.h"

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    char *dir;
    struct uv_loop_s loop;
    struct raft_io_queue queue;
    struct raft_heap heap;
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

    f->dir = test_dir_setup(params);

    rv = uv_loop_init(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_heap_setup(params, &f->heap);

    raft_io_queue__init(&f->queue);

    rv = raft_io_uv_init(&f->io, &f->loop, f->dir);
    munit_assert_int(rv, ==, 0);

    f->io.init(&f->io, &f->queue, f, __tick, __notify);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    int rv;

    f->io.close(&f->io);

    raft_io_queue__close(&f->queue);

    test_heap_tear_down(&f->heap);

    rv = uv_loop_close(&f->loop);
    munit_assert_int(rv, ==, 0);

    test_dir_tear_down(f->dir);

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
#define __submit(F, ID)                                              \
    {                                                                \
        int rv;                                                      \
                                                                     \
        rv = F->io.submit(&F->io, ID);                               \
        if (rv != 0) {                                               \
            munit_logf(MUNIT_LOG_ERROR, "submit: %s", F->io.errmsg); \
        }                                                            \
    }

/**
 * raft_uv_init
 */

/* Data directory path is too long */
static MunitResult test_init_dir_too_long(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    char dir[1024];

    memset(dir, 'a', sizeof dir - 1);
    dir[sizeof dir - 1] = 0;

    rv = raft_io_uv_init(&io, &f->loop, dir);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(io.errmsg,
                              "data directory exceeds 895 characters");

    return MUNIT_OK;
}

/* Can't create data directory */
static MunitResult test_init_cant_create_dir(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    const char *dir = "/non/existing/path";

    rv = raft_io_uv_init(&io, &f->loop, dir);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(
        io.errmsg,
        "can't create data directory '/non/existing/path': "
        "No such file or directory");

    return MUNIT_OK;
}

/* Data directory not a directory */
static MunitResult test_init_not_a_dir(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    const char *dir = "/dev/null";

    rv = raft_io_uv_init(&io, &f->loop, dir);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(io.errmsg, "path '/dev/null' is not a directory");
    return MUNIT_OK;
}

/* Data directory not accessible */
static MunitResult test_init_access_error(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;

    (void)params;

    const char *dir = "/root/foo";

    rv = raft_io_uv_init(&io, &f->loop, dir);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(
        io.errmsg,
        "can't access data directory '/root/foo': Permission denied");
    return MUNIT_OK;
}

static char *init_oom_heap_fault_delay[] = {"0", "1,", NULL};
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
static MunitResult test_init_create_dir(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_io io;
    int rv;
    struct stat sb;

    (void)params;

    char dir[1024];

    sprintf(dir, "%s/sub/", f->dir);

    rv = raft_io_uv_init(&io, &f->loop, dir);
    munit_assert_int(rv, ==, 0);

    rv = stat(dir, &sb);
    munit_assert_int(rv, ==, 0);

    munit_assert_true((sb.st_mode & S_IFMT) == S_IFDIR);

    io.close(&io);

    return MUNIT_OK;
}

static MunitTest init_tests[] = {
    {"/dir-too-long", test_init_dir_too_long, setup, tear_down, 0, NULL},
    {"/cant-create-dir", test_init_cant_create_dir, setup, tear_down, 0, NULL},
    {"/not-a-dir", test_init_not_a_dir, setup, tear_down, 0, NULL},
    {"/access-error", test_init_access_error, setup, tear_down, 0, NULL},
    {"/oom", test_init_oom, setup, tear_down, 0, init_oom_params},
    {"/create-dir", test_init_create_dir, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_uv_submit
 */

/* The data directory is empty */
static MunitResult test_submit_read_state_empty(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    munit_assert_int(request->result.read_state.term, ==, 0);
    munit_assert_int(request->result.read_state.voted_for, ==, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The data directory has a single metadata1 file */
static MunitResult test_submit_read_state_metadata1(
    const MunitParameter params[],
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

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

static MunitTest submit_tests[] = {
    {"/read-state-empty", test_submit_read_state_empty, setup, tear_down, 0,
     NULL},
    {"/read-state-metadata1", test_submit_read_state_metadata1, setup,
     tear_down, 0, NULL},
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
