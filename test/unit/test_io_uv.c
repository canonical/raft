#include "../../include/raft.h"

#include "../../src/binary.h"
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
 * Write either the metadata1 or metadata2 file, filling it with the given
 * values.
 */
#define __write_metadata(F, N, FORMAT, VERSION, TERM, VOTED_FOR, FIRST_INDEX) \
    {                                                                         \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                                \
        void *cursor = buf;                                                   \
        char filename[strlen("metadataN") + 1];                               \
                                                                              \
        sprintf(filename, "metadata%d", N);                                   \
                                                                              \
        raft__put64(&cursor, FORMAT);                                         \
        raft__put64(&cursor, VERSION);                                        \
        raft__put64(&cursor, TERM);                                           \
        raft__put64(&cursor, VOTED_FOR);                                      \
        raft__put64(&cursor, FIRST_INDEX);                                    \
                                                                              \
        test_dir_write_file(F->dir, filename, buf, sizeof buf);               \
    }

/**
 * Assert that the content of either the metadata1 or metadata2 file match the
 * given values.
 */
#define __assert_metadata(F, N, FORMAT, VERSION, TERM, VOTED_FOR, FIRST_INDEX) \
    {                                                                          \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                                 \
        void *cursor = buf;                                                    \
        char filename[strlen("metadataN") + 1];                                \
                                                                               \
        sprintf(filename, "metadata%d", N);                                    \
                                                                               \
        test_dir_read_file(F->dir, filename, buf, sizeof buf);                 \
                                                                               \
        munit_assert_int(raft__get64(&cursor), ==, FORMAT);                    \
        munit_assert_int(raft__get64(&cursor), ==, VERSION);                   \
        munit_assert_int(raft__get64(&cursor), ==, TERM);                      \
        munit_assert_int(raft__get64(&cursor), ==, VOTED_FOR);                 \
        munit_assert_int(raft__get64(&cursor), ==, FIRST_INDEX);               \
    }

/**
 * Assert that the metadata in the given RAFT_IO_READ_STATE result equals the
 * given values.
 */
#define __assert_read_state_metadata(R, TERM, VOTED_FOR, FIRST_INDEX)        \
    {                                                                        \
        munit_assert_int(R->result.read_state.term, ==, TERM);               \
        munit_assert_int(R->result.read_state.voted_for, ==, VOTED_FOR);     \
        munit_assert_int(R->result.read_state.first_index, ==, FIRST_INDEX); \
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

    munit_assert_string_equal(io.errmsg,
                              "create data directory '/non/existing/path': "
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
        io.errmsg, "access data directory '/root/foo': Permission denied");
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
 * raft_io_uv__read_state
 */

/* The data directory is empty */
static MunitResult test_read_state_empty(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    __assert_read_state_metadata(request, 0, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The data directory has a single metadata1 file. */
static MunitResult test_read_state_metadata_only_1(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    __assert_read_state_metadata(request, 1, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
static MunitResult test_read_state_metadata_1(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     3, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    __assert_read_state_metadata(request, 3, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
static MunitResult test_read_state_metadata_2(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    __assert_read_state_metadata(request, 2, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. */
static MunitResult test_read_state_metadata_short(const MunitParameter params[],
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

/* The metadata1 file has not the expected format. */
static MunitResult test_read_state_metadata_wrong_format(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    int rv;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     2, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    rv = f->io.submit(&f->io, request_id);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->io.errmsg,
                              "parse metadata1: unknown format 2");

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
static MunitResult test_read_state_metadata_wrong_version(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    int rv;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     0, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    rv = f->io.submit(&f->io, request_id);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->io.errmsg,
                              "parse metadata1: version is set to zero");

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The metadata1 file has not a valid term. */
static MunitResult test_read_state_metadata_wrong_term(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    int rv;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     0, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    rv = f->io.submit(&f->io, request_id);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->io.errmsg,
                              "parse metadata1: term is set to zero");

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
static MunitResult test_read_state_metadata_same_version(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;
    int rv;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    rv = f->io.submit(&f->io, request_id);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->io.errmsg,
                              "metadata1 and metadata2 are both at version 2");

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

static MunitTest read_state_tests[] = {
    {"/empty", test_read_state_empty, setup, tear_down, 0, NULL},
    {"/metadata-only-1", test_read_state_metadata_only_1, setup, tear_down, 0,
     NULL},
    {"/metadata-1", test_read_state_metadata_1, setup, tear_down, 0, NULL},
    {"/metadata-2", test_read_state_metadata_2, setup, tear_down, 0, NULL},
    {"/metadata-short", test_read_state_metadata_short, setup, tear_down, 0,
     NULL},
    {"/metadata-wrong-format", test_read_state_metadata_wrong_format, setup,
     tear_down, 0, NULL},
    {"/metadata-wrong-version", test_read_state_metadata_wrong_version, setup,
     tear_down, 0, NULL},
    {"/metadata-wrong-term", test_read_state_metadata_wrong_term, setup,
     tear_down, 0, NULL},
    {"/metadata-same-version", test_read_state_metadata_same_version, setup,
     tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__write_term
 */

/* The data directory is empty */
static MunitResult test_write_term_empty(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 1;

    __submit(f, request_id);

    __assert_metadata(f, 1, 1, 1, 1, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* After the very first write (which writes into metadata1), metadata2 is
 * written. */
static MunitResult test_write_term_metadata_2(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 1;

    __submit(f, request_id);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 2;

    __submit(f, request_id);

    __assert_metadata(f, 2, 1, 2, 2, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* Write term after a read state request, which updates which is the next
 * metadata file to write. */
static MunitResult test_write_term_after_read_state(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     3, /* Term                                 */
                     1, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    request->type = RAFT_IO_WRITE_TERM;
    request->args.write_term.term = 2;

    __submit(f, request_id);

    __assert_metadata(f, 2, 1, 3, 2, 0, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

static MunitTest write_term_tests[] = {
    {"/empty", test_write_term_empty, setup, tear_down, 0, NULL},
    {"/metadata-2", test_write_term_metadata_2, setup, tear_down, 0, NULL},
    {"/after-read-state", test_write_term_after_read_state, setup, tear_down, 0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv__write_vote
 */

/* The data directory is empty */
static MunitResult test_write_vote_empty(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 1;

    __submit(f, request_id);

    __assert_metadata(f, 1, 1, 1, 0, 1, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* After the very first write (which writes into metadata1), metadata2 is
 * written. */
static MunitResult test_write_vote_metadata_2(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 1;

    __submit(f, request_id);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 2;

    __submit(f, request_id);

    __assert_metadata(f, 2, 1, 2, 0, 2, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

/* Write vote after a read state request, which updates which is the next
 * metadata file to write. */
static MunitResult test_write_vote_after_read_state(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    unsigned request_id;
    struct raft_io_request *request;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __push_io_request(f, &request_id, &request);

    request->type = RAFT_IO_READ_STATE;

    __submit(f, request_id);

    request->type = RAFT_IO_WRITE_VOTE;
    request->args.write_vote.server_id = 2;

    __submit(f, request_id);

    __assert_metadata(f, 2, 1, 3, 3, 2, 0);

    raft_io_queue__pop(&f->queue, request_id);

    return MUNIT_OK;
}

static MunitTest write_vote_tests[] = {
    {"/empty", test_write_vote_empty, setup, tear_down, 0, NULL},
    {"/metadata-2", test_write_vote_metadata_2, setup, tear_down, 0, NULL},
    {"/after-read-state", test_write_vote_after_read_state, setup, tear_down, 0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_suites[] = {
    {"/init", init_tests, NULL, 1, 0},
    {"/read-state", read_state_tests, NULL, 1, 0},
    {"/write-term", write_term_tests, NULL, 1, 0},
    {"/write-vote", write_vote_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
