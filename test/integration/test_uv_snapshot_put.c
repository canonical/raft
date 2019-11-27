#include <unistd.h>

#include "../lib/runner.h"
#include "../lib/tcp.h"
#include "../lib/uv_.h"

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    bool closed;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

struct snapshot
{
    raft_term term;
    raft_index index;
    uint64_t data;
    bool done;
};

static void appendCb(struct raft_io_append *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
}

static void snapshotPutCbAssertResult(struct raft_io_snapshot_put *req,
                                      int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

static void snapshotGetCbAssertResult(struct raft_io_snapshot_get *req,
                                      struct raft_snapshot *snapshot,
                                      int status)
{
    struct snapshot *expect = req->data;
    munit_assert_int(status, ==, 0);
    munit_assert_ptr_not_null(snapshot);
    munit_assert_int(snapshot->term, ==, expect->term);
    munit_assert_int(snapshot->index, ==, snapshot->index);
    expect->done = true;
    raft_configuration_close(&snapshot->configuration);
    raft_free(snapshot->bufs[0].base);
    raft_free(snapshot->bufs);
    raft_free(snapshot);
}

/* Submit an append request to append N entries and wait for the operation to
 * successfully complete. */
#define APPEND(N)                                                 \
    do {                                                          \
        struct raft_entry _entries[N];                            \
        uint64_t _entries_data[N];                                \
        int _i;                                                   \
        struct raft_io_append _req;                               \
        bool _done = false;                                       \
        int _rv;                                                  \
        for (_i = 0; _i < N; _i++) {                              \
            struct raft_entry *_entry = &_entries[_i];            \
            _entry->term = 1;                                     \
            _entry->type = RAFT_COMMAND;                          \
            _entry->buf.base = &_entries_data[_i];                \
            _entry->buf.len = sizeof _entries_data[_i];           \
            _entry->batch = NULL;                                 \
        }                                                         \
        _req.data = &_done;                                       \
        _rv = f->io.append(&f->io, &_req, _entries, N, appendCb); \
        munit_assert_int(_rv, ==, 0);                             \
        LOOP_RUN_UNTIL(&_done);                                   \
    } while (0)

/* Submit a request to truncate the log at N */
#define TRUNCATE(N)                      \
    {                                    \
        int _rv;                         \
        _rv = f->io.truncate(&f->io, N); \
        munit_assert_int(_rv, ==, 0);    \
    }

#define SNAPSHOT_PUT_REQ(TRAILING, INDEX, RV, STATUS)                      \
    struct raft_snapshot _snapshot;                                        \
    struct raft_buffer _snapshot_buf;                                      \
    uint64_t _snapshot_data;                                               \
    struct raft_io_snapshot_put _req;                                      \
    struct result _result = {STATUS, false};                               \
    int _rv;                                                               \
    _snapshot.term = 1;                                                    \
    _snapshot.index = INDEX;                                               \
    raft_configuration_init(&_snapshot.configuration);                     \
    _rv = raft_configuration_add(&_snapshot.configuration, 1, "1", false); \
    munit_assert_int(_rv, ==, 0);                                          \
    _snapshot.bufs = &_snapshot_buf;                                       \
    _snapshot.n_bufs = 1;                                                  \
    _snapshot_buf.base = &_snapshot_data;                                  \
    _snapshot_buf.len = sizeof _snapshot_data;                             \
    _req.data = &_result;                                                  \
    _rv = f->io.snapshot_put(&f->io, TRAILING, &_req, &_snapshot,          \
                             snapshotPutCbAssertResult);                   \
    munit_assert_int(_rv, ==, RV)

/* Submit a snapshot put request for the given snapshot and wait for the
 * operation to successfully complete. */
#define SNAPSHOT_PUT(TRAILING, INDEX)                                  \
    do {                                                               \
        SNAPSHOT_PUT_REQ(TRAILING, INDEX, 0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);                                 \
        raft_configuration_close(&_snapshot.configuration);            \
    } while (0)

/* Submit a snapshot put request and assert that it fails synchronously with the
 * given error code and message. */
#define SNAPSHOT_PUT_ERROR(SNAPSHOT, TRAILING, RV, ERRMSG)           \
    do {                                                             \
        SNAPSHOT_PUT_REQ(SNAPSHOT, TRAILING, RV, 0 /* status */);    \
        /* munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    } while (0)

/* Submit a snapshot put request and wait for the operation to fail with the
 * given code and message. */
#define SNAPSHOT_PUT_FAILURE(STATUS, ERRMSG)                        \
    do {                                                            \
        SNAPSHOT_PUT_REQ(0 /* rv */, STATUS);                       \
        LOOP_RUN_UNTIL(&_result.done);                              \
        /*munit_assert_string_equal(f->transport.errmsg, ERRMSG);*/ \
    } while (0)

/* Use raft_io->snapshot_get to load the last snapshot and compare it with the
 * given parameters. */
#define ASSERT_SNAPSHOT(TERM, INDEX, DATA)                                  \
    do {                                                                    \
        struct raft_io_snapshot_get _req;                                   \
        struct snapshot _expect = {TERM, INDEX, DATA, false};               \
        int _rv;                                                            \
        _req.data = &_expect;                                               \
        _rv = f->io.snapshot_get(&f->io, &_req, snapshotGetCbAssertResult); \
        munit_assert_int(_rv, ==, 0);                                       \
        LOOP_RUN_UNTIL(&_expect.done);                                      \
    } while (0)

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    f->io.data = f;
    f->closed = false;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    SETUP_UV;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * raft_io->snapshot_put
 *
 *****************************************************************************/

SUITE(snapshot_put)

/* Put the first snapshot. */
TEST(snapshot_put, first, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    SNAPSHOT_PUT(10, /* trailing */
                 1   /* index */
    );
    ASSERT_SNAPSHOT(1, 1, 1);
    return MUNIT_OK;
}

/* If the number of closed entries is less than the given trailing amount, no
 * segment is deleted. */
TEST(snapshot_put, entriesLessThanTrailing, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned i;
    raft_uv_set_segment_size(
        &f->io, 4096); /* Lower the number of block to force finalizing */

    for (i = 0; i < 40; i++) {
        APPEND(10);
    }

    SNAPSHOT_PUT(128, /* trailing */
                 100  /* index */
    );

    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000150"));
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000151-0000000000000300"));

    return MUNIT_OK;
}

/* If the number of closed entries is greater than the given trailing amount,
 * closed segments that are fully past the trailing amount get deleted. */
TEST(snapshot_put, entriesMoreThanTrailing, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    unsigned i;
    raft_uv_set_segment_size(
        &f->io, 4096); /* Lower the number of block to force finalizing */

    for (i = 0; i < 40; i++) {
        APPEND(10);
    }

    SNAPSHOT_PUT(128, /* trailing */
                 280  /* index */
    );

    munit_assert_false(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000150"));
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000151-0000000000000300"));

    return MUNIT_OK;
}

/* Request to install a snapshot right after a truncation request. */
TEST(snapshot_put, afterTruncate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(4);
    TRUNCATE(1);
    SNAPSHOT_PUT(10, /* trailing */
                 1   /* index */
    );
    return MUNIT_OK;
}