#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/byte.h"
#include "../../src/io_uv.h"
#include "../../src/io_uv_load.h"
#include "../../src/queue.h"
#include "../../src/snapshot.h"

TEST_MODULE(io_uv__snapshot);

/**
 * io_uv__snapshot_put
 */

TEST_SUITE(put);

struct put_fixture
{
    IO_UV_FIXTURE
    struct raft_snapshot snapshot;
    struct raft_io_snapshot_put req;
    struct raft_buffer bufs[2];
    bool invoked;
    int status;
    bool appended;
};

static void put_cb(struct raft_io_snapshot_put *req, int status)
{
    struct put_fixture *f = req->data;
    f->invoked = true;
    f->status = status;
    f->appended = false;
}

static bool put_cb_was_invoked(void *data)
{
    struct put_fixture *f = data;
    return f->invoked;
}

TEST_SETUP(put)
{
    struct put_fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->bufs[0].base = raft_malloc(8);
    f->bufs[1].base = raft_malloc(8);
    f->bufs[0].len = 8;
    f->bufs[1].len = 8;
    f->snapshot.index = 8;
    f->snapshot.term = 3;
    f->snapshot.configuration_index = 2;
    f->snapshot.bufs = f->bufs;
    f->snapshot.n_bufs = 2;
    raft_configuration_init(&f->snapshot.configuration);
    rv = raft_configuration_add(&f->snapshot.configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);
    f->req.data = f;
    f->invoked = false;
    f->status = -1;
    return f;
}

TEST_TEAR_DOWN(put)
{
    struct put_fixture *f = data;
    raft_configuration_close(&f->snapshot.configuration);
    raft_free(f->bufs[0].base);
    raft_free(f->bufs[1].base);
    IO_UV_TEAR_DOWN;
}

static void append_cb(void *data, int status)
{
    struct put_fixture *f = data;
    munit_assert_int(status, ==, 0);
    f->appended = true;
}

/* Append N entries to the log. */
#define append(N)                                                       \
    {                                                                   \
        int rv;                                                         \
        int i;                                                          \
        struct raft_entry *entries = munit_malloc(N * sizeof *entries); \
        for (i = 0; i < N; i++) {                                       \
            struct raft_entry *entry = &entries[i];                     \
            entry->term = 1;                                            \
            entry->type = RAFT_COMMAND;                             \
            entry->buf.base = munit_malloc(8);                          \
            entry->buf.len = 8;                                         \
            entry->batch = NULL;                                        \
        }                                                               \
        rv = f->io.append(&f->io, entries, N, f, append_cb);            \
        munit_assert_int(rv, ==, 0);                                    \
                                                                        \
        for (i = 0; i < 5; i++) {                                       \
            test_uv_run(&f->loop, 1);                                   \
            if (f->appended) {                                          \
                break;                                                  \
            }                                                           \
        }                                                               \
        munit_assert(f->appended);                                      \
        for (i = 0; i < N; i++) {                                       \
            struct raft_entry *entry = &entries[i];                     \
            free(entry->buf.base);                                      \
        }                                                               \
        free(entries);                                                  \
    }

/* Submit a request to truncate the log at N */
#define truncate(N)                     \
    {                                   \
        int rv;                         \
        rv = f->io.truncate(&f->io, N); \
        munit_assert_int(rv, ==, 0);    \
    }

/* Invoke the snapshot_put method and check that it returns the given code. */
#define put__invoke(RV)                                                 \
    {                                                                   \
        int rv;                                                         \
        rv = f->io.snapshot_put(&f->io, &f->req, &f->snapshot, put_cb); \
        munit_assert_int(rv, ==, RV);                                   \
    }

#define put__wait_cb(STATUS) test_uv_run_until(&f->loop, f, put_cb_was_invoked);

/* Put the first snapshot. */
TEST_CASE(put, first, NULL)
{
    struct put_fixture *f = data;
    struct io_uv__snapshot_meta *snapshots;
    size_t n_snapshots;
    struct io_uv__segment_meta *segments;
    size_t n_segments;
    struct raft_snapshot snapshot;
    int rv;

    (void)params;

    put__invoke(0);
    put__wait_cb(0);

    munit_assert_true(RAFT__QUEUE_IS_EMPTY(&f->uv->snapshot_put_reqs));

    rv = io_uv__load_list(f->uv, &snapshots, &n_snapshots, &segments,
                          &n_segments);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n_snapshots, ==, 1);
    munit_assert_int(snapshots[0].index, ==, 8);
    munit_assert_int(snapshots[0].term, ==, 3);

    rv = io_uv__load_snapshot(f->uv, &snapshots[0], &snapshot);
    munit_assert_int(rv, ==, 0);

    raft_snapshot__close(&snapshot);

    raft_free(snapshots);

    return MUNIT_OK;
}

/* Request to install a snapshot right after a truncation request. */
TEST_CASE(put, after_truncate, NULL)
{
    struct put_fixture *f = data;

    (void)params;

    append(4);
    truncate(1);

    put__invoke(0);
    put__wait_cb(0);

    munit_assert_true(RAFT__QUEUE_IS_EMPTY(&f->uv->snapshot_put_reqs));

    return MUNIT_OK;
}

/**
 * io_uv__snapshot_get
 */

TEST_SUITE(get);

struct get_fixture
{
    IO_UV_FIXTURE
    struct raft_io_snapshot_get req;
    bool invoked;
    int status;
    struct raft_snapshot *snapshot;
};

static void get_cb(struct raft_io_snapshot_get *req,
                   struct raft_snapshot *snapshot,
                   int status)
{
    struct get_fixture *f = req->data;
    f->invoked = true;
    f->status = status;
    f->snapshot = snapshot;
}

static bool get_cb_was_invoked(void *data)
{
    struct get_fixture *f = data;
    return f->invoked;
}

TEST_SETUP(get)
{
    struct get_fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->req.data = f;
    f->invoked = false;
    f->status = -1;
    f->snapshot = NULL;
    return f;
}

TEST_TEAR_DOWN(get)
{
    struct get_fixture *f = data;
    if (f->snapshot != NULL) {
        raft_snapshot__close(f->snapshot);
        raft_free(f->snapshot);
    }
    IO_UV_TEAR_DOWN;
}

/* Write a snapshot file */
#define get__write_snapshot                                       \
    uint8_t buf[8];                                               \
    void *cursor = buf;                                           \
    byte__put64(&cursor, 666);                                    \
    test_io_uv_write_snapshot_meta_file(f->dir, 3, 8, 123, 1, 1); \
    test_io_uv_write_snapshot_data_file(f->dir, 3, 8, 123, buf, sizeof buf)

/* Invoke the snapshot_get method and check that it returns the given code. */
#define get__invoke(RV)                                   \
    {                                                     \
        int rv;                                           \
        rv = f->io.snapshot_get(&f->io, &f->req, get_cb); \
        munit_assert_int(rv, ==, RV);                     \
    }

#define get__wait_cb(STATUS) test_uv_run_until(&f->loop, f, get_cb_was_invoked);

TEST_CASE(get, first, NULL)
{
    struct get_fixture *f = data;

    (void)params;

    get__write_snapshot;
    get__invoke(0);
    get__wait_cb(0);

    munit_assert_true(RAFT__QUEUE_IS_EMPTY(&f->uv->snapshot_get_reqs));

    munit_assert_ptr_not_null(f->snapshot);
    munit_assert_int(f->snapshot->term, ==, 3);
    munit_assert_int(f->snapshot->index, ==, 8);
    munit_assert_int(f->snapshot->n_bufs, ==, 1);
    munit_assert_int(byte__flip64(*(uint64_t *)f->snapshot->bufs[0].base), ==,
                     666);

    return MUNIT_OK;
}
