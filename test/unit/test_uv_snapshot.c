#include "../lib/runner.h"
#include "../lib/uv.h"

#include "../../src/byte.h"
#include "../../src/queue.h"
#include "../../src/snapshot.h"
#include "../../src/uv.h"

TEST_MODULE(uv_snapshot);

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define LOAD(RV)                                            \
    {                                                       \
        int rv;                                             \
        rv = uvSnapshotLoad(f->uv, &f->meta, &f->snapshot); \
        munit_assert_int(rv, ==, RV);                       \
    }

#define WRITE_META                                              \
    UV_WRITE_SNAPSHOT_META(f->dir, f->meta.term, f->meta.index, \
                           f->meta.timestamp, 1, 1)

#define WRITE_DATA                                              \
    UV_WRITE_SNAPSHOT_DATA(f->dir, f->meta.term, f->meta.index, \
                           f->meta.timestamp, &f->data, sizeof f->data)

/******************************************************************************
 *
 * uvSnapshotLoad
 *
 *****************************************************************************/

TEST_SUITE(load);
TEST_GROUP(load, success);
TEST_GROUP(load, error);

struct load__fixture
{
    FIXTURE_UV;
    struct uvSnapshotInfo meta;
    uint64_t data;
    struct raft_snapshot snapshot;
};

TEST_SETUP(load)
{
    struct load__fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_UV;
    f->meta.term = 1;
    f->meta.index = 5;
    f->meta.timestamp = 123;
    sprintf(f->meta.filename, "snapshot-%llu-%llu-%llu.meta", f->meta.term,
            f->meta.index, f->meta.timestamp);
    raft_configuration_init(&f->snapshot.configuration);
    return f;
}

TEST_TEAR_DOWN(load)
{
    struct load__fixture *f = data;
    raft_configuration_close(&f->snapshot.configuration);
    if (f->snapshot.bufs != NULL) {
        raft_free(f->snapshot.bufs[0].base);
        raft_free(f->snapshot.bufs);
    }
    TEAR_DOWN_UV;
    free(f);
}

/* There are no snapshot metadata files */
TEST_CASE(load, success, first, NULL)
{
    struct load__fixture *f = data;
    (void)params;

    f->data = 123;

    WRITE_META;
    WRITE_DATA;
    LOAD(0);

    munit_assert_int(f->snapshot.term, ==, 1);
    munit_assert_int(f->snapshot.index, ==, 5);
    munit_assert_int(f->snapshot.configuration_index, ==, 1);
    munit_assert_int(f->snapshot.configuration.n, ==, 1);
    munit_assert_int(f->snapshot.configuration.servers[0].id, ==, 1);

    munit_assert_string_equal(f->snapshot.configuration.servers[0].address,
                              "1");

    munit_assert_int(f->snapshot.n_bufs, ==, 1);
    munit_assert_int(*(uint64_t *)f->snapshot.bufs[0].base, ==, 123);

    return MUNIT_OK;
}

/* The snapshot metadata file is missing */
TEST_CASE(load, error, no_metadata, NULL)
{
    struct load__fixture *f = data;

    (void)params;

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The snapshot metadata file is shorter than the mandatory header size. */
TEST_CASE(load, error, no_header, NULL)
{
    struct load__fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, f->meta.filename, buf, sizeof buf);
    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The snapshot metadata file has an unexpected format. */
TEST_CASE(load, error, format, NULL)
{
    struct load__fixture *f = data;
    uint64_t format = 666;

    (void)params;

    WRITE_META;

    test_dir_overwrite_file(f->dir, f->meta.filename, &format, sizeof format,
                            0);

    LOAD(RAFT_MALFORMED);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is too big. */
TEST_CASE(load, error, configuration_too_big, NULL)
{
    struct load__fixture *f = data;
    uint64_t size = byteFlip64(2 * 1024 * 1024);

    (void)params;

    WRITE_META;

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is zero. */
TEST_CASE(load, error, no_configuration, NULL)
{
    struct load__fixture *f = data;
    uint64_t size = 0;

    (void)params;

    WRITE_META;

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot data file is missing */
TEST_CASE(load, error, no_data, NULL)
{
    struct load__fixture *f = data;

    (void)params;

    WRITE_META;
    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

static char *load_error_oom_heap_fault_delay[] = {"0", "1", "2",
                                                  "3", "4", NULL};
static char *load_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum load_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, load_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, load_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(load, error, oom, load_error_oom_params)
{
    struct load__fixture *f = data;

    (void)params;

    WRITE_META;
    WRITE_DATA;

    test_heap_fault_enable(&f->heap);

    LOAD(RAFT_NOMEM);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_io->snapshot_put
 *
 *****************************************************************************/

TEST_SUITE(put);

struct put_fixture
{
    FIXTURE_UV;
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
    (void)user_data;
    int rv;
    SETUP_UV;
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
    TEAR_DOWN_UV;
    free(f);
}

static void append_cb(struct raft_io_append *req, int status)
{
    struct put_fixture *f = req->data;
    munit_assert_int(status, ==, 0);
    f->appended = true;
}

/* Append N entries to the log. */
#define append(N)                                                       \
    {                                                                   \
        struct raft_io_append req_;                                     \
        int i;                                                          \
        int rv;                                                         \
        struct raft_entry *entries = munit_malloc(N * sizeof *entries); \
        for (i = 0; i < N; i++) {                                       \
            struct raft_entry *entry = &entries[i];                     \
            entry->term = 1;                                            \
            entry->type = RAFT_COMMAND;                                 \
            entry->buf.base = munit_malloc(8);                          \
            entry->buf.len = 8;                                         \
            entry->batch = NULL;                                        \
        }                                                               \
        req_.data = f;                                                  \
        rv = f->io.append(&f->io, &req_, entries, N, append_cb);        \
        munit_assert_int(rv, ==, 0);                                    \
                                                                        \
        for (i = 0; i < 5; i++) {                                       \
            LOOP_RUN(1);                                                \
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
#define put__invoke(RV)                                                  \
    {                                                                    \
        int rv2;                                                         \
        rv2 = f->io.snapshot_put(&f->io, &f->req, &f->snapshot, put_cb); \
        munit_assert_int(rv2, ==, RV);                                   \
    }

#define put__wait_cb(STATUS) LOOP_RUN_UNTIL(put_cb_was_invoked, f);

/* Put the first snapshot. */
TEST_CASE(put, first, NULL)
{
    struct put_fixture *f = data;
    struct uvSnapshotInfo *snapshots;
    size_t n_snapshots;
    struct uvSegmentInfo *segments;
    size_t n_segments;
    struct raft_snapshot snapshot;
    int rv;

    (void)params;

    put__invoke(0);
    put__wait_cb(0);

    munit_assert_true(QUEUE_IS_EMPTY(&f->uv->snapshot_put_reqs));

    rv = uvList(f->uv, &snapshots, &n_snapshots, &segments, &n_segments);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n_snapshots, ==, 1);
    munit_assert_int(snapshots[0].index, ==, 8);
    munit_assert_int(snapshots[0].term, ==, 3);

    rv = uvSnapshotLoad(f->uv, &snapshots[0], &snapshot);
    munit_assert_int(rv, ==, 0);

    snapshotClose(&snapshot);

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

    munit_assert_true(QUEUE_IS_EMPTY(&f->uv->snapshot_put_reqs));

    return MUNIT_OK;
}

/******************************************************************************
 *
 * raft_io->snapshot_get
 *
 *****************************************************************************/

TEST_SUITE(get);

struct get_fixture
{
    FIXTURE_UV;
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
    (void)user_data;
    SETUP_UV;
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
        snapshotClose(f->snapshot);
        raft_free(f->snapshot);
    }
    TEAR_DOWN_UV;
    free(f);
}

/* Write a snapshot file */
#define get__write_snapshot                          \
    uint8_t buf[8];                                  \
    void *cursor = buf;                              \
    bytePut64(&cursor, 666);                         \
    UV_WRITE_SNAPSHOT_META(f->dir, 3, 8, 123, 1, 1); \
    UV_WRITE_SNAPSHOT_DATA(f->dir, 3, 8, 123, buf, sizeof buf)

/* Invoke the snapshot_get method and check that it returns the given code. */
#define get__invoke(RV)                                   \
    {                                                     \
        int rv;                                           \
        rv = f->io.snapshot_get(&f->io, &f->req, get_cb); \
        munit_assert_int(rv, ==, RV);                     \
    }

#define get__wait_cb(STATUS) LOOP_RUN_UNTIL(get_cb_was_invoked, f);

TEST_CASE(get, first, NULL)
{
    struct get_fixture *f = data;

    (void)params;

    get__write_snapshot;
    get__invoke(0);
    get__wait_cb(0);

    munit_assert_true(QUEUE_IS_EMPTY(&f->uv->snapshot_get_reqs));

    munit_assert_ptr_not_null(f->snapshot);
    munit_assert_int(f->snapshot->term, ==, 3);
    munit_assert_int(f->snapshot->index, ==, 8);
    munit_assert_int(f->snapshot->n_bufs, ==, 1);
    munit_assert_int(byteFlip64(*(uint64_t *)f->snapshot->bufs[0].base), ==,
                     666);

    return MUNIT_OK;
}
