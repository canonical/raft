#include "../lib/runner.h"
#include "../lib/uv.h"

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/* This block size should work fine for all file systems. */
#define SEGMENT_BLOCK_SIZE 4096

/* Default segment size */
#define SEGMENT_SIZE 4096 * MAX_SEGMENT_BLOCKS

/******************************************************************************
 *
 * Fixture with a libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    int count; /* To generate deterministic entry data */
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

static void appendCbAssertResult(struct raft_io_append *req, int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    result->done = true;
}

/* Declare and fill the entries array for the append request identified by
 * I. The array will have N entries, and each entry will have a data buffer of
 * SIZE bytes.*/
#define ENTRIES(I, N, SIZE)                                 \
    struct raft_entry _entries##I[N];                       \
    uint8_t _entries_data##I[N * SIZE];                     \
    {                                                       \
        int _i;                                             \
        for (_i = 0; _i < N; _i++) {                        \
            struct raft_entry *entry = &_entries##I[_i];    \
            entry->term = 1;                                \
            entry->type = RAFT_COMMAND;                     \
            entry->buf.base = &_entries_data##I[_i * SIZE]; \
            entry->buf.len = SIZE;                          \
            entry->batch = NULL;                            \
            munit_assert_ptr_not_null(entry->buf.base);     \
            memset(entry->buf.base, 0, entry->buf.len);     \
            *(uint64_t *)entry->buf.base = f->count;        \
            f->count++;                                     \
        }                                                   \
    }

/* Submit an append request identified by I, with N_ENTRIES entries, each one of
 * size ENTRY_SIZE. The default expectation is for the operation to succeed. A
 * custom STATUS can be set with APPEND_EXPECT. */
#define APPEND_SUBMIT(I, N_ENTRIES, ENTRY_SIZE)                     \
    struct raft_io_append _req##I;                                  \
    struct result _result##I = {0, false};                          \
    int _rv##I;                                                     \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                              \
    _req##I.data = &_result##I;                                     \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, \
                          appendCbAssertResult);                    \
    munit_assert_int(_rv##I, ==, 0)

/* Try to submit an append request and assert that the given error code and
 * message are returned. */
#define APPEND_ERROR(N_ENTRIES, ENTRY_SIZE, RV, ERRMSG)                \
    do {                                                               \
        struct raft_io_append _req;                                    \
        int _rv;                                                       \
        ENTRIES(0, N_ENTRIES, ENTRY_SIZE);                             \
        _rv = f->io.append(&f->io, &_req, _entries0, N_ENTRIES, NULL); \
        munit_assert_int(_rv, ==, RV);                                 \
        /* munit_assert_string_equal(f->io.errmsg, ERRMSG);*/          \
    } while (0)

#define APPEND_EXPECT(I, STATUS) _result##I.status = STATUS

/* Wait for the append request identified by I to complete. */
#define APPEND_WAIT(I) LOOP_RUN_UNTIL(&_result##I.done)

/* Submit an append request with an entries array with N_ENTRIES entries, each
 * one of size ENTRY_SIZE, and wait for the operation to successfully
 * complete. */
#define APPEND(N_ENTRIES, ENTRY_SIZE)            \
    do {                                         \
        APPEND_SUBMIT(0, N_ENTRIES, ENTRY_SIZE); \
        APPEND_WAIT(0);                          \
    } while (0)

/* Submit an append request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define APPEND_FAILURE(N_ENTRIES, ENTRY_SIZE, STATUS, ERRMSG) \
    {                                                         \
        APPEND_SUBMIT(0, N_ENTRIES, ENTRY_SIZE);              \
        APPEND_EXPECT(0, STATUS);                             \
        APPEND_WAIT(0);                                       \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);      \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    raft_uv_set_block_size(&f->io, SEGMENT_BLOCK_SIZE);
    raft_uv_set_segment_size(&f->io, SEGMENT_SIZE);
    f->count = 0;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    if (f == NULL) {
        return;
    }
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Shutdown the fixture's raft_io instance, then load all entries on disk using
 * a new raft_io instance, and assert that there are N entries with a total data
 * size of TOTAL_DATA_SIZE bytes. */
#define ASSERT_ENTRIES(N, TOTAL_DATA_SIZE)                                   \
    TEAR_DOWN_UV;                                                            \
    do {                                                                     \
        struct uv_loop_s _loop;                                              \
        struct raft_uv_transport _transport;                                 \
        struct raft_io _io;                                                  \
        raft_term _term;                                                     \
        raft_id _voted_for;                                                  \
        struct raft_snapshot *_snapshot;                                     \
        raft_index _start_index;                                             \
        struct raft_entry *_entries;                                         \
        size_t _i;                                                           \
        size_t _n;                                                           \
        void *_batch = NULL;                                                 \
        size_t _total_data_size = 0;                                         \
        int _rv;                                                             \
                                                                             \
        _rv = uv_loop_init(&_loop);                                          \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = raft_uv_tcp_init(&_transport, &_loop);                         \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = raft_uv_init(&_io, &_loop, f->dir, &_transport);               \
        munit_assert_int(_rv, ==, 0);                                        \
        _rv = _io.init(&_io, 1, "1");                                        \
        if (_rv != 0) {                                                      \
            munit_errorf("io->init(): %s (%d)", _io.errmsg, _rv);            \
        }                                                                    \
        _rv = _io.load(&_io, &_term, &_voted_for, &_snapshot, &_start_index, \
                       &_entries, &_n);                                      \
        if (_rv != 0) {                                                      \
            munit_errorf("io->load(): %s (%d)", _io.errmsg, _rv);            \
        }                                                                    \
        _io.close(&_io, NULL);                                               \
        uv_run(&_loop, UV_RUN_NOWAIT);                                       \
        raft_uv_close(&_io);                                                 \
        raft_uv_tcp_close(&_transport);                                      \
        uv_loop_close(&_loop);                                               \
                                                                             \
        munit_assert_ptr_null(_snapshot);                                    \
        munit_assert_int(_n, ==, N);                                         \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            uint64_t _value = *(uint64_t *)_entry->buf.base;                 \
            munit_assert_int(_entry->term, ==, 1);                           \
            munit_assert_int(_entry->type, ==, RAFT_COMMAND);                \
            munit_assert_int(_value, ==, _i);                                \
            munit_assert_ptr_not_null(_entry->batch);                        \
        }                                                                    \
        for (_i = 0; _i < _n; _i++) {                                        \
            struct raft_entry *_entry = &_entries[_i];                       \
            if (_entry->batch != _batch) {                                   \
                _batch = _entry->batch;                                      \
                raft_free(_batch);                                           \
            }                                                                \
            _total_data_size += _entry->buf.len;                             \
        }                                                                    \
        raft_free(_entries);                                                 \
        munit_assert_int(_total_data_size, ==, TOTAL_DATA_SIZE);             \
    } while (0);

/******************************************************************************
 *
 * raft_io->append()
 *
 *****************************************************************************/

SUITE(append)

/* Append the very first batch of entries. */
TEST(append, first, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    ASSERT_ENTRIES(1, 64);
    return MUNIT_OK;
}

/* As soon as the backend starts writing the first open segment, a second one
 * and a third one get prepared. */
TEST(append, prepareSegments, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    while (!test_dir_has_file(f->dir, "open-3")) {
        LOOP_RUN(1);
    }
    munit_assert_true(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-2"));
    munit_assert_true(test_dir_has_file(f->dir, "open-3"));
    return MUNIT_OK;
}

/* Once the first segment fills up, it gets finalized, and an additional one
 * gets prepared, to maintain the available segments pool size. */
TEST(append, finalizeSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND(1, 64);
    while (!test_dir_has_file(f->dir, "open-4")) {
        LOOP_RUN(1);
    }
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000004"));
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "open-4"));
    return MUNIT_OK;
}

/* The very first batch of entries to append is bigger than the regular open
 * segment size. */
TEST(append, firstBig, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    ASSERT_ENTRIES(MAX_SEGMENT_BLOCKS, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* The second batch of entries to append is bigger than the regular open
 * segment size. */
TEST(append, secondBig, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* Schedule multiple appends each one exceeding the segment size. */
TEST(append, severalBig, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(1, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(2, 2, MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    ASSERT_ENTRIES(6, 6 * MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE);
    return MUNIT_OK;
}

/* Write the very first entry and then another one, both fitting in the same
 * block. */
TEST(append, fitBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    APPEND(1, 64);
    ASSERT_ENTRIES(2, 128);
    return MUNIT_OK;
}

/* Write an entry that fills the first block exactly and then another one. */
TEST(append, matchBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    size_t size;

    size = SEGMENT_BLOCK_SIZE;
    size -= sizeof(uint64_t) + /* Format */
            sizeof(uint64_t) + /* Checksums */
            8 + 16;            /* Header */

    APPEND(1, size);
    APPEND(1, 64);

    ASSERT_ENTRIES(2, size + 64);

    return MUNIT_OK;
}

/* Write an entry that exceeds the first block, then another one that fits in
 * the second block, then a third one that fills the rest of the second block
 * plus the whole third block exactly, and finally a fourth entry that fits in
 * the fourth block */
TEST(append, exceedBlock, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    size_t written;
    size_t size1;
    size_t size2;

    size1 = SEGMENT_BLOCK_SIZE;

    APPEND(1, size1);
    APPEND(1, 64);

    written = sizeof(uint64_t) +     /* Format version */
              2 * sizeof(uint32_t) + /* CRC sums of first batch */
              8 + 16 +               /* Header of first batch */
              size1 +                /* Size of first batch */
              2 * sizeof(uint32_t) + /* CRC of second batch */
              8 + 16 +               /* Header of second batch */
              64;                    /* Size of second batch */

    /* Write a third entry that fills the second block exactly */
    size2 = SEGMENT_BLOCK_SIZE - (written % SEGMENT_BLOCK_SIZE);
    size2 -= (2 * sizeof(uint32_t) + 8 + 16);
    size2 += SEGMENT_BLOCK_SIZE;

    APPEND(1, size2);

    /* Write a fourth entry */
    APPEND(1, 64);

    ASSERT_ENTRIES(4, size1 + 64 + size2 + 64);

    return MUNIT_OK;
}

/* If an append request is submitted before the write operation of the previous
 * append request is started, then a single write will be performed for both
 * requests. */
TEST(append, batch, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    APPEND_SUBMIT(1, 1, 64);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    return MUNIT_OK;
}

/* An append request submitted while a write operation is in progress gets
 * executed only when the write completes. */
TEST(append, wait, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    LOOP_RUN(1);
    APPEND_SUBMIT(1, 1, 64);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    return MUNIT_OK;
}

/* Several batches with different size gets appended in fast pace, forcing the
 * segment arena to grow. */
TEST(append, resizeArena, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, 64);
    APPEND_SUBMIT(1, 1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(2, 2, 64);
    APPEND_SUBMIT(3, 1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(4, 1, SEGMENT_BLOCK_SIZE);
    APPEND_WAIT(0);
    APPEND_WAIT(1);
    APPEND_WAIT(2);
    APPEND_WAIT(3);
    APPEND_WAIT(4);
    ASSERT_ENTRIES(7, 64 * 4 + SEGMENT_BLOCK_SIZE * 3);
    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. */
TEST(append, truncate, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    return MUNIT_SKIP; /* FIXME: flaky */

    APPEND(2, 64);

    APPEND_SUBMIT(0, 2, 64);

    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    APPEND_SUBMIT(1, 2, 64);

    APPEND_WAIT(0);
    APPEND_WAIT(1);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. However
 * the backend is closed before the truncation request can be processed. */
TEST(append, truncateClosing, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    int rv;
    APPEND(2, 64);
    APPEND_SUBMIT(0, 2, 64);
    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);
    APPEND_SUBMIT(1, 2, 64);
    APPEND_EXPECT(1, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* A few append requests get queued, however the backend is closed before
 * preparing the second segment completes. */
TEST(append, prepareClosing, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 2, 64);
    LOOP_RUN(1);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The counters of the open segments get increased as they are closed. */
TEST(append, counter, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    size_t size = SEGMENT_BLOCK_SIZE;
    int i;
    for (i = 0; i < 10; i++) {
        APPEND(1, size);
    }
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000003"));
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000004-0000000000000006"));
    munit_assert_true(test_dir_has_file(f->dir, "open-4"));
    return MUNIT_OK;
}

/* If the I/O instance is closed, all pending append requests get canceled. */
TEST(append, cancel, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND_SUBMIT(0, 1, 64);
    APPEND_EXPECT(0, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* The creation of the current open segment fails because there's no space. */
TEST(append, noSpaceUponPrepareCurrent, setUp, tearDown, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if !HAVE_DECL_UV_FS_O_CREAT
    /* This test appears to leak memory on older libuv versions. */
    return MUNIT_SKIP;
#endif
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE * 32768);
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-1: not enough space to allocate 134217728 bytes");
    return MUNIT_OK;
}

/* The creation of a spare open segment fails because there's no space. */
TEST(append, noSpaceUponPrepareSpare, setUp, tearDown, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if !HAVE_DECL_UV_FS_O_CREAT
    /* This test appears to leak memory on older libuv versions. */
    return MUNIT_SKIP;
#endif
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    return MUNIT_SKIP;
#endif
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE * 2);
    test_dir_fill(f->dir, SEGMENT_BLOCK_SIZE * 3);
    APPEND(1, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(0, 1, SEGMENT_BLOCK_SIZE);
    APPEND_EXPECT(0, RAFT_NOSPACE);
    APPEND_WAIT(0);
    return MUNIT_OK;
}

/* The write request fails because there's not enough space. */
TEST(append, noSpaceUponWrite, setUp, tearDownDeps, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if !HAVE_DECL_UV_FS_O_CREAT
    /* This test appears to leak memory on older libuv versions. */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE);
    test_dir_fill(f->dir, SEGMENT_BLOCK_SIZE * 2);
    APPEND(1, 64);
    APPEND_FAILURE(1, (SEGMENT_BLOCK_SIZE + 128), RAFT_NOSPACE,
                   "short write: 4096 bytes instead of 8192");
    test_dir_remove_file(f->dir, ".fill");
    ASSERT_ENTRIES(1, 64);
    return MUNIT_OK;
}

/* A few requests fail because not enough disk space is available. Eventually
 * the space is released and the request succeeds. */
TEST(append, noSpaceResolved, setUp, tearDownDeps, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
#if !HAVE_DECL_UV_FS_O_CREAT
    /* This test appears to leak memory on older libuv versions. */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
#if defined(__powerpc64__)
    /* XXX: fails on ppc64el */
    TEAR_DOWN_UV;
    return MUNIT_SKIP;
#endif
    test_dir_fill(f->dir, SEGMENT_BLOCK_SIZE);
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-1: not enough space to allocate 16384 bytes");
    APPEND_FAILURE(
        1, 64, RAFT_NOSPACE,
        "create segment open-2: not enough space to allocate 16384 bytes");
    test_dir_remove_file(f->dir, ".fill");
    f->count = 0; /* Reset the data counter */
    APPEND(1, 64);
    ASSERT_ENTRIES(1, 64);
    return MUNIT_OK;
}

/* An error occurs while performing a write. */
TEST(append, writeError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    /* FIXME: doesn't fail anymore after
     * https://github.com/CanonicalLtd/raft/pull/49 */
    return MUNIT_SKIP;

    APPEND_SUBMIT(0, 1, 64);
    test_aio_fill(&ctx, 0);
    APPEND_WAIT(0);
    test_aio_destroy(ctx);
    return MUNIT_OK;
}

static char *oomHeapFaultDelay[] = {"1", /* FIXME "2", */ NULL};
static char *oomHeapFaultRepeat[] = {"1", NULL};

static MunitParameterEnum oomParams[] = {
    {TEST_HEAP_FAULT_DELAY, oomHeapFaultDelay},
    {TEST_HEAP_FAULT_REPEAT, oomHeapFaultRepeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(append, oom, setUp, tearDown, 0, oomParams)
{
    struct fixture *f = data;
    HEAP_FAULT_ENABLE;
    APPEND_ERROR(1, 64, RAFT_NOMEM, "");
    return MUNIT_OK;
}

/* The uv instance is closed while a write request is in progress. */
TEST(append, closeDuringWrite, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* TODO: broken */
    return MUNIT_SKIP;

    APPEND_SUBMIT(0, 1, 64);
    LOOP_RUN(1);
    TEAR_DOWN_UV;

    return MUNIT_OK;
}

/* When the backend is closed, all unused open segments get removed. */
TEST(append, removeSegmentUponClose, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(1, 64);
    while (!test_dir_has_file(f->dir, "open-2")) {
        LOOP_RUN(1);
    }
    TEAR_DOWN_UV;
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));
    return MUNIT_OK;
}

/* When the backend is closed, all pending prepare get requests get canceled. */
TEST(append, cancelPrepareRequest, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    /* TODO: find a way to test a prepare request cancelation */
    return MUNIT_SKIP;
    APPEND(MAX_SEGMENT_BLOCKS, SEGMENT_BLOCK_SIZE);
    APPEND_SUBMIT(0, 1, 64);
    APPEND_EXPECT(0, RAFT_CANCELED);
    TEAR_DOWN_UV;
    return MUNIT_OK;
}

/* When the writer gets closed it tells the writer to close the segment that
 * it's currently writing. */
TEST(append, currentSegment, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;

    APPEND(1, 64);

    TEAR_DOWN_UV;

    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000001"));

    return MUNIT_OK;
}

/* The kernel has ran out of available AIO events. */
TEST(append, ioSetupError, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;
    int rv;
    rv = test_aio_fill(&ctx, 0);
    if (rv != 0) {
        return MUNIT_SKIP;
    }
    APPEND_FAILURE(1, 64, RAFT_TOOMANY,
                   "setup writer for open-1: AIO events user limit exceeded");
    return MUNIT_OK;
}
