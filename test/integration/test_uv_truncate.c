#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture
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
    do {                                                    \
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
    } while (0)

/* Submit an append request identified by I, with N_ENTRIES entries, each one of
 * size ENTRY_SIZE). */
#define APPEND_SUBMIT(I, N_ENTRIES, ENTRY_SIZE)                     \
    struct raft_io_append _req##I;                                  \
    struct result _result##I = {0, false};                          \
    int _rv##I;                                                     \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                              \
    _req##I.data = &_result##I;                                     \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, \
                          appendCbAssertResult);                    \
    munit_assert_int(_rv##I, ==, 0)

/* Submit an append request and wait for it to successfully complete. */
#define APPEND(N)                       \
    do {                                \
        APPEND_SUBMIT(0, N, 8);         \
        LOOP_RUN_UNTIL(&_result0.done); \
    } while (0)

#define TRUNCATE(N, RV)                  \
    do {                                 \
        int rv_;                         \
        rv_ = f->io.truncate(&f->io, N); \
        munit_assert_int(rv_, ==, RV);   \
    } while (0)

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
    f->count = 0;
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Shutdown the fixture's raft_io instance, then load all entries on disk using
 * a new raft_io instance, and assert that there are N entries with data
 * starting at DATA and a total size of TOTAL_DATA_SIZE bytes. */
#define ASSERT_ENTRIES(N, DATA, TOTAL_DATA_SIZE)                  \
    TEAR_DOWN_UV;                                                 \
    do {                                                          \
        struct uv_loop_s _loop;                                   \
        struct raft_uv_transport _transport;                      \
        struct raft_io _io;                                       \
        struct raft_tracer _tracer;                               \
        raft_term _term;                                          \
        unsigned _voted_for;                                      \
        struct raft_snapshot *_snapshot;                          \
        raft_index _start_index;                                  \
        struct raft_entry *_entries;                              \
        size_t _i;                                                \
        size_t _n;                                                \
        void *_batch = NULL;                                      \
        size_t _total_data_size = 0;                              \
        int _rv;                                                  \
                                                                  \
        _rv = uv_loop_init(&_loop);                               \
        munit_assert_int(_rv, ==, 0);                             \
        _rv = raft_uv_tcp_init(&_transport, &_loop);              \
        munit_assert_int(_rv, ==, 0);                             \
        _rv = raft_uv_init(&_io, &_loop, f->dir, &_transport);    \
        munit_assert_int(_rv, ==, 0);                             \
        _tracer.emit = test_tracer_emit;                          \
        raft_uv_set_tracer(&_io, &_tracer);                       \
        _rv = _io.init(&_io, 1, "1");                             \
        munit_assert_int(_rv, ==, 0);                             \
        _rv = _io.load(&_io, 10, &_term, &_voted_for, &_snapshot, \
                       &_start_index, &_entries, &_n);            \
        munit_assert_int(_rv, ==, 0);                             \
        _io.close(&_io, NULL);                                    \
        uv_run(&_loop, UV_RUN_NOWAIT);                            \
        raft_uv_close(&_io);                                      \
        raft_uv_tcp_close(&_transport);                           \
        uv_loop_close(&_loop);                                    \
                                                                  \
        munit_assert_ptr_null(_snapshot);                         \
        munit_assert_int(_n, ==, N);                              \
        for (_i = 0; _i < _n; _i++) {                             \
            struct raft_entry *_entry = &_entries[_i];            \
            uint64_t _value = *(uint64_t *)_entry->buf.base;      \
            munit_assert_int(_entry->term, ==, 1);                \
            munit_assert_int(_entry->type, ==, RAFT_COMMAND);     \
            munit_assert_int(_value, ==, DATA + _i);              \
            munit_assert_ptr_not_null(_entry->batch);             \
        }                                                         \
        for (_i = 0; _i < _n; _i++) {                             \
            struct raft_entry *_entry = &_entries[_i];            \
            if (_entry->batch != _batch) {                        \
                _batch = _entry->batch;                           \
                raft_free(_batch);                                \
            }                                                     \
            _total_data_size += _entry->buf.len;                  \
        }                                                         \
        raft_free(_entries);                                      \
        munit_assert_int(_total_data_size, ==, TOTAL_DATA_SIZE);  \
    } while (0);

/******************************************************************************
 *
 * raft_io->truncate()
 *
 *****************************************************************************/

SUITE(truncate)

/* If the index to truncate is at the start of a segment, that segment and all
 * subsequent ones are removed. */
TEST(truncate, wholeSegment, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    APPEND(3);
    TRUNCATE(1, 0);
    APPEND(1);
    ASSERT_ENTRIES(1 /* n entries */, 3 /* initial data */, 8 /* total size */);
    return MUNIT_OK;
}

/* The index to truncate is the same as the last appended entry. */
TEST(truncate, sameAsLastIndex, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    (void)params;

    return MUNIT_SKIP; /* FIXME: flaky on Travis */

    APPEND(3);
    TRUNCATE(3, 0);
    LOOP_RUN(2);

    if (test_dir_has_file(f->dir, "1-3")) {
        /* Run one more iteration */
        LOOP_RUN(1);
    }

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    return MUNIT_OK;
}

/* If the index to truncate is not at the start of a segment, that segment gets
 * truncated. */
TEST(truncate, partialSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    raft_term term;
    unsigned voted_for;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n;
    int rv;

    (void)params;

    return MUNIT_SKIP; /* FIXME: flaky on Travis */

    APPEND(3);
    APPEND(1);
    TRUNCATE(2, 0);
    LOOP_RUN(3);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    rv = f->io.load(&f->io, 10, &term, &voted_for, &snapshot, &start_index,
                    &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n, ==, 1);
    munit_assert_int(*(uint64_t *)entries[0].buf.base, ==, 1);

    raft_free(entries[0].batch);
    raft_free(entries);

    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions */
TEST(truncate, oom, setUp, tearDown, 0, error_oom_params)
{
    struct fixture *f = data;
    (void)params;
    test_heap_fault_enable(&f->heap);
    return MUNIT_OK;
}
