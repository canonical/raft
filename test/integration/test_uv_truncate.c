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
    bool appended;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
    f->count = 0;
    f->appended = false;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    TEAR_DOWN_UV_DEPS;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static void appendCb(struct raft_io_append *req, int status)
{
    bool *done = req->data;
    munit_assert_int(status, ==, 0);
    *done = true;
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
 * size ENTRY_SIZE). */
#define APPEND_SUBMIT(I, N_ENTRIES, ENTRY_SIZE)                                \
    struct raft_io_append _req##I;                                             \
    int _rv##I;                                                                \
    ENTRIES(I, N_ENTRIES, ENTRY_SIZE);                                         \
    _req##I.data = &f->appended;                                               \
    _rv##I = f->io.append(&f->io, &_req##I, _entries##I, N_ENTRIES, appendCb); \
    munit_assert_int(_rv##I, ==, 0)

/* Submit an append request and wait for it to successfully complete. */
#define APPEND(N)                     \
    {                                 \
        APPEND_SUBMIT(0, N, 8);       \
        LOOP_RUN_UNTIL(&f->appended); \
    }

#define TRUNCATE(N, RV)                  \
    {                                    \
        int rv_;                         \
        rv_ = f->io.truncate(&f->io, N); \
        munit_assert_int(rv_, ==, RV);   \
    }

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

SUITE(truncate)

/* If the index to truncate is at the start of a segment, that segment and all
 * subsequent ones are removed. */
TEST(truncate, wholeSegment, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;

    APPEND(3);
    TRUNCATE(1, 0);
    LOOP_RUN(2);

    if (test_dir_has_file(f->dir, "1-3")) {
        /* Run one more iteration */
        LOOP_RUN(1);
    }

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

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
