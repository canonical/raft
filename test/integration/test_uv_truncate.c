#include "../lib/runner.h"
#include "../lib/uv_.h"

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
    bool appended;
};

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV_DEPS;
    SETUP_UV;
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
    struct fixture *f = req->data;
    munit_assert_int(status, ==, 0);
    f->appended = true;
}

/* Append N entries to the log. */
#define APPEND(N)                                                         \
    {                                                                     \
        struct raft_io_append req_;                                       \
        int i;                                                            \
        int rv_;                                                          \
        struct raft_entry *entries_ = munit_malloc(N * sizeof *entries_); \
        for (i = 0; i < N; i++) {                                         \
            struct raft_entry *entry = &entries_[i];                      \
            entry->term = 1;                                              \
            entry->type = RAFT_COMMAND;                                   \
            entry->buf.base = munit_malloc(8);                            \
            entry->buf.len = 8;                                           \
            *(uint64_t *)entry->buf.base = i + 1;                         \
            entry->batch = NULL;                                          \
        }                                                                 \
        req_.data = f;                                                    \
        rv_ = f->io.append(&f->io, &req_, entries_, N, appendCb);         \
        munit_assert_int(rv_, ==, 0);                                     \
                                                                          \
        for (i = 0; i < 5; i++) {                                         \
            LOOP_RUN(1);                                                  \
            if (f->appended) {                                            \
                break;                                                    \
            }                                                             \
        }                                                                 \
        munit_assert(f->appended);                                        \
        for (i = 0; i < N; i++) {                                         \
            struct raft_entry *entry = &entries_[i];                      \
            free(entry->buf.base);                                        \
        }                                                                 \
        free(entries_);                                                   \
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
    (void)params;

    return MUNIT_SKIP; /* FIXME: flaky on Travis */

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
