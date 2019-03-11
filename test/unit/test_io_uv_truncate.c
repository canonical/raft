#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/io_uv.h"

TEST_MODULE(io_uv__truncate);

/**
 * Helpers.
 */

struct fixture
{
    IO_UV_FIXTURE;
    bool appended;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->appended = false;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    IO_UV_TEAR_DOWN;
    free(f);
}

static void append_cb(void *data, int status)
{
    struct fixture *f = data;
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
            entry->type = RAFT_LOG_COMMAND;                             \
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

#define invoke(N, RV)                   \
    {                                   \
        int rv;                         \
        rv = f->io.truncate(&f->io, N); \
        munit_assert_int(rv, ==, RV);   \
    }

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* If the index to truncate is at the start of a segment, that segment and all
 * subsequent ones are removed. */
TEST_CASE(success, whole_segment, NULL)
{
    struct fixture *f = data;
    /* TODO: fix timeouts like
     * https://travis-ci.org/CanonicalLtd/raft/jobs/503676478 */
    return MUNIT_SKIP;

    (void)params;

    append(3);
    invoke(1, 0);
    test_uv_run(&f->loop, 3);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    return MUNIT_OK;
}

/* If the index to truncate is not at the start of a segment, that segment gets
 * truncated. */
TEST_CASE(success, partial_segment, NULL)
{
    struct fixture *f = data;
    /* struct raft_snapshot *snapshot; */
    /* struct raft_entry *entries; */
    /* size_t n; */
    /* int rv; */

    (void)params;

    append(3);
    append(1);
    invoke(2, 0);
    test_uv_run(&f->loop, 3);

    munit_assert_false(test_dir_has_file(f->dir, "1-3"));
    munit_assert_false(test_dir_has_file(f->dir, "4-4"));

    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    /*rv = raft__io_uv_loader_load_all(&f->loader, &snapshot, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n, ==, 1);
    munit_assert_int(byte__flip64(*(uint64_t *)entries[0].buf.base), ==, 1);*/

    /* raft_free(entries[0].batch); */
    /* raft_free(entries); */

    return MUNIT_OK;
}

/**
 * Failure scenarios.
 */

TEST_SUITE(error);

TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

static char *error_oom_heap_fault_delay[] = {"0", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions */
TEST_CASE(error, oom, error_oom_params)
{
    struct fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    return MUNIT_OK;
}
