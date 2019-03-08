#include "../../src/log.h"

#include "../lib/heap.h"
#include "../lib/runner.h"

TEST_MODULE(log);

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct raft_log log;
};

/**
 * Append an empty entry to the log and check the result.
 */
#define __append_empty_entry(F)                                          \
    {                                                                    \
        struct raft_buffer buf;                                          \
        int rv;                                                          \
                                                                         \
        buf.base = NULL;                                                 \
        buf.len = 0;                                                     \
                                                                         \
        rv = raft_log__append(&F->log, 1, RAFT_LOG_COMMAND, &buf, NULL); \
        munit_assert_int(rv, ==, 0);                                     \
    }

/**
 * Append an entry with a small payload to the log and check the result.
 */
#define __append_entry(F, TERM)                                             \
    {                                                                       \
        struct raft_buffer buf;                                             \
        int rv;                                                             \
                                                                            \
        buf.base = raft_malloc(8);                                          \
        buf.len = 8;                                                        \
                                                                            \
        strcpy(buf.base, "hello");                                          \
                                                                            \
        rv = raft_log__append(&F->log, TERM, RAFT_LOG_COMMAND, &buf, NULL); \
        munit_assert_int(rv, ==, 0);                                        \
    }

/**
 * Append N entries all belonging to the same batch.
 */
#define __append_batch(F, N)                                                  \
    {                                                                         \
        void *batch;                                                          \
        size_t offset;                                                        \
        int i;                                                                \
                                                                              \
        batch = raft_malloc(8 * N);                                           \
                                                                              \
        munit_assert_ptr_not_null(batch);                                     \
                                                                              \
        offset = 0;                                                           \
                                                                              \
        for (i = 0; i < N; i++) {                                             \
            struct raft_buffer buf;                                           \
            int rv;                                                           \
                                                                              \
            buf.base = batch + offset;                                        \
            buf.len = 8;                                                      \
                                                                              \
            *(uint64_t *)buf.base = i * 1000;                                 \
                                                                              \
            rv = raft_log__append(&F->log, 1, RAFT_LOG_COMMAND, &buf, batch); \
            munit_assert_int(rv, ==, 0);                                      \
                                                                              \
            offset += 8;                                                      \
        }                                                                     \
    }

/**
 * Assert the state of the fixture's log in terms of size, front/back indexes,
 * offset and number of entries.
 */
#define __assert_state(F, SIZE, FRONT, BACK, OFFSET, N)        \
    {                                                          \
        munit_assert_int(f->log.size, ==, SIZE);               \
        munit_assert_int(f->log.front, ==, FRONT);             \
        munit_assert_int(f->log.back, ==, BACK);               \
        munit_assert_int(f->log.offset, ==, OFFSET);           \
        munit_assert_int(raft_log__n_entries(&f->log), ==, N); \
    }

/**
 * Assert the term of the I'th entry in the entries array.
 */
#define __assert_term(F, I, TERM)                           \
    {                                                       \
        munit_assert_int(F->log.entries[I].term, ==, TERM); \
    }

/**
 * Assert the type of the I'th entryin the entries array.
 */
#define __assert_type(F, I, TYPE)                           \
    {                                                       \
        munit_assert_int(F->log.entries[I].type, ==, TYPE); \
    }

/**
 * Assert the number of outstanding references for the entry at the given index.
 */
#define __assert_refcount(F, INDEX, COUNT)                            \
    {                                                                 \
        size_t i;                                                     \
                                                                      \
        munit_assert_ptr_not_null(F->log.refs);                       \
                                                                      \
        for (i = 0; i < F->log.refs_size; i++) {                      \
            if (F->log.refs[i].index == INDEX) {                      \
                munit_assert_int(F->log.refs[i].count, ==, COUNT);    \
                break;                                                \
            }                                                         \
        }                                                             \
                                                                      \
        if (i == F->log.refs_size) {                                  \
            munit_errorf("no refcount found for entry with index %d", \
                         (int)INDEX);                                 \
        }                                                             \
    }

/**
 * Setup and tear down
 */

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;
    (void)params;

    test_heap_setup(params, &f->heap);

    raft_log__init(&f->log);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    raft_log__close(&f->log);

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * raft_log_append
 */

TEST_SUITE(append);

TEST_SETUP(append, setup);
TEST_TEAR_DOWN(append, tear_down);

TEST_GROUP(append, error);
TEST_GROUP(append, success);

static char *append_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *append_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum append_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, append_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, append_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST_CASE(append, error, oom, append_oom_params)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    rv = raft_log__append(&f->log, 1, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* Out of memory when trying to grow the refs count table. */
TEST_CASE(append, success, oom_refs, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int i;
    int rv;

    (void)params;

    for (i = 0; i < RAFT_LOG__REFS_INITIAL_SIZE; i++) {
        __append_entry(f, 1);
    }

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    buf.base = NULL;
    buf.len = 0;

    rv = raft_log__append(&f->log, 1, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* Append one entry to an empty log. */
TEST_CASE(append, success, one, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    __assert_state(f, 2, 0, 1, 0, 1);
    __assert_term(f, 0, 1);
    __assert_refcount(f, 1, 1);

    return MUNIT_OK;
}

/* Append two entries to to an empty log. */
TEST_CASE(append, success, two, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);
    __append_empty_entry(f);

    __assert_state(f, 6, 0, 2, 0, 2);

    __assert_term(f, 0, 1);
    __assert_type(f, 0, RAFT_LOG_COMMAND);
    __assert_term(f, 1, 1);
    __assert_type(f, 1, RAFT_LOG_COMMAND);

    __assert_refcount(f, 1, 1);
    __assert_refcount(f, 2, 1);

    return MUNIT_OK;
}

/* Append three entries in sequence. */
TEST_CASE(append, success, three, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* One -> [e1, NULL] */
    __append_empty_entry(f);

    __assert_state(f, 2, 0, 1, 0, 1);
    __assert_term(f, 0, 1);

    /* Two -> [e1, e2, NULL, NULL, NULL, NULL] */
    __append_empty_entry(f);

    __assert_state(f, 6, 0, 2, 0, 2);
    __assert_term(f, 0, 1);
    __assert_term(f, 1, 1);

    /* Three -> [e1, e2, e3, NULL, NULL, NULL] */
    __append_empty_entry(f);

    __assert_state(f, 6, 0, 3, 0, 3);
    __assert_term(f, 0, 1);
    __assert_term(f, 1, 1);
    __assert_term(f, 2, 1);

    __assert_refcount(f, 1, 1);
    __assert_refcount(f, 2, 1);
    __assert_refcount(f, 3, 1);

    return MUNIT_OK;
}

/* Append enough entries to force the reference count hash table to be
 * resized. */
TEST_CASE(append, success, many, NULL)
{
    struct fixture *f = data;
    int i;

    (void)params;

    for (i = 0; i < 3000; i++) {
        __append_empty_entry(f);
        __assert_refcount(f, i + 1, 1);
    }

    munit_assert_int(f->log.refs_size, ==, 4096);

    return MUNIT_OK;
}

/* Append to wrapped log that needs to be grown. */
TEST_CASE(append, success, wrap, NULL)
{
    struct fixture *f = data;
    int i;

    (void)params;

    for (i = 0; i < 5; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    __assert_state(f, 6, 0, 5, 0, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    __assert_state(f, 6, 4, 5, 4, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    __assert_state(f, 6, 4, 2, 4, 4);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e5, ..., e11, NULL, ..., NULL] */
    __assert_state(f, 14, 0, 7, 4, 7);

    return MUNIT_OK;
}

/* Append a batch of entries to an empty log. */
TEST_CASE(append, success, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_batch(f, 3);

    __assert_state(f, 6, 0, 3, 0, 3);

    return MUNIT_OK;
}

/**
 * raft_log_append_configuration
 */

TEST_SUITE(append_configuration);

TEST_SETUP(append_configuration, setup);
TEST_TEAR_DOWN(append_configuration, tear_down);

TEST_GROUP(append_configuration, error);

static char *append_configuration_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *append_configuration_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum append_configuration_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, append_configuration_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, append_configuration_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
TEST_CASE(append_configuration, error, _oom, append_configuration_oom_params)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;

    (void)params;

    raft_configuration_init(&configuration);

    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    test_heap_fault_enable(&f->heap);

    rv = raft_log__append_configuration(&f->log, 1, &configuration);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/**
 * raft_log__n_entries
 */

TEST_SUITE(n_entries);

TEST_SETUP(n_entries, setup);
TEST_TEAR_DOWN(n_entries, tear_down);

TEST_GROUP(n_entries, success);

/* The log is empty. */
TEST_CASE(n_entries, success, empty, NULL)
{
    struct fixture *f = data;
    size_t n;

    (void)params;

    n = raft_log__n_entries(&f->log);
    munit_assert_int(n, ==, 0);

    return MUNIT_OK;
}

/* The log is not wrapped. */
TEST_CASE(n_entries, success, not_wrapped, NULL)
{
    struct fixture *f = data;
    size_t n;

    (void)params;

    __append_empty_entry(f);

    n = raft_log__n_entries(&f->log);
    munit_assert_int(n, ==, 1);

    return MUNIT_OK;
}

/**
 * raft_log__first_index
 */

TEST_SUITE(first_index);

TEST_SETUP(first_index, setup);
TEST_TEAR_DOWN(first_index, tear_down);

TEST_GROUP(first_index, success);

/* The log is empty. */
TEST_CASE(first_index, success, empty, NULL)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_int(raft_log__first_index(&f->log), ==, 0);

    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(first_index, success, one_entry, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    munit_assert_int(raft_log__first_index(&f->log), ==, 1);

    return MUNIT_OK;
}

/**
 * raft_log__last_term
 */

TEST_SUITE(last_term);

TEST_SETUP(last_term, setup);
TEST_TEAR_DOWN(last_term, tear_down);

TEST_GROUP(last_term, success);

/* If the log is empty, last term is 0. */
TEST_CASE(last_term, success, empty_log, NULL)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_int(raft_log__last_term(&f->log), ==, 0);

    return MUNIT_OK;
}

/**
 * raft_log__last_index
 */

TEST_SUITE(last_index);

TEST_SETUP(last_index, setup);
TEST_TEAR_DOWN(last_index, tear_down);

/* If the log is empty, last index is 0. */
TEST_CASE(last_index, empty_log, NULL)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_int(raft_log__last_index(&f->log), ==, 0);

    return MUNIT_OK;
}

/* If the log starts at a certain offset, the last index is bumped
 * accordingly. */
TEST_CASE(last_index, empty_log_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;

    raft_log__set_offset(&f->log, 3);
    munit_assert_int(raft_log__last_index(&f->log), ==, 3);

    return MUNIT_OK;
}

/**
 * raft_log__acquire
 */

TEST_SUITE(acquire);

TEST_SETUP(acquire, setup);
TEST_TEAR_DOWN(acquire, tear_down);

TEST_GROUP(acquire, error);
TEST_GROUP(acquire, success);

/* Trying to acquire entries out of range results in a NULL pointer. */
TEST_CASE(acquire, error, out_of_range, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_empty_entry(f);
    __append_empty_entry(f);

    raft_log__shift(&f->log, 1);

    rv = raft_log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);
    munit_assert_ptr_null(entries);

    rv = raft_log__acquire(&f->log, 3, &entries, &n);
    munit_assert_int(rv, ==, 0);
    munit_assert_ptr_null(entries);

    return MUNIT_OK;
}

/* Out of memory. */
TEST_CASE(acquire, error, oom, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_empty_entry(f);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = raft_log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/* Acquire a single log entry. */
TEST_CASE(acquire, success, one, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 1);
    munit_assert_int(entries[0].type, ==, RAFT_LOG_COMMAND);

    __assert_refcount(f, 1, 2);

    raft_log__release(&f->log, 1, entries, n);

    __assert_refcount(f, 1, 1);

    return MUNIT_OK;
}

/* Acquire two log entries. */
TEST_CASE(acquire, success, two, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_entry(f, 1);
    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 2);
    munit_assert_int(entries[0].type, ==, RAFT_LOG_COMMAND);
    munit_assert_int(entries[1].type, ==, RAFT_LOG_COMMAND);

    __assert_refcount(f, 1, 2);
    __assert_refcount(f, 2, 2);

    raft_log__release(&f->log, 1, entries, n);

    __assert_refcount(f, 1, 1);
    __assert_refcount(f, 2, 1);

    return MUNIT_OK;
}

/* Acquire two log entries in a wrapped log. */
TEST_CASE(acquire, success, wrap, NULL)
{
    struct fixture *f = data;
    int i;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    for (i = 0; i < 5; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    __assert_state(f, 6, 0, 5, 0, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    __assert_state(f, 6, 4, 5, 4, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    __assert_state(f, 6, 4, 2, 4, 4);

    rv = raft_log__acquire(&f->log, 6, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_int(n, ==, 3);

    raft_log__release(&f->log, 6, entries, n);

    return MUNIT_OK;
}

/* Acquire several entries some of which belong to batches. */
TEST_CASE(acquire, success, batch, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_entry(f, 1);
    __append_batch(f, 2);
    __append_entry(f, 1);
    __append_batch(f, 3);

    rv = raft_log__acquire(&f->log, 2, &entries, &n);
    munit_assert_int(rv, ==, 0);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 6);

    __assert_refcount(f, 2, 2);

    /* Truncate the last 5 entries, so the only references left for the second
     * batch are the ones in the acquired entries. */
    raft_log__truncate(&f->log, 3);

    raft_log__release(&f->log, 2, entries, n);

    __assert_refcount(f, 2, 1);

    return MUNIT_OK;
}

/**
 * raft_log__truncate
 */

TEST_SUITE(truncate);

TEST_SETUP(truncate, setup);
TEST_TEAR_DOWN(truncate, tear_down);

TEST_GROUP(truncate, success);
TEST_GROUP(truncate, error);

/* Truncate the last entry of a log with a single entry. */
TEST_CASE(truncate, success, 1_last, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    raft_log__truncate(&f->log, 1);

    __assert_state(f, 0, 0, 0, 0, 0);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a two entries. */
TEST_CASE(truncate, success, 2_last, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);
    __append_empty_entry(f);

    raft_log__truncate(&f->log, 2);

    __assert_state(f, 6, 0, 1, 0, 1);
    __assert_term(f, 0, 1);

    return MUNIT_OK;
}

/* Truncate from an entry which is older than the first one in the log. */
TEST_CASE(truncate, success, compacted, NULL)
{
    struct fixture *f = data;

    (void)params;

    f->log.offset = 2;

    __append_empty_entry(f);
    __append_empty_entry(f);

    raft_log__truncate(&f->log, 2);

    __assert_state(f, 0, 0, 0, 2, 0);

    return MUNIT_OK;
}

/* Truncate from an entry which makes the log wrap. */
TEST_CASE(truncate, success, wrap, NULL)
{
    struct fixture *f = data;
    int i;

    (void)params;

    for (i = 0; i < 5; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    __assert_state(f, 6, 0, 5, 0, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    __assert_state(f, 6, 4, 5, 4, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    __assert_state(f, 6, 4, 2, 4, 4);

    /* Truncate from e6 onward (wrapping) */
    raft_log__truncate(&f->log, 6);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    __assert_state(f, 6, 4, 5, 4, 1);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a single entry, which still has an
 * outstanding reference created by a call to raft_log__acquire(). */
TEST_CASE(truncate, success, referenced, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, 0);

    raft_log__truncate(&f->log, 1);

    __assert_state(f, 0, 0, 0, 0, 0);

    /* The entry has still an outstanding reference. */
    __assert_refcount(f, 1, 1);

    munit_assert_string_equal((const char *)entries[0].buf.base, "hello");

    raft_log__release(&f->log, 1, entries, n);

    __assert_refcount(f, 1, 0);

    return MUNIT_OK;
}

/* Truncate all entries belonging to a batch. */
TEST_CASE(truncate, success, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_batch(f, 3);

    raft_log__truncate(&f->log, 1);

    munit_assert_int(f->log.size, ==, 0);

    return MUNIT_OK;
}

/* Acquire entries at a certain index. Truncate the log at that index. The
 * truncated entries are still referenced. Then append a new entry, which will
 * have the same index but different term. */
TEST_CASE(truncate, success, acquired, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    int rv;

    (void)params;

    __append_entry(f, 1);
    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 2, &entries, &n);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(n, ==, 1);

    raft_log__truncate(&f->log, 2);

    __append_entry(f, 2);

    raft_log__release(&f->log, 2, entries, n);

    return MUNIT_OK;
}

static char *truncate_acquired_heap_fault_delay[] = {"0", NULL};
static char *truncate_acquired_fault_repeat[] = {"1", NULL};

static MunitParameterEnum truncate_acquired_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, truncate_acquired_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, truncate_acquired_fault_repeat},
    {NULL, NULL},
};

/* Acquire entries at a certain index. Truncate the log at that index. The
 * truncated entries are still referenced. Then append a new entry, which fails
 * to be appended due to OOM. */
TEST_CASE(truncate, error, acquired_oom, truncate_acquired_oom_params)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    struct raft_buffer buf;
    int rv;

    (void)params;

    __append_entry(f, 1);
    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 2, &entries, &n);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(n, ==, 1);

    raft_log__truncate(&f->log, 2);

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    rv = raft_log__append(&f->log, 2, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    raft_log__release(&f->log, 2, entries, n);

    return MUNIT_OK;
}

/* Acquire some entries, truncate the log and then append new ones forcing the
   log to be grown and the reference count hash table to be re-built. */
TEST_CASE(truncate, success, acquire_append, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rv;

    (void)params;

    __append_entry(f, 1);
    __append_entry(f, 1);

    rv = raft_log__acquire(&f->log, 2, &entries, &n);
    munit_assert_int(rv, ==, 0);
    munit_assert_int(n, ==, 1);

    raft_log__truncate(&f->log, 2);

    for (i = 0; i < RAFT_LOG__REFS_INITIAL_SIZE; i++) {
        __append_entry(f, 2);
    }

    raft_log__release(&f->log, 2, entries, n);

    return MUNIT_OK;
}

/**
 * raft_log__shift
 */

TEST_SUITE(shift);

TEST_SETUP(shift, setup);
TEST_TEAR_DOWN(shift, tear_down);

TEST_GROUP(shift, success);

/* Shift up to the first entry of a log with a single entry. */
TEST_CASE(shift, success, 1_first, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_entry(f, 1);

    raft_log__shift(&f->log, 1);

    __assert_state(f, 0, 0, 0, 1, 0);

    return MUNIT_OK;
}

/* Shift up to the first entry of a log with a two entries. */
TEST_CASE(shift, success, 2_first, NULL)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);
    __append_empty_entry(f);

    raft_log__shift(&f->log, 1);

    __assert_state(f, 6, 1, 2, 1, 1);

    return MUNIT_OK;
}

/* Shift to an entry which makes the log wrap. */
TEST_CASE(shift, success, wrap, NULL)
{
    struct fixture *f = data;
    int i;

    (void)params;

    for (i = 0; i < 5; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    __assert_state(f, 6, 0, 5, 0, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    __assert_state(f, 6, 4, 5, 4, 1);

    /* Append another 4 entries. */
    for (i = 0; i < 4; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, e9, NULL, e5, e6] */
    __assert_state(f, 6, 4, 3, 4, 5);

    /* Shift up to e7 included (wrapping) */
    raft_log__shift(&f->log, 7);

    /* Now the log is [NULL, e8, e9, NULL, NULL, NULL] */
    __assert_state(f, 6, 1, 3, 7, 2);

    return MUNIT_OK;
}
