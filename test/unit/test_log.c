#include "../../src/log.h"

#include "../lib/heap.h"
#include "../lib/munit.h"

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

static char *append_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *append_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum append_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, append_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, append_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
static MunitResult test_append_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;

    (void)params;

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    rv = raft_log__append(&f->log, 1, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* Out of memory when trying to grow the refs count table. */
static MunitResult test_append_oom_refs(const MunitParameter params[],
                                        void *data)
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
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* Append one entry to an empty log. */
static MunitResult test_append_one(const MunitParameter params[], void *data)
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
static MunitResult test_append_two(const MunitParameter params[], void *data)
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
static MunitResult test_append_three(const MunitParameter params[], void *data)
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

/* Append enough entries to force the reference count hash table to be resized
   . */
static MunitResult test_append_many(const MunitParameter params[], void *data)
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
static MunitResult test_append_wrap(const MunitParameter params[], void *data)
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
static MunitResult test_append_batch(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_batch(f, 3);

    __assert_state(f, 6, 0, 3, 0, 3);

    return MUNIT_OK;
}

static MunitTest append_tests[] = {
    {"/oom", test_append_oom, setup, tear_down, 0, append_oom_params},
    {"/oom-refs", test_append_oom_refs, setup, tear_down, 0, NULL},
    {"/one", test_append_one, setup, tear_down, 0, NULL},
    {"/two", test_append_two, setup, tear_down, 0, NULL},
    {"/three", test_append_three, setup, tear_down, 0, NULL},
    {"/many", test_append_many, setup, tear_down, 0, NULL},
    {"/wrap", test_append_wrap, setup, tear_down, 0, NULL},
    {"/batch", test_append_batch, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_log_append_configuration
 */

static char *append_configuration_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *append_configuration_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum append_configuration_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, append_configuration_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, append_configuration_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory. */
static MunitResult test_append_configuration_oom(const MunitParameter params[],
                                                 void *data)
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
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

static MunitTest append_configuration_tests[] = {
    {"/oom", test_append_configuration_oom, setup, tear_down, 0,
     append_configuration_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_log__n_entries
 */

/* The log is empty. */
static MunitResult test_n_entries_empty(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    size_t n;

    (void)params;

    n = raft_log__n_entries(&f->log);
    munit_assert_int(n, ==, 0);

    return MUNIT_OK;
}

/* The log is not wrapped. */
static MunitResult test_n_entries_not_wrapped(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    size_t n;

    (void)params;

    __append_empty_entry(f);

    n = raft_log__n_entries(&f->log);
    munit_assert_int(n, ==, 1);

    return MUNIT_OK;
}

static MunitTest n_entries_tests[] = {
    {"/empty", test_n_entries_empty, setup, tear_down, 0, NULL},
    {"/not-wrapped", test_n_entries_not_wrapped, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_log__first_index
 */

/* The log is empty. */
static MunitResult test_first_index_empty(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_int(raft_log__first_index(&f->log), ==, 0);

    return MUNIT_OK;
}

/* The log has one entry. */
static MunitResult test_first_index_one_entry(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    munit_assert_int(raft_log__first_index(&f->log), ==, 1);

    return MUNIT_OK;
}

static MunitTest first_index_tests[] = {
    {"/empty", test_first_index_empty, setup, tear_down, 0, NULL},
    {"/one-entry", test_first_index_one_entry, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * raft_log__last_term
 *
 */

/* If the log is empty, last term is 0. */
static MunitResult test_last_term_empty_log(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    munit_assert_int(raft_log__last_term(&f->log), ==, 0);

    return MUNIT_OK;
}

static MunitTest last_term_tests[] = {
    {"/empty-log", test_last_term_empty_log, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 *
 * raft_log__acquire
 *
 */

/* Trying to acquire entries out of range results in a NULL pointer. */
static MunitResult test_acquire_out_of_range(const MunitParameter params[],
                                             void *data)
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
static MunitResult test_acquire_oom(const MunitParameter params[], void *data)
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
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

/* Acquire a single log entry. */
static MunitResult test_acquire_one(const MunitParameter params[], void *data)
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
static MunitResult test_acquire_two(const MunitParameter params[], void *data)
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
static MunitResult test_acquire_wrap(const MunitParameter params[], void *data)
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
static MunitResult test_acquire_batch(const MunitParameter params[], void *data)
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

static MunitTest acquire_tests[] = {
    {"/out-of-range", test_acquire_out_of_range, setup, tear_down, 0, NULL},
    {"/oom", test_acquire_oom, setup, tear_down, 0, NULL},
    {"/one", test_acquire_one, setup, tear_down, 0, NULL},
    {"/two", test_acquire_two, setup, tear_down, 0, NULL},
    {"/wrap", test_acquire_wrap, setup, tear_down, 0, NULL},
    {"/batch", test_acquire_batch, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_log__truncate
 */

/* Truncate the last entry of a log with a single entry. */
static MunitResult test_truncate_1_last(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    raft_log__truncate(&f->log, 1);

    __assert_state(f, 0, 0, 0, 0, 0);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a two entries. */
static MunitResult test_truncate_2_last(const MunitParameter params[],
                                        void *data)
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
static MunitResult test_truncate_compacted(const MunitParameter params[],
                                           void *data)
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
static MunitResult test_truncate_wrap(const MunitParameter params[], void *data)
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
static MunitResult test_truncate_referenced(const MunitParameter params[],
                                            void *data)
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
static MunitResult test_truncate_batch(const MunitParameter params[],
                                       void *data)
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
static MunitResult test_truncate_acquired(const MunitParameter params[],
                                          void *data)
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
static MunitResult test_truncate_acquired_oom(const MunitParameter params[],
                                              void *data)
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
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    raft_log__release(&f->log, 2, entries, n);

    return MUNIT_OK;
}

/* Acquire some entries, truncate the log and then append new ones forcing the
   log to be grown and the reference count hash table to be re-built. */
static MunitResult test_truncate_acquire_append(const MunitParameter params[],
                                                void *data)
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

static MunitTest truncate_tests[] = {
    {"/1-last", test_truncate_1_last, setup, tear_down, 0, NULL},
    {"/2-last", test_truncate_2_last, setup, tear_down, 0, NULL},
    {"/compacted", test_truncate_compacted, setup, tear_down, 0, NULL},
    {"/wrap", test_truncate_wrap, setup, tear_down, 0, NULL},
    {"/referenced", test_truncate_referenced, setup, tear_down, 0, NULL},
    {"/batch", test_truncate_batch, setup, tear_down, 0, NULL},
    {"/acquired", test_truncate_acquired, setup, tear_down, 0, NULL},
    {"/acquired-oom", test_truncate_acquired_oom, setup, tear_down, 0,
     truncate_acquired_oom_params},
    {"/acquire-append", test_truncate_acquire_append, setup, tear_down, 0,
     NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_log__shift
 */

/* Shift up to the first entry of a log with a single entry. */
static MunitResult test_shift_1_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_entry(f, 1);

    raft_log__shift(&f->log, 1);

    __assert_state(f, 0, 0, 0, 1, 0);

    return MUNIT_OK;
}

/* Shift up to the first entry of a log with a two entries. */
static MunitResult test_shift_2_first(const MunitParameter params[], void *data)
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
static MunitResult test_shift_wrap(const MunitParameter params[], void *data)
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
static MunitTest shift_tests[] = {
    {"/1-first", test_shift_1_first, setup, tear_down, 0, NULL},
    {"/2-first", test_shift_2_first, setup, tear_down, 0, NULL},
    {"/wrap", test_shift_wrap, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_log_suites[] = {
    {"/append", append_tests, NULL, 1, 0},
    {"/append-configuration", append_configuration_tests, NULL, 1, 0},
    {"/n-entries", n_entries_tests, NULL, 1, 0},
    {"/first-index", first_index_tests, NULL, 1, 0},
    {"/last-term", last_term_tests, NULL, 1, 0},
    {"/acquire", acquire_tests, NULL, 1, 0},
    {"/truncate", truncate_tests, NULL, 1, 0},
    {"/shift", shift_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
