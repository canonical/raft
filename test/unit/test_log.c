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
static void __append_empty_entry(struct fixture *f)
{
    struct raft_buffer buf;
    int rv;

    buf.base = NULL;
    buf.len = 0;

    rv = raft_log__append(&f->log, 1, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, 0);
}

/**
 * Append an entry with a small payload to the log and check the result.
 */
static void __append_entry(struct fixture *f, const raft_term term)
{
    struct raft_buffer buf;
    int rv;

    buf.base = raft_malloc(8);
    buf.len = 8;

    strcpy(buf.base, "hello");

    rv = raft_log__append(&f->log, term, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, 0);
}

/**
 * Append n entries all belonging to the same batch.
 */
static void __append_batch(struct fixture *f, int n)
{
    void *batch;
    size_t offset;
    int i;

    batch = raft_malloc(8 * n);

    munit_assert_ptr_not_null(batch);

    offset = 0;

    for (i = 0; i < n; i++) {
        struct raft_buffer buf;
        int rv;

        buf.base = batch + offset;
        buf.len = 8;

        *(uint64_t *)buf.base = i * 1000;

        rv = raft_log__append(&f->log, 1, RAFT_LOG_COMMAND, &buf, batch);
        munit_assert_int(rv, ==, 0);

        offset += 8;
    }
}

/**
 * Return the current refcount value for the entry with the given index.
 */
static short __refcount(struct fixture *f, raft_index index)
{
    size_t i;

    munit_assert_ptr_not_null(f->log.refs);

    for (i = 0; i < f->log.refs_size; i++) {
        if (f->log.refs[i].index == index) {
            return f->log.refs[i].count;
        }
    }

    munit_errorf("no refcount found for entry with index %lld", index);
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

/* Append one entry to an empty log. */
static MunitResult test_append_one(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);

    munit_assert_int(f->log.size, ==, 2);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 1);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    munit_assert_int(f->log.entries[0].term, ==, 1);

    munit_assert_int(__refcount(f, 1), ==, 1);

    return MUNIT_OK;
}

/* Append two entries to to an empty log. */
static MunitResult test_append_two(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_empty_entry(f);
    __append_empty_entry(f);

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 2);

    munit_assert_int(f->log.entries[0].term, ==, 1);
    munit_assert_int(f->log.entries[0].type, ==, RAFT_LOG_COMMAND);
    munit_assert_int(f->log.entries[1].term, ==, 1);
    munit_assert_int(f->log.entries[1].type, ==, RAFT_LOG_COMMAND);

    munit_assert_int(__refcount(f, 1), ==, 1);
    munit_assert_int(__refcount(f, 2), ==, 1);

    return MUNIT_OK;
}

/* Append three entries in sequence. */
static MunitResult test_append_three(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    /* One -> [e1, NULL] */
    __append_empty_entry(f);

    munit_assert_int(f->log.size, ==, 2);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 1);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    munit_assert_int(f->log.entries[0].term, ==, 1);

    /* Two -> [e1, e2, NULL, NULL, NULL, NULL] */
    __append_empty_entry(f);

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 2);

    munit_assert_int(f->log.entries[0].term, ==, 1);
    munit_assert_int(f->log.entries[1].term, ==, 1);

    /* Three -> [e1, e2, e3, NULL, NULL, NULL] */
    __append_empty_entry(f);

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 3);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 3);

    munit_assert_int(f->log.entries[0].term, ==, 1);
    munit_assert_int(f->log.entries[1].term, ==, 1);
    munit_assert_int(f->log.entries[2].term, ==, 1);

    munit_assert_int(__refcount(f, 1), ==, 1);
    munit_assert_int(__refcount(f, 2), ==, 1);
    munit_assert_int(__refcount(f, 3), ==, 1);

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
        munit_assert_int(__refcount(f, i + 1), ==, 1);
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
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);
    return 0;

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 4);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e5, ..., e11, NULL, ..., NULL] */
    munit_assert_int(f->log.size, ==, 14);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 7);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 7);

    return MUNIT_OK;
}

/* Append a batch of entries to an empty log. */
static MunitResult test_append_batch(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_batch(f, 3);

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 3);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 3);

    return MUNIT_OK;
}

static MunitTest append_tests[] = {
    {"/oom", test_append_oom, setup, tear_down, 0, append_oom_params},
    {"/one", test_append_one, setup, tear_down, 0, NULL},
    {"/two", test_append_two, setup, tear_down, 0, NULL},
    {"/three", test_append_three, setup, tear_down, 0, NULL},
    {"/many", test_append_many, setup, tear_down, 0, NULL},
    {"/wrap", test_append_wrap, setup, tear_down, 0, NULL},
    {"/batch", test_append_batch, setup, tear_down, 0, NULL},
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

    munit_assert_int(__refcount(f, 1), ==, 2);

    raft_log__release(&f->log, 1, entries, n);

    munit_assert_int(__refcount(f, 1), ==, 1);

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

    munit_assert_int(__refcount(f, 1), ==, 2);
    munit_assert_int(__refcount(f, 2), ==, 2);

    raft_log__release(&f->log, 1, entries, n);

    munit_assert_int(__refcount(f, 1), ==, 1);
    munit_assert_int(__refcount(f, 2), ==, 1);

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
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 4);

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

    munit_assert_int(__refcount(f, 2), ==, 2);

    /* Truncate the last 5 entries, so the only references left for the second
     * batch are the ones in the acquired entries. */
    raft_log__truncate(&f->log, 3);

    raft_log__release(&f->log, 2, entries, n);

    munit_assert_int(__refcount(f, 2), ==, 1);

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

    munit_assert_int(f->log.size, ==, 0);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 0);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_ptr_null(f->log.entries);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 0);

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

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 1);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    munit_assert_int(f->log.entries[0].term, ==, 1);

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

    munit_assert_int(f->log.size, ==, 0);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 0);
    munit_assert_int(f->log.offset, ==, 2);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 0);

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
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    /* Append another 3 entries. */
    for (i = 0; i < 3; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 4);

    /* Truncate from e6 onward (wrapping) */
    raft_log__truncate(&f->log, 6);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

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

    munit_assert_int(f->log.size, ==, 0);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 0);
    munit_assert_int(f->log.offset, ==, 0);
    munit_assert_ptr_null(f->log.entries);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 0);

    /* The entry has still an outstanding reference. */
    munit_assert_int(__refcount(f, 1), ==, 1);

    munit_assert_string_equal((const char *)entries[0].buf.base, "hello");

    raft_log__release(&f->log, 1, entries, n);

    munit_assert_int(__refcount(f, 1), ==, 0);

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
    munit_log(MUNIT_LOG_INFO, "fault");

    rv = raft_log__append(&f->log, 2, RAFT_LOG_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

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

    munit_assert_int(f->log.size, ==, 0);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 0);
    munit_assert_int(f->log.offset, ==, 1);
    munit_assert_ptr_null(f->log.entries);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 0);

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

    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 1);
    munit_assert_int(f->log.back, ==, 2);
    munit_assert_int(f->log.offset, ==, 1);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

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
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 0);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 5);

    /* Delete the first 4 entries. */
    raft_log__shift(&f->log, 4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 5);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 1);

    /* Append another 4 entries. */
    for (i = 0; i < 4; i++) {
        __append_empty_entry(f);
    }

    /* Now the log is [e7, e8, e9, NULL, e5, e6] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 4);
    munit_assert_int(f->log.back, ==, 3);
    munit_assert_int(f->log.offset, ==, 4);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 5);

    /* Shift up to e7 included (wrapping) */
    raft_log__shift(&f->log, 7);

    /* Now the log is [NULL, e8, e9, NULL, NULL, NULL] */
    munit_assert_int(f->log.size, ==, 6);
    munit_assert_int(f->log.front, ==, 1);
    munit_assert_int(f->log.back, ==, 3);
    munit_assert_int(f->log.offset, ==, 7);
    munit_assert_int(raft_log__n_entries(&f->log), ==, 2);

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
    {"/n-entries", n_entries_tests, NULL, 1, 0},
    {"/first-index", first_index_tests, NULL, 1, 0},
    {"/last-term", last_term_tests, NULL, 1, 0},
    {"/acquire", acquire_tests, NULL, 1, 0},
    {"/truncate", truncate_tests, NULL, 1, 0},
    {"/shift", shift_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
