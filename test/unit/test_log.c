#include "../lib/heap.h"
#include "../lib/log.h"
#include "../lib/runner.h"

TEST_MODULE(log);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOG;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_HEAP;
    SETUP_LOG;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_LOG;
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * log__set_offset
 *
 *****************************************************************************/

TEST_SUITE(set_offset);
TEST_SETUP(set_offset, setup);
TEST_TEAR_DOWN(set_offset, tear_down);

/* By default the offset is 0 and the first appended entry has index 1. */
TEST_CASE(set_offset, default, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__FIRST_INDEX, ==, 1);
    return MUNIT_OK;
}

/* Set the offset to one. */
TEST_CASE(set_offset, one, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(1);
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__FIRST_INDEX, ==, 2);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__append
 *
 *****************************************************************************/

TEST_SUITE(append);

TEST_SETUP(append, setup);
TEST_TEAR_DOWN(append, tear_down);

/* Append one entry to an empty log. */
TEST_CASE(append, one, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND(1 /* term */);

    LOG__ASSERT(2 /* size                                                    */,
                0 /* front                                                   */,
                1 /* back                                                    */,
                0 /* offset                                                  */,
                1 /* n */);
    LOG__ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    LOG__ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append two entries to to an empty log. */
TEST_CASE(append, two, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__ASSERT(6 /* size                                                    */,
                0 /* front                                                   */,
                2 /* back                                                    */,
                0 /* offset                                                  */,
                2 /* n */);
    LOG__ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    LOG__ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    LOG__ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    LOG__ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append three entries in sequence. */
TEST_CASE(append, three, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* One -> [e1, NULL] */
    LOG__APPEND(1 /* term */);

    /* Two -> [e1, e2, NULL, NULL, NULL, NULL] */
    LOG__APPEND(1 /* term */);

    /* Three -> [e1, e2, e3, NULL, NULL, NULL] */
    LOG__APPEND(1 /* term */);

    LOG__ASSERT(6 /* size                                                    */,
                0 /* front                                                   */,
                3 /* back                                                    */,
                0 /* offset                                                  */,
                3 /* n */);
    LOG__ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    LOG__ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    LOG__ASSERT_TERM_OF(3 /* entry index */, 1 /* term */);
    LOG__ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    LOG__ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);
    LOG__ASSERT_REFCOUNT(3 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append enough entries to force the reference count hash table to be
 * resized. */
TEST_CASE(append, many, NULL)
{
    struct fixture *f = data;
    int i;
    (void)params;
    for (i = 0; i < 3000; i++) {
        LOG__APPEND(1 /* term */);
    }
    munit_assert_int(f->log.refs_size, ==, 4096);
    return MUNIT_OK;
}

/* Append to wrapped log that needs to be grown. */
TEST_CASE(append, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    LOG__ASSERT(6 /* size                                                    */,
                0 /* front                                                   */,
                5 /* back                                                    */,
                0 /* offset                                                  */,
                5 /* n */);

    /* Delete the first 4 entries. */
    LOG__SHIFT(4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    LOG__ASSERT(6 /* size                                                    */,
                4 /* front                                                   */,
                5 /* back                                                    */,
                4 /* offset                                                  */,
                1 /* n */);

    /* Append another 3 entries. */
    LOG__APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    LOG__ASSERT(6 /* size                                                    */,
                4 /* front                                                   */,
                2 /* back                                                    */,
                4 /* offset                                                  */,
                4 /* n */);

    /* Append another 3 entries. */
    LOG__APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e5, ..., e11, NULL, ..., NULL] */
    LOG__ASSERT(14 /* size                                                 */,
                0 /* front                                                 */,
                7 /* back                                                  */,
                4 /* offset                                                */,
                7 /* n */);

    return MUNIT_OK;
}

/* Append a batch of entries to an empty log. */
TEST_CASE(append, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    LOG__APPEND_BATCH(3);

    LOG__ASSERT(6 /* size                                                 */,
                0 /* front                                                 */,
                3 /* back                                                  */,
                0 /* offset                                                */,
                3 /* n */);

    return MUNIT_OK;
}

TEST_GROUP(append, error);

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
    rv = log__append(&f->log, 1, RAFT_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);
    return MUNIT_OK;
}

/* Out of memory when trying to grow the refs count table. */
TEST_CASE(append, error, oom_refs, NULL)
{
    struct fixture *f = data;
    struct raft_buffer buf;
    int rv;
    (void)params;

    LOG__APPEND_MANY(1, LOG__REFS_INITIAL_SIZE);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    buf.base = NULL;
    buf.len = 0;

    rv = log__append(&f->log, 1, RAFT_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__append_configuration
 *
 *****************************************************************************/

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
TEST_CASE(append_configuration, error, oom, append_configuration_oom_params)
{
    struct fixture *f = data;
    struct raft_configuration configuration;
    int rv;
    (void)params;

    raft_configuration_init(&configuration);
    rv = raft_configuration_add(&configuration, 1, "1", true);
    munit_assert_int(rv, ==, 0);

    test_heap_fault_enable(&f->heap);

    rv = log__append_configuration(&f->log, 1, &configuration);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    raft_configuration_close(&configuration);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__n_entries
 *
 *****************************************************************************/

TEST_SUITE(n_entries);

TEST_SETUP(n_entries, setup);
TEST_TEAR_DOWN(n_entries, tear_down);

/* The log is empty. */
TEST_CASE(n_entries, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(LOG__N_ENTRIES, ==, 0);
    return MUNIT_OK;
}

/* The log is not wrapped. */
TEST_CASE(n_entries, not_wrapped, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__N_ENTRIES, ==, 1);
    return MUNIT_OK;
}

/* The log is wrapped. */
TEST_CASE(n_entries, wrapped, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__APPEND_MANY(1 /* term */, 5 /* term */);
    LOG__SHIFT(4);
    LOG__APPEND_MANY(1 /* term */, 3 /* term */);
    munit_assert_int(LOG__N_ENTRIES, ==, 4);
    return MUNIT_OK;
}

/* The log has an offset. */
TEST_CASE(n_entries, offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(10);
    munit_assert_int(LOG__N_ENTRIES, ==, 0);
    return MUNIT_OK;
}

/* The log has an offset and is not empty. */
TEST_CASE(n_entries, offset_not_empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(10);
    LOG__APPEND_MANY(1 /* term */, 3 /* n */);
    munit_assert_int(LOG__N_ENTRIES, ==, 3);
    return MUNIT_OK;
}

/* The log has an offset and is wrapped. */
TEST_CASE(n_entries, offset_and_wrapped, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(9);
    LOG__APPEND_MANY(1 /* term */, 5 /* term */);
    LOG__SHIFT(14);
    LOG__APPEND_MANY(1 /* term */, 3 /* term */);
    munit_assert_int(LOG__N_ENTRIES, ==, 3);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__first_index
 *
 *****************************************************************************/

TEST_SUITE(first_index);

TEST_SETUP(first_index, setup);
TEST_TEAR_DOWN(first_index, tear_down);

/* The log is empty. */
TEST_CASE(first_index, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(LOG__FIRST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* The log is empty but has an offset. */
TEST_CASE(first_index, empty_with_offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(10);
    munit_assert_int(LOG__FIRST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(first_index, one, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__FIRST_INDEX, ==, 1);
    return MUNIT_OK;
}

/* The log has one entry and an offset. */
TEST_CASE(first_index, one_with_offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(10);
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__FIRST_INDEX, ==, 11);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__last_index
 *
 *****************************************************************************/

TEST_SUITE(last_index);

TEST_SETUP(last_index, setup);
TEST_TEAR_DOWN(last_index, tear_down);

/* If the log is empty, last index is 0. */
TEST_CASE(last_index, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(LOG__LAST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* If the log is empty and has an offset, last index is 0. */
TEST_CASE(last_index, empty_with_offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    LOG__SET_OFFSET(10);
    munit_assert_int(LOG__LAST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(last_index, one, NULL)
{
    struct fixture *f = data;

    (void)params;
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__LAST_INDEX, ==, 1);

    return MUNIT_OK;
}

/* The log has two entries. */
TEST_CASE(last_index, two, NULL)
{
    struct fixture *f = data;

    (void)params;
    LOG__APPEND_MANY(1 /* term */, 2 /* n */);
    munit_assert_int(LOG__LAST_INDEX, ==, 2);

    return MUNIT_OK;
}

/* If the log starts at a certain offset, the last index is bumped
 * accordingly. */
TEST_CASE(last_index, two_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;
    LOG__SET_OFFSET(3);
    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);
    munit_assert_int(LOG__LAST_INDEX, ==, 5);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__last_term
 *
 *****************************************************************************/

TEST_SUITE(last_term);

TEST_SETUP(last_term, setup);
TEST_TEAR_DOWN(last_term, tear_down);

/* If the log is empty, last term is 0. */
TEST_CASE(last_term, empty_log, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(LOG__LAST_TERM, ==, 0);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__acquire
 *
 *****************************************************************************/

TEST_SUITE(acquire);

TEST_SETUP(acquire, setup);
TEST_TEAR_DOWN(acquire, tear_down);

/* Acquire a single log entry. */
TEST_CASE(acquire, one, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);

    LOG__ACQUIRE(1);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 1);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);

    LOG__ASSERT_REFCOUNT(1, 2);

    LOG__RELEASE(1);

    LOG__ASSERT_REFCOUNT(1, 1);

    return MUNIT_OK;
}

/* Acquire two log entries. */
TEST_CASE(acquire, two, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__ACQUIRE(1);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 2);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);
    munit_assert_int(entries[1].type, ==, RAFT_COMMAND);

    LOG__ASSERT_REFCOUNT(1, 2);
    LOG__ASSERT_REFCOUNT(2, 2);

    LOG__RELEASE(1);

    LOG__ASSERT_REFCOUNT(1, 1);
    LOG__ASSERT_REFCOUNT(2, 1);

    return MUNIT_OK;
}

/* Acquire two log entries in a wrapped log. */
TEST_CASE(acquire, wrap, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                0 /* front                                                */,
                5 /* back                                                 */,
                0 /* offset                                               */,
                5 /* n */);

    /* Delete the first 4 entries. */
    LOG__SHIFT(4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                5 /* back                                                 */,
                4 /* offset                                               */,
                1 /* n */);

    /* Append another 3 entries. */
    LOG__APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                2 /* back                                                 */,
                4 /* offset                                               */,
                4 /* n */);

    LOG__ACQUIRE(6);

    munit_assert_int(n, ==, 3);

    LOG__RELEASE(6);

    return MUNIT_OK;
}

/* Acquire several entries some of which belong to batches. */
TEST_CASE(acquire, batch, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND_BATCH(2 /* n entries */);
    LOG__APPEND(1 /* term */);
    LOG__APPEND_BATCH(3 /* n entries */);

    LOG__ACQUIRE(2);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 6);

    LOG__ASSERT_REFCOUNT(2, 2);

    /* Truncate the last 5 entries, so the only references left for the second
     * batch are the ones in the acquired entries. */
    LOG__TRUNCATE(3);

    LOG__RELEASE(2);

    LOG__ASSERT_REFCOUNT(2, 1);

    return MUNIT_OK;
}

TEST_GROUP(acquire, error);

/* Trying to acquire entries out of range results in a NULL pointer. */
TEST_CASE(acquire, error, out_of_range, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__SHIFT(1);

    LOG__ACQUIRE(1);

    munit_assert_ptr_null(entries);

    LOG__ACQUIRE(3);

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

    LOG__APPEND(1 /* term */);

    test_heap_fault_config(&f->heap, 0, 1);
    test_heap_fault_enable(&f->heap);

    rv = log__acquire(&f->log, 1, &entries, &n);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__truncate
 *
 *****************************************************************************/

TEST_SUITE(truncate);

TEST_SETUP(truncate, setup);
TEST_TEAR_DOWN(truncate, tear_down);

/* Truncate the last entry of a log with a single entry. */
TEST_CASE(truncate, 1_last, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__TRUNCATE(1);

    LOG__ASSERT(0 /* size                                                 */,
                0 /* front                                                */,
                0 /* back                                                 */,
                0 /* offset                                               */,
                0 /* n */);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a two entries. */
TEST_CASE(truncate, 2_last, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__TRUNCATE(2);

    LOG__ASSERT(6 /* size                                                 */,
                0 /* front                                                */,
                1 /* back                                                 */,
                0 /* offset                                               */,
                1 /* n */);
    LOG__ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);

    return MUNIT_OK;
}

/* Truncate from an entry which is older than the first one in the log. */
TEST_CASE(truncate, compacted, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__SET_OFFSET(2);

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__TRUNCATE(2);

    LOG__ASSERT(0 /* size                                                 */,
                0 /* front                                                */,
                0 /* back                                                 */,
                2 /* offset                                               */,
                0 /* n */);

    return MUNIT_OK;
}

/* Truncate from an entry which makes the log wrap. */
TEST_CASE(truncate, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;

    LOG__APPEND_MANY(1 /* term */, 5 /* n entries */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                0 /* front                                                */,
                5 /* back                                                 */,
                0 /* offset                                               */,
                5 /* n */);

    /* Delete the first 4 entries. */
    LOG__SHIFT(4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                5 /* back                                                 */,
                4 /* offset                                               */,
                1 /* n */);

    /* Append another 3 entries. */
    LOG__APPEND_MANY(1 /* term */, 3 /* n entries */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                2 /* back                                                 */,
                4 /* offset                                               */,
                4 /* n */);

    /* Truncate from e6 onward (wrapping) */
    LOG__TRUNCATE(6);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                5 /* back                                                 */,
                4 /* offset                                               */,
                1 /* n */);

    return MUNIT_OK;
}

/* Truncate the last entry of a log with a single entry, which still has an
 * outstanding reference created by a call to log__acquire(). */
TEST_CASE(truncate, referenced, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__ACQUIRE(1 /* index */);
    LOG__TRUNCATE(1 /* index */);

    LOG__ASSERT(0 /* size                                                 */,
                0 /* front                                                */,
                0 /* back                                                 */,
                0 /* offset                                               */,
                0 /* n */);

    /* The entry has still an outstanding reference. */
    LOG__ASSERT_REFCOUNT(1, 1);

    munit_assert_string_equal((const char *)entries[0].buf.base, "hello");

    LOG__RELEASE(1);
    LOG__ASSERT_REFCOUNT(1, 0);

    return MUNIT_OK;
}

/* Truncate all entries belonging to a batch. */
TEST_CASE(truncate, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    LOG__APPEND_BATCH(3);

    LOG__TRUNCATE(1);

    munit_assert_int(f->log.size, ==, 0);

    return MUNIT_OK;
}

/* Acquire entries at a certain index. Truncate the log at that index. The
 * truncated entries are still referenced. Then append a new entry, which will
 * have the same index but different term. */
TEST_CASE(truncate, acquired, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__ACQUIRE(2);

    munit_assert_int(n, ==, 1);

    LOG__TRUNCATE(2);

    LOG__APPEND(2 /* term */);

    LOG__RELEASE(2);

    return MUNIT_OK;
}

/* Acquire some entries, truncate the log and then append new ones forcing the
   log to be grown and the reference count hash table to be re-built. */
TEST_CASE(truncate, acquire_append, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;
    size_t i;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__ACQUIRE(2);

    munit_assert_int(n, ==, 1);

    LOG__TRUNCATE(2);

    for (i = 0; i < LOG__REFS_INITIAL_SIZE; i++) {
        LOG__APPEND(2 /* term */);
    }

    LOG__RELEASE(2);

    return MUNIT_OK;
}

/* Truncate an empty log which has an offset. */
TEST_CASE(truncate, empty_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;

    LOG__SET_OFFSET(10);
    LOG__TRUNCATE(1);

    return MUNIT_OK;
}

TEST_GROUP(truncate, error);

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

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__ACQUIRE(2);
    munit_assert_int(n, ==, 1);

    LOG__TRUNCATE(2);

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    rv = log__append(&f->log, 2, RAFT_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    LOG__RELEASE(2);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__truncate
 *
 *****************************************************************************/

TEST_SUITE(shift);

TEST_SETUP(shift, setup);
TEST_TEAR_DOWN(shift, tear_down);

/* Shift up to the first entry of a log with a single entry. */
TEST_CASE(shift, 1_first, NULL)
{
    struct fixture *f = data;

    (void)params;

    LOG__APPEND(1 /* term */);

    LOG__SHIFT(1);

    LOG__ASSERT(0 /* size                                                 */,
                0 /* front                                                */,
                0 /* back                                                 */,
                1 /* offset                                               */,
                0 /* n */);

    return MUNIT_OK;
}

/* Shift up to the first entry of a log with a two entries. */
TEST_CASE(shift, 2_first, NULL)
{
    struct fixture *f = data;

    (void)params;

    LOG__APPEND(1 /* term */);
    LOG__APPEND(1 /* term */);

    LOG__SHIFT(1);

    LOG__ASSERT(6 /* size                                                 */,
                1 /* front                                                */,
                2 /* back                                                 */,
                1 /* offset                                               */,
                1 /* n */);

    return MUNIT_OK;
}

/* Shift to an entry which makes the log wrap. */
TEST_CASE(shift, wrap, NULL)
{
    struct fixture *f = data;
    int i;

    (void)params;

    for (i = 0; i < 5; i++) {
        LOG__APPEND(1 /* term */);
    }

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                0 /* front                                                */,
                5 /* back                                                 */,
                0 /* offset                                               */,
                5 /* n */);

    /* Delete the first 4 entries. */
    LOG__SHIFT(4);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                5 /* back                                                 */,
                4 /* offset                                               */,
                1 /* n */);

    /* Append another 4 entries. */
    LOG__APPEND_MANY(1 /* term */, 4 /* n */);

    /* Now the log is [e7, e8, e9, NULL, e5, e6] */
    LOG__ASSERT(6 /* size                                                 */,
                4 /* front                                                */,
                3 /* back                                                 */,
                4 /* offset                                               */,
                5 /* n */);

    /* Shift up to e7 included (wrapping) */
    LOG__SHIFT(7);

    /* Now the log is [NULL, e8, e9, NULL, NULL, NULL] */
    LOG__ASSERT(6 /* size                                                 */,
                1 /* front                                                */,
                3 /* back                                                 */,
                7 /* offset                                               */,
                2 /* n */);

    return MUNIT_OK;
}
