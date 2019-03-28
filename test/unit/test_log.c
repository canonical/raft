#include "../lib/heap.h"
#include "../lib/log.h"
#include "../lib/runner.h"

TEST_MODULE(log);

/******************************************************************************
 *
 * Fixture
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
 * Helper macros
 *
 *****************************************************************************/

/* Accessors */
#define N_OUTSTANDING log__n_outstanding(&f->log)
#define LAST_INDEX log__last_index(&f->log)
#define TERM_OF(INDEX) log__term_of(&f->log, INDEX)
#define LAST_TERM log__last_term(&f->log)
#define GET(INDEX) log__get(&f->log, INDEX)

/* Append one command entry with the given term and a hard-coded payload. */
#define APPEND(TERM)                                                 \
    {                                                                \
        struct raft_buffer buf2;                                     \
        int rv2;                                                     \
        buf2.base = raft_malloc(8);                                  \
        buf2.len = 8;                                                \
        strcpy(buf2.base, "hello");                                  \
        rv2 = log__append(&f->log, TERM, RAFT_COMMAND, &buf2, NULL); \
        munit_assert_int(rv2, ==, 0);                                \
    }

/* Same as APPEND, but repeated N times. */
#define APPEND_MANY(TERM, N)      \
    {                             \
        int i;                    \
        for (i = 0; i < N; i++) { \
            APPEND(TERM);         \
        }                         \
    }

/* Append N entries all belonging to the same batch. Each entry will have 64-bit
 * payload set to i * 1000, where i is the index of the entry in the batch. */
#define APPEND_BATCH(N)                                              \
    {                                                                \
        void *batch;                                                 \
        size_t offset;                                               \
        int i;                                                       \
        batch = raft_malloc(8 * N);                                  \
        munit_assert_ptr_not_null(batch);                            \
        offset = 0;                                                  \
        for (i = 0; i < N; i++) {                                    \
            struct raft_buffer buf;                                  \
            int rv;                                                  \
            buf.base = batch + offset;                               \
            buf.len = 8;                                             \
            *(uint64_t *)buf.base = i * 1000;                        \
            rv = log__append(&f->log, 1, RAFT_COMMAND, &buf, batch); \
            munit_assert_int(rv, ==, 0);                             \
            offset += 8;                                             \
        }                                                            \
    }

#define ACQUIRE(INDEX)                                    \
    {                                                     \
        int rv2;                                          \
        rv2 = log__acquire(&f->log, INDEX, &entries, &n); \
        munit_assert_int(rv2, ==, 0);                     \
    }

#define RELEASE(INDEX) log__release(&f->log, INDEX, entries, n);

#define TRUNCATE(N) log__truncate(&f->log, N)
#define SNAPSHOT(INDEX, TRAILING) log__snapshot(&f->log, INDEX, TRAILING)
#define RESTORE(INDEX, TERM) log__restore(&f->log, INDEX, TERM)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert the state of the fixture's log in terms of size, front/back indexes,
 * offset and number of entries. */
#define ASSERT(SIZE, FRONT, BACK, OFFSET, N)     \
    munit_assert_int(f->log.size, ==, SIZE);     \
    munit_assert_int(f->log.front, ==, FRONT);   \
    munit_assert_int(f->log.back, ==, BACK);     \
    munit_assert_int(f->log.offset, ==, OFFSET); \
    munit_assert_int(log__n_outstanding(&f->log), ==, N)

/* Assert the last index and term of the most recent snapshot. */
#define ASSERT_SNAPSHOT(INDEX, TERM)                         \
    munit_assert_int(f->log.snapshot.last_index, ==, INDEX); \
    munit_assert_int(f->log.snapshot.last_term, ==, TERM)

/* Assert that the term of entry at INDEX equals TERM. */
#define ASSERT_TERM_OF(INDEX, TERM)              \
    {                                            \
        const struct raft_entry *entry;          \
        entry = log__get(&f->log, INDEX);        \
        munit_assert_ptr_not_null(entry);        \
        munit_assert_int(entry->term, ==, TERM); \
    }

/* Assert that the number of outstanding references for the entry at INDEX
 * equals COUNT. */
#define ASSERT_REFCOUNT(INDEX, COUNT)                                 \
    {                                                                 \
        size_t i;                                                     \
        munit_assert_ptr_not_null(f->log.refs);                       \
        for (i = 0; i < f->log.refs_size; i++) {                      \
            if (f->log.refs[i].index == INDEX) {                      \
                munit_assert_int(f->log.refs[i].count, ==, COUNT);    \
                break;                                                \
            }                                                         \
        }                                                             \
        if (i == f->log.refs_size) {                                  \
            munit_errorf("no refcount found for entry with index %d", \
                         (int)INDEX);                                 \
        }                                                             \
    }

/******************************************************************************
 *
 * log__n_outstanding
 *
 *****************************************************************************/

TEST_SUITE(n_outstanding);

TEST_SETUP(n_outstanding, setup);
TEST_TEAR_DOWN(n_outstanding, tear_down);

/* If the log is empty, the return value is zero. */
TEST_CASE(n_outstanding, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(N_OUTSTANDING, ==, 0);
    return MUNIT_OK;
}

/* The log is not wrapped. */
TEST_CASE(n_outstanding, not_wrapped, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND(1 /* term */);
    munit_assert_int(N_OUTSTANDING, ==, 1);
    return MUNIT_OK;
}

/* The log is wrapped. */
TEST_CASE(n_outstanding, wrapped, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(4, 1);
    APPEND_MANY(1 /* term */, 2 /* n entries */);
    munit_assert_int(N_OUTSTANDING, ==, 4);
    return MUNIT_OK;
}

/* The log has an offset and is empty. */
TEST_CASE(n_outstanding, offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(5, 0);
    munit_assert_int(N_OUTSTANDING, ==, 0);
    return MUNIT_OK;
}

/* The log has an offset and is not empty. */
TEST_CASE(n_outstanding, offset_not_empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(4, 2);
    munit_assert_int(N_OUTSTANDING, ==, 3);
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
    munit_assert_int(LAST_INDEX, ==, 0);
    return MUNIT_OK;
}

/* If the log is empty and has an offset, last index is calculated
   accordingly. */
TEST_CASE(last_index, empty_with_offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND(1);
    SNAPSHOT(1, 0);
    munit_assert_int(LAST_INDEX, ==, 1);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(last_index, one, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND(1 /* term */);
    munit_assert_int(LAST_INDEX, ==, 1);

    return MUNIT_OK;
}

/* The log has two entries. */
TEST_CASE(last_index, two, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(1 /* term */, 2 /* n */);
    munit_assert_int(LAST_INDEX, ==, 2);

    return MUNIT_OK;
}

/* If the log starts at a certain offset, the last index is bumped
 * accordingly. */
TEST_CASE(last_index, two_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n */);
    SNAPSHOT(5, 2);
    munit_assert_int(LAST_INDEX, ==, 5);

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

/* If the log is empty, return zero. */
TEST_CASE(last_term, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_int(LAST_TERM, ==, 0);
    return MUNIT_OK;
}

/* If the log has a snapshot and no outstanding entries, return the last term of
 * the snapshot. */
TEST_CASE(last_term, snapshot, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND(1);
    SNAPSHOT(1, 0);
    munit_assert_int(LAST_TERM, ==, 1);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__term_of
 *
 *****************************************************************************/

TEST_SUITE(term_of);

TEST_SETUP(term_of, setup);
TEST_TEAR_DOWN(term_of, tear_down);

/* If the given index is beyond the last index, return 0. */
TEST_CASE(term_of, beyond_last, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND(1);
    munit_assert_int(TERM_OF(2), ==, 0);
    munit_assert_int(TERM_OF(10), ==, 0);
    return MUNIT_OK;
}

/* If the log is empty but has a snapshot, and the given index matches the last
 * index of the snapshot, return the snapshot last term. */
TEST_CASE(term_of, snapshot_last_index, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(5, 0);
    munit_assert_int(TERM_OF(5), ==, 1);
    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(term_of, one, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND(3 /* term */);
    munit_assert_int(TERM_OF(1), ==, 3);

    return MUNIT_OK;
}

/* The log has two entries. */
TEST_CASE(term_of, two, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(4 /* term */, 2 /* n */);
    munit_assert_int(TERM_OF(1), ==, 4);
    munit_assert_int(TERM_OF(2), ==, 4);

    return MUNIT_OK;
}

/* The log has a snapshot and hence an offset. */
TEST_CASE(term_of, before_snapshot, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(1 /* term */, 5 /* n entries */);
    SNAPSHOT(3, 0);
    munit_assert_int(TERM_OF(1), ==, 0);
    munit_assert_int(TERM_OF(2), ==, 0);
    munit_assert_int(TERM_OF(3), ==, 1);
    munit_assert_int(TERM_OF(4), ==, 1);
    munit_assert_int(TERM_OF(5), ==, 1);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__get
 *
 *****************************************************************************/

TEST_SUITE(get);

TEST_SETUP(get, setup);
TEST_TEAR_DOWN(get, tear_down);

/* The log is empty. */
TEST_CASE(get, empty_log, NULL)
{
    struct fixture *f = data;
    (void)params;
    munit_assert_ptr_null(GET(1));
    return MUNIT_OK;
}

/* The log is empty but has an offset. */
TEST_CASE(get, empty_with_offset, NULL)
{
    struct fixture *f = data;
    (void)params;
    APPEND_MANY(4 /* term */, 10 /* n */);
    SNAPSHOT(10, 0);
    munit_assert_ptr_null(GET(1));
    munit_assert_ptr_null(GET(10));
    munit_assert_ptr_null(GET(11));
    return MUNIT_OK;
}

/* The log has one entry. */
TEST_CASE(get, one, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND(3 /* term */);

    munit_assert_int(GET(1)->term, ==, 3);

    munit_assert_ptr_null(GET(2));

    return MUNIT_OK;
}

/* The log has two entries. */
TEST_CASE(get, two, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(4 /* term */, 2 /* n */);

    munit_assert_int(GET(1)->term, ==, 4);
    munit_assert_int(GET(2)->term, ==, 4);

    munit_assert_ptr_null(GET(3));

    return MUNIT_OK;
}

/* The log starts at a certain offset. */
TEST_CASE(get, two_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;
    APPEND_MANY(1 /* term */, 3 /* n */);
    APPEND(2 /* term */);
    APPEND(3 /* term */);
    SNAPSHOT(4, 1);

    munit_assert_ptr_null(GET(1));
    munit_assert_ptr_null(GET(2));
    munit_assert_ptr_null(GET(3));

    munit_assert_int(GET(4)->term, ==, 2);
    munit_assert_int(GET(5)->term, ==, 3);

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

    APPEND(1 /* term */);

    ASSERT(2 /* size                                                    */,
           0 /* front                                                   */,
           1 /* back                                                    */,
           0 /* offset                                                  */,
           1 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append two entries to to an empty log. */
TEST_CASE(append, two, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           2 /* back                                                    */,
           0 /* offset                                                  */,
           2 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);

    return MUNIT_OK;
}

/* Append three entries in sequence. */
TEST_CASE(append, three, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* One -> [e1, NULL] */
    APPEND(1 /* term */);

    /* Two -> [e1, e2, NULL, NULL, NULL, NULL] */
    APPEND(1 /* term */);

    /* Three -> [e1, e2, e3, NULL, NULL, NULL] */
    APPEND(1 /* term */);

    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           3 /* back                                                    */,
           0 /* offset                                                  */,
           3 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(2 /* entry index */, 1 /* term */);
    ASSERT_TERM_OF(3 /* entry index */, 1 /* term */);
    ASSERT_REFCOUNT(1 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(2 /* entry index */, 1 /* count */);
    ASSERT_REFCOUNT(3 /* entry index */, 1 /* count */);

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
        APPEND(1 /* term */);
    }
    munit_assert_int(f->log.refs_size, ==, 4096);
    return MUNIT_OK;
}

/* Append to wrapped log that needs to be grown. */
TEST_CASE(append, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                    */,
           0 /* front                                                   */,
           5 /* back                                                    */,
           0 /* offset                                                  */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4, 0);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                    */,
           4 /* front                                                   */,
           5 /* back                                                    */,
           4 /* offset                                                  */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                    */,
           4 /* front                                                   */,
           2 /* back                                                    */,
           4 /* offset                                                  */,
           4 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e5, ..., e11, NULL, ..., NULL] */
    ASSERT(14 /* size                                                 */,
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

    APPEND_BATCH(3);

    ASSERT(6 /* size                                                 */,
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

    APPEND_MANY(1, LOG__REFS_INITIAL_SIZE);

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

    APPEND(1 /* term */);

    ACQUIRE(1);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 1);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);

    ASSERT_REFCOUNT(1, 2);

    RELEASE(1);

    ASSERT_REFCOUNT(1, 1);

    return MUNIT_OK;
}

/* Acquire two log entries. */
TEST_CASE(acquire, two, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(1);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 2);
    munit_assert_int(entries[0].type, ==, RAFT_COMMAND);
    munit_assert_int(entries[1].type, ==, RAFT_COMMAND);

    ASSERT_REFCOUNT(1, 2);
    ASSERT_REFCOUNT(2, 2);

    RELEASE(1);

    ASSERT_REFCOUNT(1, 1);
    ASSERT_REFCOUNT(2, 1);

    return MUNIT_OK;
}

/* Acquire two log entries in a wrapped log. */
TEST_CASE(acquire, wrap, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    APPEND_MANY(1 /* term */, 5 /* n */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4, 0);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           2 /* back                                                 */,
           4 /* offset                                               */,
           4 /* n */);

    ACQUIRE(6);

    munit_assert_int(n, ==, 3);

    RELEASE(6);

    return MUNIT_OK;
}

/* Acquire several entries some of which belong to batches. */
TEST_CASE(acquire, batch, NULL)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    unsigned n;

    (void)params;

    APPEND(1 /* term */);
    APPEND_BATCH(2 /* n entries */);
    APPEND(1 /* term */);
    APPEND_BATCH(3 /* n entries */);

    ACQUIRE(2);

    munit_assert_ptr_not_null(entries);
    munit_assert_int(n, ==, 6);

    ASSERT_REFCOUNT(2, 2);

    /* Truncate the last 5 entries, so the only references left for the second
     * batch are the ones in the acquired entries. */
    TRUNCATE(3);

    RELEASE(2);

    ASSERT_REFCOUNT(2, 1);

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

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    SNAPSHOT(1, 0);

    ACQUIRE(1);

    munit_assert_ptr_null(entries);

    ACQUIRE(3);

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

    APPEND(1 /* term */);

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

    APPEND(1 /* term */);
    TRUNCATE(1);

    ASSERT(0 /* size                                                 */,
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

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    TRUNCATE(2);

    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           1 /* back                                                 */,
           0 /* offset                                               */,
           1 /* n */);
    ASSERT_TERM_OF(1 /* entry index */, 1 /* term */);

    return MUNIT_OK;
}

/* Truncate from an entry which makes the log wrap. */
TEST_CASE(truncate, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND_MANY(1 /* term */, 5 /* n entries */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Delete the first 4 entries. */
    SNAPSHOT(4, 0);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    /* Append another 3 entries. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);

    /* Now the log is [e7, e8, NULL, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           2 /* back                                                 */,
           4 /* offset                                               */,
           4 /* n */);

    /* Truncate from e6 onward (wrapping) */
    TRUNCATE(6);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
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

    APPEND(1 /* term */);
    ACQUIRE(1 /* index */);
    TRUNCATE(1 /* index */);

    ASSERT(0 /* size                                                 */,
           0 /* front                                                */,
           0 /* back                                                 */,
           0 /* offset                                               */,
           0 /* n */);

    /* The entry has still an outstanding reference. */
    ASSERT_REFCOUNT(1, 1);

    munit_assert_string_equal((const char *)entries[0].buf.base, "hello");

    RELEASE(1);
    ASSERT_REFCOUNT(1, 0);

    return MUNIT_OK;
}

/* Truncate all entries belonging to a batch. */
TEST_CASE(truncate, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    APPEND_BATCH(3);

    TRUNCATE(1);

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

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(2);

    munit_assert_int(n, ==, 1);

    TRUNCATE(2);

    APPEND(2 /* term */);

    RELEASE(2);

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

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(2);

    munit_assert_int(n, ==, 1);

    TRUNCATE(2);

    for (i = 0; i < LOG__REFS_INITIAL_SIZE; i++) {
        APPEND(2 /* term */);
    }

    RELEASE(2);

    return MUNIT_OK;
}

/* Truncate an empty log which has an offset. */
TEST_CASE(truncate, empty_with_offset, NULL)
{
    struct fixture *f = data;

    (void)params;

    // SET_OFFSET(10);
    TRUNCATE(1);

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

    APPEND(1 /* term */);
    APPEND(1 /* term */);

    ACQUIRE(2);
    munit_assert_int(n, ==, 1);

    TRUNCATE(2);

    buf.base = NULL;
    buf.len = 0;

    test_heap_fault_enable(&f->heap);

    rv = log__append(&f->log, 2, RAFT_COMMAND, &buf, NULL);
    munit_assert_int(rv, ==, RAFT_ENOMEM);

    RELEASE(2);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__snapshot
 *
 *****************************************************************************/

TEST_SUITE(snapshot);

TEST_SETUP(snapshot, setup);
TEST_TEAR_DOWN(snapshot, tear_down);

/* Take a snapshot at entry 3, keeping 2 trailing entries. */
TEST_CASE(snapshot, trailing, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND(1 /* term */);
    APPEND(2 /* term */);
    APPEND(2 /* term */);

    SNAPSHOT(3, 2);

    ASSERT(6 /* size                                                 */,
           1 /* front                                                */,
           3 /* back                                                 */,
           1 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(3 /* index */, 2 /* term */);

    return MUNIT_OK;
}

/* Take a snapshot when the number of outstanding entries is lower than the
 * desired trail (so no entry will be deleted). */
TEST_CASE(snapshot, trailing_higher_than_outstanding, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Take a snapshot leaving just one entry in the log. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);
    SNAPSHOT(3, 1);

    /* Take another snapshot, trying to leave 3 entries, but only 2 are
     * available at all. */
    APPEND(2 /* term */);

    SNAPSHOT(4, 3);

    ASSERT(6 /* size                                                 */,
           2 /* front                                                */,
           4 /* back                                                 */,
           2 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(4 /* index */, 2 /* term */);

    return MUNIT_OK;
}

/* Take a snapshot when the number of outstanding entries is exactly equal to
 * the desired trail (so no entry will be deleted). */
TEST_CASE(snapshot, trailing_matches_outstanding, NULL)
{
    struct fixture *f = data;
    (void)params;

    /* Take a snapshot leaving just one entry in the log. */
    APPEND_MANY(1 /* term */, 3 /* n entries */);
    SNAPSHOT(3, 1);

    /* Take another snapshot, leaving 2 entries, which are the ones we have. */
    APPEND(2 /* term */);

    SNAPSHOT(4, 2);

    ASSERT(6 /* size                                                 */,
           2 /* front                                                */,
           4 /* back                                                 */,
           2 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(4 /* index */, 2 /* term */);

    return MUNIT_OK;
}

/* Take a snapshot at a point where the log needs to wrap. */
TEST_CASE(snapshot, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;

    APPEND_MANY(1 /* term */, 5 /* n entries */);

    /* Now the log is [e1, e2, e3, e4, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           0 /* front                                                */,
           5 /* back                                                 */,
           0 /* offset                                               */,
           5 /* n */);

    /* Take a snapshot at e5, keeping just e5 itself. */
    SNAPSHOT(5, 1);

    /* Now the log is [NULL, NULL, NULL, NULL, e5, NULL] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           5 /* back                                                 */,
           4 /* offset                                               */,
           1 /* n */);

    ASSERT_SNAPSHOT(5 /* index */, 1 /* term */);

    /* Append another 4 entries. */
    APPEND_MANY(1 /* term */, 4 /* n */);

    /* Now the log is [e7, e8, e9, NULL, e5, e6] */
    ASSERT(6 /* size                                                 */,
           4 /* front                                                */,
           3 /* back                                                 */,
           4 /* offset                                               */,
           5 /* n */);

    /* Take a snapshot at e8 keeping only e8 itself (wrapping) */
    SNAPSHOT(8, 1);

    /* Now the log is [NULL, e8, e9, NULL, NULL, NULL] */
    ASSERT(6 /* size                                                 */,
           1 /* front                                                */,
           3 /* back                                                 */,
           7 /* offset                                               */,
           2 /* n */);

    ASSERT_SNAPSHOT(8 /* index */, 1 /* term */);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * log__restore
 *
 *****************************************************************************/

TEST_SUITE(restore);

TEST_SETUP(restore, setup);
TEST_TEAR_DOWN(restore, tear_down);

/* Mimick the initial restore of a snapshot after loading state from disk, when
 * there are no outstanding entries.. */
TEST_CASE(restore, initial, NULL)
{
    (void)params;
    struct fixture *f = data;
    RESTORE(2, 3);
    ASSERT_SNAPSHOT(2 /* index */, 3 /* term */);
    munit_assert_int(LAST_INDEX, ==, 2);
    return MUNIT_OK;
}
