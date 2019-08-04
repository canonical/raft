#include <stdlib.h>

#include "../lib/heap.h"
#include "../lib/runner.h"

#include "../../src/tracer.h"

TEST_MODULE(tracer);

/******************************************************************************
 *
 * Helpers
 *
 *****************************************************************************/

struct entry
{
    raft_time time;
    unsigned type;
    const char *message;
};

struct fixture
{
    FIXTURE_HEAP;
    struct raft_tracer tracer;
    struct entry *entries; /* Hold entries passed to the walkCb callback. */
    unsigned n_entries;    /* Length of the entries array. */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    int rv;
    SETUP_HEAP;
    rv = raft_tracer_init(&f->tracer, 1024);
    munit_assert_int(rv, ==, 0);
    f->entries = NULL;
    f->n_entries = 0;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    raft_tracer_close(&f->tracer);
    TEAR_DOWN_HEAP;
    free(f->entries);
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

static raft_time clock = 0; /* Increased after each call to EMIT */

static void emit(struct fixture *f, const char *format, ...)
{
    va_list args;
    clock++;
    va_start(args, format);
    tracerEmit(&f->tracer, clock, RAFT_DEBUG, format, args);
    va_end(args);
}

static void walkCb(void *data,
                   raft_time time,
                   unsigned type,
                   const char *message)
{
    struct fixture *f = data;
    struct entry *entry;

    f->n_entries++;

    f->entries = reallocarray(f->entries, f->n_entries, sizeof *f->entries);
    munit_assert_ptr_not_null(f->entries);

    entry = &f->entries[f->n_entries - 1];
    entry->time = time;
    entry->type = type;
    entry->message = message;
}

#define EMIT(FORMAT, ...) emit(f, FORMAT, ##__VA_ARGS__)
#define EMIT_N(N, FORMAT, ...)           \
    {                                    \
        int i_;                          \
        for (i_ = 0; i_ < N; i_++) {     \
            EMIT(FORMAT, ##__VA_ARGS__); \
        }                                \
    }

#define WALK raft_tracer_walk(&f->tracer, walkCb, f);

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert the number of entries that were passed to the walk callback. */
#define ASSERT_N(N) munit_assert_int(f->n_entries, ==, N);

/* Assert that the I'th entry emitted matches the given parameters. */
#define ASSERT_ENTRY(I, TIME, MESSAGE)                      \
    {                                                       \
        struct entry *entry = &f->entries[I];                \
        munit_assert_int(entry->time, ==, TIME);            \
        munit_assert_string_equal(entry->message, MESSAGE); \
    }

/******************************************************************************
 *
 * tracerEmit
 *
 *****************************************************************************/

TEST_SUITE(emit);
TEST_SETUP(emit, setup);
TEST_TEAR_DOWN(emit, tear_down);

/* Emit a single entry. */
TEST_CASE(emit, one, NULL)
{
    struct fixture *f = data;
    (void)params;
    EMIT("hello");
    WALK;
    ASSERT_N(1);
    ASSERT_ENTRY(0, 1, "hello");
    return MUNIT_OK;
}

/* Emit two entries. */
TEST_CASE(emit, two, NULL)
{
    struct fixture *f = data;
    (void)params;
    EMIT("hello %s!", "world");
    EMIT("this is %s", "great");
    WALK;
    ASSERT_N(2);
    ASSERT_ENTRY(0, 1, "hello world!");
    ASSERT_ENTRY(1, 2, "this is great");
    return MUNIT_OK;
}

/* Emit enough entries to cause the buffer to wrap. */
TEST_CASE(emit, wrap, NULL)
{
    struct fixture *f = data;
    (void)params;
    EMIT("first");
    EMIT_N(44, "middle");
    EMIT("hello world!");
    WALK;
    ASSERT_N(42);
    ASSERT_ENTRY(41, 46, "hello world!");
    return MUNIT_OK;
}

/* Emit enough entries to cause the buffer to wrap twice. */
TEST_CASE(emit, wrap_twice, NULL)
{
    struct fixture *f = data;
    (void)params;
    EMIT("first");
    EMIT_N(90, "middle");
    EMIT("hello world!");
    WALK;
    ASSERT_N(42);
    ASSERT_ENTRY(41, 92, "hello world!");
    return MUNIT_OK;
}

/* Shifting the head would not be enough for the new message. */
TEST_CASE(emit, overflow, NULL)
{
    struct fixture *f = data;
    const char *message = "very long message that can't fit in trailing bytes, so the all the messages are wiped";
    (void)params;
    EMIT("first");
    EMIT_N(81, "middle");
    EMIT(message);
    WALK;
    ASSERT_N(1);
    ASSERT_ENTRY(0, 83, message);
    return MUNIT_OK;
}

/* A dummy entry needs to be written. */
TEST_CASE(emit, dummy, NULL)
{
    struct fixture *f = data;
    (void)params;
    EMIT_N(44, "long sentence");
    EMIT("hello world!");
    WALK;
    return MUNIT_OK;
}

/* A message is too long and gets truncated. */
TEST_CASE(emit, truncate, NULL) {
    struct fixture *f = data;
    char message[300];
    (void)params;
    memset(message, 'a', sizeof message - 1);
    EMIT(message);
    WALK;
    ASSERT_N(1);
    message[254] = 0;
    ASSERT_ENTRY(0, 1, message);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * tracerWalk
 *
 *****************************************************************************/

TEST_SUITE(walk);
TEST_SETUP(walk, setup);
TEST_TEAR_DOWN(walk, tear_down);

/* Walk a tracer with no entries. */
TEST_CASE(walk, empty, NULL)
{
    struct fixture *f = data;
    (void)params;
    WALK;
    ASSERT_N(0);
    return MUNIT_OK;
}
