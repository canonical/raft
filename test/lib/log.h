/**
 * In-memory raft log helpers.
 */

#ifndef TEST_LOG_H
#define TEST_LOG_H

#include "macros.h"

#include "../../src/log.h"

#define FIXTURE_LOG struct raft_log log;
#define SETUP_LOG log__init(&f->log)
#define TEAR_DOWN_LOG log__close(&f->log)

/**
 * Append an entry to the fixture's log.
 *
 * Possible signatures are:
 *
 * - LOG__APPEND_ENTRY(TERM): The entry will have the given term.
 */
#define LOG__APPEND(...) LOG__APPEND_X(__VA_ARGS__)(__VA_ARGS__)
#define LOG__APPEND_X(...) GET_2ND_ARG(__VA_ARGS__, LOG__APPEND_1)
#define LOG__APPEND_1(TERM)                                        \
    {                                                              \
        struct raft_buffer buf;                                    \
        int rv;                                                    \
        buf.base = raft_malloc(8);                                 \
        buf.len = 8;                                               \
        strcpy(buf.base, "hello");                                 \
        rv = log__append(&f->log, TERM, RAFT_COMMAND, &buf, NULL); \
        munit_assert_int(rv, ==, 0);                               \
    }

#define LOG__APPEND_MANY(TERM, N) \
    {                             \
        int i;                    \
        for (i = 0; i < N; i++) { \
            LOG__APPEND(TERM);    \
        }                         \
    }

/**
 * Append N entries all belonging to the same batch. Each entry will have 64-bit
 * payload set to i * 1000, where i is the index of the entry in the batch.
 */
#define LOG__APPEND_BATCH(N)                                         \
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

#define LOG__FIRST_INDEX log__first_index(&f->log)
#define LOG__SET_OFFSET(OFFSET) log__set_offset(&f->log, OFFSET)
#define LOG__N_ENTRIES log__n_entries(&f->log)
#define LOG__LAST_TERM log__last_term(&f->log)
#define LOG__LAST_INDEX log__last_index(&f->log)

#define LOG__ACQUIRE(INDEX)                              \
    {                                                    \
        int rv;                                          \
        rv = log__acquire(&f->log, INDEX, &entries, &n); \
        munit_assert_int(rv, ==, 0);                     \
    }

#define LOG__RELEASE(INDEX) log__release(&f->log, INDEX, entries, n);

#define LOG__TRUNCATE(N) log__truncate(&f->log, N)
#define LOG__SHIFT(N) log__shift(&f->log, N)

/**
 * Assert the state of the fixture's log in terms of size, front/back indexes,
 * offset and number of entries.
 */
#define LOG__ASSERT(SIZE, FRONT, BACK, OFFSET, N)         \
    {                                                     \
        munit_assert_int(f->log.size, ==, SIZE);          \
        munit_assert_int(f->log.front, ==, FRONT);        \
        munit_assert_int(f->log.back, ==, BACK);          \
        munit_assert_int(f->log.offset, ==, OFFSET);      \
        munit_assert_int(log__n_entries(&f->log), ==, N); \
    }

/**
 * Assert that the term of entry at INDEX equals TERM.
 */
#define LOG__ASSERT_TERM_OF(INDEX, TERM)         \
    {                                            \
        const struct raft_entry *entry;          \
        entry = log__get(&f->log, INDEX);        \
        munit_assert_ptr_not_null(entry);        \
        munit_assert_int(entry->term, ==, TERM); \
    }

/**
 * Assert that the number of outstanding references for the entry at INDEX
 * equals COUNT.
 */
#define LOG__ASSERT_REFCOUNT(INDEX, COUNT)                            \
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

#endif /* TEST_LOG_H */
