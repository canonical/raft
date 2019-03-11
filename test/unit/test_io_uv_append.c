#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/byte.h"
#include "../../src/io_uv.h"
#include "../../src/io_uv_encoding.h"

TEST_MODULE(io_uv__append);

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/**
 * Helpers.
 */

struct fixture
{
    IO_UV_FIXTURE;
    struct raft_entry *entries;
    unsigned n;
    int count;   /* To generate deterministic entry data */
    int invoked; /* Number of times append_cb was invoked */
    int status;  /* Last status passed to append_cb */
};

struct append_req
{
    struct fixture *f;
    struct raft_entry *entries;
    unsigned n;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->uv->n_blocks = MAX_SEGMENT_BLOCKS;
    f->count = 0;
    f->invoked = 0;
    f->status = 0;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    IO_UV_TEAR_DOWN;
}

static void append_cb(void *data, int status)
{
    struct append_req *r = data;
    struct fixture *f = r->f;
    unsigned i;

    f->invoked++;
    f->status = status;

    for (i = 0; i < r->n; i++) {
        raft_free(r->entries[i].buf.base);
    }

    raft_free((struct raft_entry *)r->entries);
    free(r);
}

/* Set the arguments for the next append entries call. The f->entries array will
 * be populated with N entries each of size SIZE. */
#define append_args(N, SIZE)                                     \
    {                                                            \
        int i;                                                   \
        f->entries = raft_malloc(N * sizeof(struct raft_entry)); \
        f->n = N;                                                \
        munit_assert_ptr_not_null(f->entries);                   \
        for (i = 0; i < N; i++) {                                \
            struct raft_entry *entry = &f->entries[i];           \
            void *cursor;                                        \
            entry->term = 1;                                     \
            entry->type = RAFT_LOG_COMMAND;                      \
            entry->buf.base = raft_malloc(SIZE);                 \
            entry->buf.len = SIZE;                               \
            entry->batch = NULL;                                 \
            munit_assert_ptr_not_null(entry->buf.base);          \
            memset(entry->buf.base, 0, entry->buf.len);          \
            cursor = entry->buf.base;                            \
            byte__put64(&cursor, f->count);                      \
            f->count++;                                          \
        }                                                        \
    }

/* Invoke raft_io->append() and assert that it returns the given code. */
#define append_invoke(RV)                                          \
    {                                                              \
        unsigned i;                                                \
        struct append_req *r = munit_malloc(sizeof *r);            \
        int rv;                                                    \
        r->f = f;                                                  \
        r->entries = f->entries;                                   \
        r->n = f->n;                                               \
        rv = f->io.append(&f->io, f->entries, f->n, r, append_cb); \
        munit_assert_int(rv, ==, RV);                              \
        if (rv != 0) {                                             \
            for (i = 0; i < f->n; i++) {                           \
                raft_free(f->entries[i].buf.base);                 \
            }                                                      \
            raft_free(f->entries);                                 \
            free(r);                                               \
        }                                                          \
    }

/* Wait for the given number of append request callbacks to fire and check the
 * last status. */
#define append_wait_cb(N, STATUS)                \
    {                                            \
        int i;                                   \
        for (i = 0; i < 5; i++) {                \
            test_uv_run(&f->loop, 1);            \
            if (f->invoked == N) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, N);     \
        munit_assert_int(f->status, ==, STATUS); \
        f->invoked = 0;                          \
    }

/* Assert that the open segment with the given counter has format version 1 and
 * N entries with a total data size of S bytes. */
#define assert_segment(COUNTER, N, SIZE)                                    \
    {                                                                       \
        struct raft_buffer buf;                                             \
        const void *cursor;                                                 \
        char filename[strlen("open-N") + 1];                                \
        unsigned i = 0;                                                     \
        size_t total_data_size = 0;                                         \
                                                                            \
        sprintf(filename, "open-%d", COUNTER);                              \
                                                                            \
        buf.len = MAX_SEGMENT_BLOCKS * f->uv->block_size;                   \
        buf.base = munit_malloc(buf.len);                                   \
                                                                            \
        test_dir_read_file(f->dir, filename, buf.base, buf.len);            \
                                                                            \
        cursor = buf.base;                                                  \
        munit_assert_int(byte__get64(&cursor), ==, 1);                      \
                                                                            \
        while (i < N) {                                                     \
            unsigned crc1 = byte__get32(&cursor);                           \
            unsigned crc2 = byte__get32(&cursor);                           \
            const void *header = cursor;                                    \
            const void *data;                                               \
            unsigned n = byte__get64(&cursor);                              \
            struct raft_entry *entries = munit_malloc(n * sizeof *entries); \
            unsigned j;                                                     \
            unsigned crc;                                                   \
            size_t data_size = 0;                                           \
	    munit_logf(MUNIT_LOG_INFO, "batch %d has %d entries", i, n);\
                                                                            \
            for (j = 0; j < n; j++) {                                       \
                struct raft_entry *entry = &entries[j];                     \
                                                                            \
                entry->term = byte__get64(&cursor);                         \
                entry->type = byte__get8(&cursor);                          \
                byte__get8(&cursor);                                        \
                byte__get8(&cursor);                                        \
                byte__get8(&cursor);                                        \
                entry->buf.len = byte__get32(&cursor);                      \
                                                                            \
                munit_assert_int(entry->term, ==, 1);                       \
                munit_assert_int(entry->type, ==, RAFT_LOG_COMMAND);        \
                                                                            \
                data_size += entry->buf.len;                                \
            }                                                               \
                                                                            \
            crc = byte__crc32(header, io_uv__sizeof_batch_header(n), 0);    \
            munit_assert_int(crc, ==, crc1);                                \
                                                                            \
            data = cursor;                                                  \
                                                                            \
            for (j = 0; j < n; j++) {                                       \
                struct raft_entry *entry = &entries[j];                     \
                uint64_t value;                                             \
                                                                            \
                value = byte__flip64(*(uint64_t *)cursor);                  \
                munit_assert_int(value, ==, i);                             \
                                                                            \
                cursor += entry->buf.len;                                   \
                                                                            \
                i++;                                                        \
            }                                                               \
                                                                            \
            crc = byte__crc32(data, data_size, 0);                          \
            munit_assert_int(crc, ==, crc2);                                \
                                                                            \
            free(entries);                                                  \
                                                                            \
            total_data_size += data_size;                                   \
        }                                                                   \
                                                                            \
        munit_assert_int(total_data_size, ==, SIZE);                        \
        free(buf.base);                                                     \
    }

/**
 * Success scenarios.
 */

TEST_SUITE(success);

TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

/* Append the very first batch of entries. */
TEST_CASE(success, first, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);
    assert_segment(1, 1, 64);

    return MUNIT_OK;
}

/* Write the very first entry and then another one, both fitting in the same
 * block. */
TEST_CASE(success, fit_block, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    assert_segment(1, 2, 128);

    return MUNIT_OK;
}

/* Write an entry that fills the first block exactly and then another one. */
TEST_CASE(success, match_block, NULL)
{
    struct fixture *f = data;
    size_t size;

    (void)params;

    size = f->uv->block_size;
    size -= sizeof(uint64_t) +             /* Format */
            sizeof(uint64_t) +             /* Checksums */
            io_uv__sizeof_batch_header(1); /* Header */

    append_args(1, size);
    append_invoke(0);
    append_wait_cb(1, 0);

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    assert_segment(1, 2, size + 64);

    return MUNIT_OK;
}

/* Write an entry that exceeds the first block, then another one that fits in
 * the second block, then a third one that fills the rest of the second block
 * plus the whole third block exactly, and finally a fourth entry that fits in
 * the fourth block */
TEST_CASE(success, exceed_block, NULL)
{
    struct fixture *f = data;
    size_t written;
    size_t size1;
    size_t size2;

    (void)params;

    size1 = f->uv->block_size;

    append_args(1, size1);
    append_invoke(0);
    append_wait_cb(1, 0);

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    written = sizeof(uint64_t) +              /* Format version */
              2 * sizeof(uint32_t) +          /* CRC sums of first batch */
              io_uv__sizeof_batch_header(1) + /* Header of first batch */
              size1 +                         /* Size of first batch */
              2 * sizeof(uint32_t) +          /* CRC of second batch */
              io_uv__sizeof_batch_header(1) + /* Header of second batch */
              64;                             /* Size of second batch */

    /* Write a third entry that fills the second block exactly */
    size2 = f->uv->block_size - (written % f->uv->block_size);
    size2 -= (2 * sizeof(uint32_t) + io_uv__sizeof_batch_header(1));
    size2 += f->uv->block_size;

    append_args(1, size2);
    append_invoke(0);
    append_wait_cb(1, 0);

    /* Write a fourth entry */
    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    assert_segment(1, 4, size1 + 64 + size2 + 64);

    return MUNIT_OK;
}

/* If an append request is submitted before the write operation of the previous
 * append request is started, then a single write will be performed for both
 * requests. */
TEST_CASE(success, batch, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);
    append_invoke(0);

    append_args(1, 64);
    append_invoke(0);

    append_wait_cb(2, 0);

    return MUNIT_OK;
}

/* An append request submitted while a write operation is in progress gets
 * executed only when the write completes. */
TEST_CASE(success, wait, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);
    append_invoke(0);

    test_uv_run(&f->loop, 1);

    append_args(1, 64);
    append_invoke(0);

    append_wait_cb(1, 0);
    append_wait_cb(1, 0);

    return MUNIT_OK;
}

/* Several batches with different size gets appended in fast pace, which forces
 * the segment arena to grow. */
TEST_CASE(success, resize_arena, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(2, 64);
    append_invoke(0);

    append_args(1, f->uv->block_size);
    append_invoke(0);

    append_args(2, 64);
    append_invoke(0);

    append_args(1, f->uv->block_size);
    append_invoke(0);

    append_args(1, f->uv->block_size);
    append_invoke(0);

    append_wait_cb(5, 0);

    assert_segment(1, 7, 64 * 4 + f->uv->block_size * 3);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. */
TEST_CASE(success, truncate, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    append_args(2, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    append_args(2, 64);

    append_invoke(0);

    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    append_args(2, 64);
    append_invoke(0);

    append_wait_cb(2, 0);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. However
 * the backend is closed before the truncation request can be processed. */
TEST_CASE(success, truncate_closing, NULL)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    append_args(2, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    append_args(2, 64);

    append_invoke(0);

    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    append_args(2, 64);
    append_invoke(0);

    return MUNIT_OK;
}

/* The counters of the open segments get increased as they are closed. */
TEST_CASE(success, counter, NULL)
{
    struct fixture *f = data;
    size_t size = f->uv->block_size;
    int i;

    (void)params;

    for (i = 0; i < 10; i++) {
        append_args(1, size);
        append_invoke(0);
        append_wait_cb(1, 0);
    }

    munit_assert_true(test_dir_has_file(f->dir, "1-3"));
    munit_assert_true(test_dir_has_file(f->dir, "4-6"));
    munit_assert_true(test_dir_has_file(f->dir, "open-4"));

    return MUNIT_OK;
}

/**
 * Failure scenarios.
 */

TEST_SUITE(error);
TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* The batch of entries to append is too big. */
TEST_CASE(error, too_big, NULL)
{
    struct fixture *f = data;
    struct io_uv *uv = f->io.impl;

    (void)params;

    append_args(MAX_SEGMENT_BLOCKS, uv->block_size);
    append_invoke(RAFT_ERR_IO_TOOBIG);

    return MUNIT_OK;
}

/* If the I/O instance is closed, all pending append requests get canceled. */
TEST_CASE(error, cancel, NULL)
{
    struct fixture *f = data;
    return MUNIT_SKIP;

    (void)params;

    append_args(1, 64);
    append_invoke(0);

    io_uv__close;

    append_wait_cb(1, RAFT_ERR_IO_CANCELED);

    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* An error occurs while performing a write. */
TEST_CASE(error, write, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    append_args(1, 64);
    append_invoke(0);

    test_aio_fill(&ctx, 0);

    append_wait_cb(1, RAFT_ERR_IO);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(error, oom, error_oom_params)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);

    test_heap_fault_enable(&f->heap);

    append_invoke(RAFT_ENOMEM);

    return MUNIT_OK;
}

/**
 * Close raft_io instance scenarios.
 */

TEST_SUITE(close);

TEST_SETUP(close, setup);
TEST_TEAR_DOWN(close, tear_down);

/* The write is closed while a write request is in progress. */
TEST_CASE(close, during_write, NULL)
{
    struct fixture *f = data;

    (void)params;

    append_args(1, 64);
    append_invoke(0);

    test_uv_run(&f->loop, 1);

    io_uv__close;

    append_wait_cb(1, 0);

    return MUNIT_OK;
}

/* When the writer gets close it tells the writer to close the segment that it's
 * currently writing. */
TEST_CASE(close, current_segment, NULL)
{
    struct fixture *f = data;

    (void)params;
    return MUNIT_OK;

    append_args(1, 64);
    append_invoke(0);
    append_wait_cb(1, 0);

    io_uv__close;

    test_uv_run(&f->loop, 1);

    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    return MUNIT_OK;
}
