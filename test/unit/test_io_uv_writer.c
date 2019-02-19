#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"
#include "../lib/uv.h"

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_uv_encoding.h"
#include "../../src/io_uv_writer.h"

/* Maximum number of blocks a segment can have */
#define __MAX_SEGMENT_BLOCKS 4

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;
    struct uv_loop_s loop;
    struct raft_logger logger;
    char *dir;
    size_t block_size;
    struct raft__io_uv_loader loader;
    struct raft__io_uv_preparer preparer;
    struct raft__io_uv_closer closer;
    struct raft__io_uv_writer writer;
    int count; /* To generate deterministic entry data */
    struct
    {
        int invoked; /* Number of times __append_cb was invoked */
        int status;  /* Last status passed to __append_cb */
    } append_cb;
    struct
    {
        int invoked; /* Number of times the close callback was invoked */
    } close_cb;
    struct
    {
        struct raft_entry *entries;
        unsigned n;
    } append_args;
};

static void __append_cb(struct raft__io_uv_writer_append *req, int status)
{
    struct fixture *f = req->data;
    unsigned i;

    f->append_cb.invoked++;
    f->append_cb.status = status;

    for (i = 0; i < req->n; i++) {
        raft_free(req->entries[i].buf.base);
    }

    raft_free((struct raft_entry *)req->entries);
    free(req);
}

static void __close_cb(struct raft__io_uv_writer *writer)
{
    struct fixture *f = writer->data;

    f->close_cb.invoked++;
}

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    unsigned n_blocks = __MAX_SEGMENT_BLOCKS;
    int rv;

    (void)user_data;
    (void)params;

    test_heap_setup(params, &f->heap);
    test_uv_setup(params, &f->loop);
    test_logger_setup(params, &f->logger, 1);

    f->dir = test_dir_setup(params);

    rv = raft__uv_file_block_size(f->dir, &f->block_size);
    munit_assert_int(rv, ==, 0);

    raft__io_uv_loader_init(&f->loader, &f->logger, f->dir);

    rv = raft__io_uv_preparer_init(&f->preparer, &f->loop, &f->logger, f->dir,
                                   f->block_size, n_blocks);
    munit_assert_int(rv, ==, 0);

    rv = raft__io_uv_closer_init(&f->closer, &f->loop, &f->logger, &f->loader,
                                 f->dir);
    munit_assert_int(rv, ==, 0);

    rv = raft__io_uv_writer_init(&f->writer, &f->loop, &f->logger, &f->preparer,
                                 &f->closer, f->dir, f->block_size, n_blocks);
    munit_assert_int(rv, ==, 0);

    raft__io_uv_writer_set_next_index(&f->writer, 1);

    f->writer.data = f;

    f->count = 0;

    f->append_cb.invoked = 0;
    f->append_cb.status = 0;

    f->append_args.entries = NULL;
    f->append_args.n = 0;

    f->close_cb.invoked = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    if (!raft__io_uv_writer_is_closing(&f->writer)) {
        raft__io_uv_writer_close(&f->writer, __close_cb);
    }

    raft__io_uv_preparer_close(&f->preparer, NULL);
    raft__io_uv_closer_close(&f->closer, NULL);

    test_uv_stop(&f->loop);

    munit_assert_int(f->close_cb.invoked, ==, 1);

    test_dir_tear_down(f->dir);

    test_logger_tear_down(&f->logger);
    test_uv_tear_down(&f->loop);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Set the arguments for the next append entries call. The append_args.entries
 * array will be populated with #N entries each of size #SIZE.
 */
#define __append_args(F, N, SIZE)                                            \
    {                                                                        \
        int i;                                                               \
                                                                             \
        F->append_args.entries = raft_malloc(N * sizeof(struct raft_entry)); \
        F->append_args.n = N;                                                \
                                                                             \
        munit_assert_ptr_not_null(F->append_args.entries);                   \
                                                                             \
        for (i = 0; i < N; i++) {                                            \
            struct raft_entry *entry = &F->append_args.entries[i];           \
            void *cursor;                                                    \
                                                                             \
            entry->term = 1;                                                 \
            entry->type = RAFT_LOG_COMMAND;                                  \
            entry->buf.base = munit_malloc(SIZE);                            \
            entry->buf.len = SIZE;                                           \
            entry->batch = NULL;                                             \
                                                                             \
            memset(entry->buf.base, 0, entry->buf.len);                      \
            cursor = entry->buf.base;                                        \
            raft__put64(&cursor, F->count);                                  \
            F->count++;                                                      \
        }                                                                    \
    }

/**
 * Assert that @raft__io_uv_writer_append returns the given code.
 */
#define __append_assert_result(F, RV)                                          \
    {                                                                          \
        struct raft__io_uv_writer_append *req;                                 \
        unsigned i;                                                            \
        int rv;                                                                \
                                                                               \
        req = munit_malloc(sizeof *req);                                       \
        req->data = F;                                                         \
                                                                               \
        rv =                                                                   \
            raft__io_uv_writer_append(&F->writer, req, F->append_args.entries, \
                                      F->append_args.n, __append_cb);          \
        munit_assert_int(rv, ==, RV);                                          \
                                                                               \
        if (rv != 0) {                                                         \
            for (i = 0; i < f->append_args.n; i++) {                           \
                raft_free(f->append_args.entries[i].buf.base);                 \
            }                                                                  \
                                                                               \
            raft_free(f->append_args.entries);                                 \
            free(req);                                                         \
        }                                                                      \
    }

/**
 * Wait for the given number of append request callbacks to fire and check the
 * last status.
 */
#define __append_assert_cb(F, N, STATUS)                         \
    {                                                            \
        int i;                                                   \
                                                                 \
        /* Run the loop until the append request is completed */ \
        for (i = 0; i < 5; i++) {                                \
            test_uv_run(&F->loop, 1);                            \
                                                                 \
            if (F->append_cb.invoked == N) {                     \
                break;                                           \
            }                                                    \
        }                                                        \
                                                                 \
        munit_assert_int(F->append_cb.invoked, ==, N);           \
        munit_assert_int(F->append_cb.status, ==, STATUS);       \
                                                                 \
        F->append_cb.invoked = 0;                                \
    }

/**
 * Assert the open segment with the given counter has format version 1 and N
 * entries with a total data size of S bytes.
 */
#define __append_assert_segment(F, COUNTER, N, SIZE)                          \
    {                                                                         \
        struct raft_buffer buf;                                               \
        const void *cursor;                                                   \
        char filename[strlen("open-N") + 1];                                  \
        unsigned i = 0;                                                       \
        size_t total_data_size = 0;                                           \
                                                                              \
        sprintf(filename, "open-%d", COUNTER);                                \
                                                                              \
        buf.len = __MAX_SEGMENT_BLOCKS * F->block_size;                       \
        buf.base = munit_malloc(buf.len);                                     \
                                                                              \
        test_dir_read_file(F->dir, filename, buf.base, buf.len);              \
                                                                              \
        cursor = buf.base;                                                    \
        munit_assert_int(raft__get64(&cursor), ==, 1);                        \
                                                                              \
        while (i < N) {                                                       \
            unsigned crc1 = raft__get32(&cursor);                             \
            unsigned crc2 = raft__get32(&cursor);                             \
            const void *header = cursor;                                      \
            const void *data;                                                 \
            unsigned n = raft__get64(&cursor);                                \
            struct raft_entry *entries = munit_malloc(n * sizeof *entries);   \
            unsigned j;                                                       \
            unsigned crc;                                                     \
            size_t data_size = 0;                                             \
                                                                              \
            for (j = 0; j < n; j++) {                                         \
                struct raft_entry *entry = &entries[j];                       \
                                                                              \
                entry->term = raft__get64(&cursor);                           \
                entry->type = raft__get8(&cursor);                            \
                raft__get8(&cursor);                                          \
                raft__get8(&cursor);                                          \
                raft__get8(&cursor);                                          \
                entry->buf.len = raft__get32(&cursor);                        \
                                                                              \
                munit_assert_int(entry->term, ==, 1);                         \
                munit_assert_int(entry->type, ==, RAFT_LOG_COMMAND);          \
                                                                              \
                data_size += entry->buf.len;                                  \
            }                                                                 \
                                                                              \
            crc = raft__crc32(header, raft_io_uv_sizeof__batch_header(n), 0); \
            munit_assert_int(crc, ==, crc1);                                  \
                                                                              \
            data = cursor;                                                    \
                                                                              \
            for (j = 0; j < n; j++) {                                         \
                struct raft_entry *entry = &entries[j];                       \
                uint64_t value;                                               \
                                                                              \
                value = raft__flip64(*(uint64_t *)cursor);                    \
                munit_assert_int(value, ==, i);                               \
                                                                              \
                cursor += entry->buf.len;                                     \
                                                                              \
                i++;                                                          \
            }                                                                 \
                                                                              \
            crc = raft__crc32(data, data_size, 0);                            \
            munit_assert_int(crc, ==, crc2);                                  \
                                                                              \
            free(entries);                                                    \
                                                                              \
            total_data_size += data_size;                                     \
        }                                                                     \
                                                                              \
        munit_assert_int(total_data_size, ==, SIZE);                          \
        free(buf.base);                                                       \
    }

/**
 * Perform an append request and wait for it to complete successfully.
 */
#define __append(F)                   \
    {                                 \
        __append_assert_result(F, 0); \
        __append_assert_cb(F, 1, 0);  \
    }

/**
 * Assert that @raft__io_uv_writer_truncate returns the given code.
 */
#define __truncate(F, INDEX, RV)                             \
    {                                                        \
        int rv;                                              \
                                                             \
        rv = raft__io_uv_writer_truncate(&F->writer, INDEX); \
        munit_assert_int(rv, ==, RV);                        \
    }

/**
 * Close the fixture writer.
 */
#define __close(F)                                        \
    {                                                     \
        raft__io_uv_writer_close(&F->writer, __close_cb); \
    }

/**
 * raft__uv_writer_append
 */

/* Append the very first batch of entries. */
static MunitResult test_append_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append(f);
    __append_assert_segment(f, 1, 1, 64);

    return MUNIT_OK;
}

/* Write the very first entry and then another one, both fitting in the same
 * block. */
static MunitResult test_append_fit_block(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append(f);

    __append_args(f, 1, 64);
    __append(f);

    __append_assert_segment(f, 1, 2, 128);

    return MUNIT_OK;
}

/* Write an entry that fills the first block exactly and then another one. */
static MunitResult test_append_match_block(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    size_t size;

    (void)params;

    size = f->writer.block_size -
           (sizeof(uint64_t) + /* Format */
            sizeof(uint64_t) + /* Checksums */
            raft_io_uv_sizeof__batch_header(1)) /* Header */;

    __append_args(f, 1, size);
    __append(f);

    __append_args(f, 1, 64);
    __append(f);

    __append_assert_segment(f, 1, 2, size + 64);

    return MUNIT_OK;
}

/* Write an entry that exceeds the first block, then another one that fits in
 * the second block, then a third one that fills the rest of the second block
 * plus the whole third block exactly, and finally a fourth entry that fits in
 * the fourth block */
static MunitResult test_append_exceed_block(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t written;
    size_t size1;
    size_t size2;

    (void)params;

    size1 = f->writer.block_size;

    __append_args(f, 1, size1);
    __append(f);

    __append_args(f, 1, 64);
    __append(f);

    written = sizeof(uint64_t) +     /* Format version */
              2 * sizeof(uint32_t) + /* CRC sums of first batch */
              raft_io_uv_sizeof__batch_header(1) /* Header of first batch */ +
              size1 /* Size of first batch */ +
              2 * sizeof(uint32_t) /* CRC sums of second batch */ +
              raft_io_uv_sizeof__batch_header(1) /* Header of second batch */ +
              64 /* Size of second batch */;

    /* Write a third entry that fills the second block exactly */
    size2 = f->writer.block_size - (written % f->writer.block_size);
    size2 -= (2 * sizeof(uint32_t) + raft_io_uv_sizeof__batch_header(1));
    size2 += f->writer.block_size;

    __append_args(f, 1, size2);
    __append(f);

    /* Write a fourth entry */
    __append_args(f, 1, 64);
    __append(f);

    __append_assert_segment(f, 1, 4, size1 + 64 + size2 + 64);

    return MUNIT_OK;
}

/* If an append request is submitted before the write operation of the previous
 * append request is started, then a single write will be performed for both
 * requests. */
static MunitResult test_append_batch(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    __append_assert_cb(f, 2, 0);

    return MUNIT_OK;
}

/* An append request submitted while a write operation is in progress gets
 * executed only when the write completes. */
static MunitResult test_append_wait(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    test_uv_run(&f->loop, 1);

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    __append_assert_cb(f, 1, 0);
    __append_assert_cb(f, 1, 0);

    return MUNIT_OK;
}

/* The counters of the open segments get increased as they are closed. */
static MunitResult test_append_counter(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;
    size_t size = f->writer.block_size;
    int i;

    (void)params;

    for (i = 0; i < 10; i++) {
        __append_args(f, 1, size);
        __append(f);
    }

    munit_assert_true(
        test_dir_has_file(f->dir, "00000000000000000001-00000000000000000003"));
    munit_assert_true(
        test_dir_has_file(f->dir, "00000000000000000004-00000000000000000006"));
    munit_assert_true(test_dir_has_file(f->dir, "open-4"));

    return MUNIT_OK;
}

/* If the writer is closed, all pending append requests get canceled. */
static MunitResult test_append_cancel(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    __close(f);

    __append_assert_cb(f, 1, RAFT_ERR_IO_CANCELED);

    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The batch of entries to append is too big. */
static MunitResult test_append_too_big(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 4, f->block_size);
    __append_assert_result(f, RAFT_ERR_IO_TOOBIG);

    return MUNIT_OK;
}

/* An error occurs while performing a write. */
static MunitResult test_append_write_error(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    (void)params;

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    test_aio_fill(&ctx, 0);

    __append_assert_cb(f, 1, RAFT_ERR_IO);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_append_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);

    test_heap_fault_enable(&f->heap);

    __append_assert_result(f, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static char *append_oom_heap_fault_delay[] = {"0", NULL};
static char *append_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum append_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, append_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, append_oom_heap_fault_repeat},
    {NULL, NULL},
};

#define __test_append(NAME, FUNC, PARAMS)                         \
    {                                                             \
        "/" NAME, test_append_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest append_tests[] = {
    __test_append("first", first, NULL),
    __test_append("fit-block", fit_block, NULL),
    __test_append("match-block", match_block, NULL),
    __test_append("exceed-block", exceed_block, NULL),
    __test_append("batch", batch, NULL),
    __test_append("wait", wait, NULL),
    __test_append("counter", counter, NULL),
    __test_append("too-big", too_big, NULL),
    __test_append("cancel", cancel, NULL),
    __test_append("write-error", write_error, NULL),
    __test_append("oom", oom, append_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__uv_writer_close
 */

/* Truncate the log after the very first entry. */
static MunitResult test_truncate_after_first(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append(f);

    __truncate(f, 1, 0);

    __append_args(f, 1, 64);
    __append(f);

    return MUNIT_OK;
}

#define __test_truncate(NAME, FUNC, PARAMS)                         \
    {                                                               \
        "/" NAME, test_truncate_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest truncate_tests[] = {
    __test_truncate("after-first", after_first, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft__uv_writer_close
 */

/* It's possilble to close the writer right after initialization. */
static MunitResult test_close_noop(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;

    (void)params;

    __close(f);

    return MUNIT_OK;
}

/* The write is closed while a write request is in progress. */
static MunitResult test_close_during_write(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append_assert_result(f, 0);

    test_uv_run(&f->loop, 1);

    __close(f);

    __append_assert_cb(f, 1, 0);

    return MUNIT_OK;
}

/* When the writer gets close it tells the writer to close the segment that it's
 * currently writing. */
static MunitResult test_close_current_segment(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    __append_args(f, 1, 64);
    __append(f);

    __close(f);

    test_uv_run(&f->loop, 1);

    munit_assert_true(
        test_dir_has_file(f->dir, "00000000000000000001-00000000000000000001"));

    return MUNIT_OK;
}

#define __test_close(NAME, FUNC, PARAMS)                         \
    {                                                            \
        "/" NAME, test_close_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest close_tests[] = {
    __test_close("noop", noop, NULL),
    __test_close("during-write", during_write, NULL),
    __test_close("current-segment", current_segment, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_writer_suites[] = {
    {"/append", append_tests, NULL, 1, 0},
    {"/truncate", truncate_tests, NULL, 1, 0},
    {"/close", close_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
