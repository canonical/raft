#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_uv_encoding.h"
#include "../../src/io_uv_loader.h"

/**
 * Filename of the first open segments.
 */
#define __OPEN_FILENAME_1 "open-1"
#define __OPEN_FILENAME_2 "open-2"
#define __OPEN_FILENAME_3 "open-3"

/**
 * Filename of the first closed segments.
 */
#define __CLOSED_FILENAME_1 "00000000000000000001-00000000000000000001"
#define __CLOSED_FILENAME_2 "00000000000000000002-00000000000000000002"

#define __WORD_SIZE sizeof(uint64_t)

/**
 * Test suite
 */

struct fixture
{
    struct raft_heap heap;     /* Testable allocator */
    struct raft_logger logger; /* Test logger */
    char *dir;                 /* Data directory */
    struct raft__io_uv_loader loader;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n;
    int count;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, 1);

    f->dir = test_dir_setup(params);

    raft__io_uv_loader_init(&f->loader, &f->logger, f->dir);

    f->start_index = 1;

    f->count = 0;

    f->entries = NULL;
    f->n = 0;

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    void *batch = NULL;
    size_t i;

    /* Free any loaded entry */
    for (i = 0; i < f->n; i++) {
        struct raft_entry *entry = &f->entries[i];

        if (entry->batch != batch) {
            batch = entry->batch;
            raft_free(batch);
        }
    }

    if (f->entries != NULL) {
        raft_free(f->entries);
    }

    test_dir_tear_down(f->dir);

    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Write a valid segment with #M batches.
 */
#define __segments_prepare(F, FILENAME, M)                                    \
    {                                                                         \
        size_t size = __WORD_SIZE /* Format version */;                       \
        int i;                                                                \
        uint8_t *buf;                                                         \
        void *cursor;                                                         \
        unsigned crc1;                                                        \
        unsigned crc2;                                                        \
        uint8_t *batch; /* Start of the batch */                              \
        size_t header_size = raft_io_uv_sizeof__batch_header(1);              \
        size_t data_size = __WORD_SIZE;                                       \
                                                                              \
        size += (__WORD_SIZE /* Checksums */ + header_size + data_size) * M;  \
        buf = munit_malloc(size);                                             \
        cursor = buf;                                                         \
        raft__put64(&cursor, 1); /* Format version */                         \
        batch = cursor;                                                       \
                                                                              \
        for (i = 0; i < M; i++) {                                             \
            F->count++;                                                       \
                                                                              \
            raft__put64(&cursor, 0);               /* CRC sums placeholder */ \
            raft__put64(&cursor, 1);               /* Number of entries */    \
            raft__put64(&cursor, 1);               /* Entry term */           \
            raft__put8(&cursor, RAFT_LOG_COMMAND); /* Entry type */           \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put32(&cursor, 8);               /* Size of entry data */   \
            raft__put64(&cursor, F->count);        /* Entry data */           \
                                                                              \
            cursor = batch + __WORD_SIZE;                                     \
            crc1 = raft__crc32(cursor, header_size, 0);                       \
            crc2 = raft__crc32(cursor + header_size, data_size, 0);           \
            cursor = batch;                                                   \
            raft__put32(&cursor, crc1); /* Header checksum */                 \
            raft__put32(&cursor, crc2); /* Data checksum */                   \
            batch += __WORD_SIZE + header_size + data_size;                   \
            cursor = batch;                                                   \
        }                                                                     \
                                                                              \
        test_dir_write_file(F->dir, FILENAME, buf, size);                     \
        free(buf);                                                            \
    }

/**
 * Write a open segment with index #N and #M batches.
 */
#define __segments_prepare_open(F, N, M)              \
    {                                                 \
        char filename[strlen(__OPEN_FILENAME_1) + 1]; \
                                                      \
        sprintf(filename, "open-%d", N);              \
                                                      \
        __segments_prepare(F, filename, M);           \
    }

/**
 * Write a closed segment with first index #N and #M batches.
 */
#define __segments_prepare_closed(F, N, M)                  \
    {                                                       \
        char filename[strlen(__CLOSED_FILENAME_1) + 1];     \
                                                            \
        sprintf(filename, "%020llu-%020llu", (raft_index)N, \
                (raft_index)(N + M - 1));                   \
                                                            \
        __segments_prepare(F, filename, M);                 \
    }

/**
 * Assert that @raft__io_uv_loader_load_all returns the given code.
 */
#define __load_all_assert_result(F, RV)                              \
    {                                                                \
        int rv;                                                      \
                                                                     \
        rv = raft__io_uv_loader_load_all(&F->loader, F->start_index, \
                                         &F->entries, &F->n);        \
        munit_assert_int(rv, ==, RV);                                \
    }

/**
 * raft__io_uv_loader_segments
 */

/* Unknown files in the data directory are ignored. */
static MunitResult test_segments_ignore(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    uint8_t buf[8];
    char filename1[128];
    char filename2[128];

    (void)params;

    strcpy(filename1, __CLOSED_FILENAME_1);
    strcpy(filename2, __OPEN_FILENAME_1);

    strcat(filename1, "garbage");
    strcat(filename2, "garbage");

    memset(buf, 0, sizeof buf);

    test_dir_write_file(f->dir, "garbage", buf, sizeof buf);
    test_dir_write_file(f->dir, filename1, buf, sizeof buf);
    test_dir_write_file(f->dir, filename2, buf, sizeof buf);

    __load_all_assert_result(f, 0);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete format data. */
static MunitResult test_segments_short_format(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, __WORD_SIZE / 2);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
static MunitResult test_segments_short_preamble(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    size_t offset =
        __WORD_SIZE /* Format version */ + __WORD_SIZE /* Checksums */;

    (void)params;

    __segments_prepare_open(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
static MunitResult test_segments_short_header(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE /* Partial batch header */;

    (void)params;

    __segments_prepare_open(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
static MunitResult test_segments_short_data(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE + /* Entry term */
                    __WORD_SIZE + /* Entry type and data size */
                    __WORD_SIZE / 2 /* Partial entry data */;

    (void)params;

    __segments_prepare_open(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
static MunitResult test_segments_corrupt_header(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */;
    uint8_t buf[__WORD_SIZE];
    void *cursor = &buf;

    (void)params;

    /* Render invalid checksums */
    raft__put64(&cursor, 123);

    __segments_prepare_closed(f, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __load_all_assert_result(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
static MunitResult test_segments_corrupt_data(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */ +
                    __WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[__WORD_SIZE / 2];
    void *cursor = buf;

    (void)params;

    /* Render an invalid data checksum. */
    raft__put32(&cursor, 123456789);

    __segments_prepare_closed(f, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __load_all_assert_result(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
static MunitResult test_segments_closed_bad_index(const MunitParameter params[],
                                                  void *data)
{
    struct fixture *f = data;

    (void)params;

    __segments_prepare_closed(f, 2, 1);

    __load_all_assert_result(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
static MunitResult test_segments_closed_empty(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, NULL, 0);

    __load_all_assert_result(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
static MunitResult test_segments_closed_bad_format(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed. */
static MunitResult test_segments_closed_not_needed(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    f->start_index = 2;

    __load_all_assert_result(f, 0);

    /* The segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
static MunitResult test_segments_closed(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    __segments_prepare_closed(f, 1, 2);
    __segments_prepare_closed(f, 3, 1);
    __segments_prepare_open(f, 1, 1);

    __load_all_assert_result(f, 0);

    //__assert_result_entries(f, 4);

    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
static MunitResult test_segments_open_no_access(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;

    (void)params;

    __segments_prepare_open(f, 1, 1);

    test_dir_unreadable_file(f->dir, __OPEN_FILENAME_1);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
static MunitResult test_segments_open_empty(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __OPEN_FILENAME_1, NULL, 0);

    __load_all_assert_result(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
static MunitResult test_segments_open_zero_format(const MunitParameter params[],
                                                  void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 0); /* Format version */

    __segments_prepare_open(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
static MunitResult test_segments_open_bad_format(const MunitParameter params[],
                                                 void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 2); /* Format version */

    __segments_prepare_open(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
static MunitResult test_segments_open_all_zeros(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, 256);

    __load_all_assert_result(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
static MunitResult test_segments_open_not_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE + /* CRC32 checksum */
                __WORD_SIZE + /* Number of entries */
                __WORD_SIZE + /* Entry term */
                __WORD_SIZE + /* Entry type and data size */
                __WORD_SIZE /* Entry data */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 123);             /* Invalid checksums */
    raft__put64(&cursor, 1);               /* Number of entries */
    raft__put64(&cursor, 1);               /* Entry term */
    raft__put8(&cursor, RAFT_LOG_COMMAND); /* Entry type */
    raft__put8(&cursor, 0);                /* Unused */
    raft__put8(&cursor, 0);                /* Unused */
    raft__put8(&cursor, 0);                /* Unused */
    raft__put32(&cursor, 8);               /* Size of entry data */

    __segments_prepare_open(f, 1, 1);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load_all_assert_result(f, 0);

    /* The segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The first batch has been loaded */
    // munit_assert_int(f->loaded.n, ==, 1);

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
static MunitResult test_segments_open_truncate(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    uint8_t buf[256];

    (void)params;

    __segments_prepare_open(f, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load_all_assert_result(f, 0);

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
static MunitResult test_segments_open_partial_batch(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE + /* Format version */
                __WORD_SIZE + /* CRC32 checksums */
                __WORD_SIZE + /* Number of entries */
                __WORD_SIZE /* Batch data */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 1); /* Format version */
    raft__put64(&cursor, 0); /* CRC32 checksum */
    raft__put64(&cursor, 0); /* Number of entries */
    raft__put64(&cursor, 0); /* Batch data */

    __segments_prepare_open(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_assert_result(f, 0);

    /* The partially written segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
static MunitResult test_segments_open_second(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    __segments_prepare_open(f, 1, 1);

    /* Second segment */
    __segments_prepare_open(f, 2, 1);

    __load_all_assert_result(f, 0);

    /* The first and second segments have been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
static MunitResult test_segments_open_second_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    __segments_prepare_open(f, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_2, 256);

    __load_all_assert_result(f, 0);

    /* The first segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The second segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
static MunitResult test_segments_open(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __segments_prepare_open(f, 1, 1);

    __load_all_assert_result(f, 0);

    return MUNIT_OK;
}

#define __test_segments(NAME, FUNC, PARAMS)                         \
    {                                                               \
        "/" NAME, test_segments_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest segments_tests[] = {
    __test_segments("ignore", ignore, NULL),
    __test_segments("short-format", short_format, NULL),
    __test_segments("short-preamble", short_preamble, NULL),
    __test_segments("short-header", short_header, NULL),
    __test_segments("short-data", short_data, NULL),
    __test_segments("corrupt-header", corrupt_header, NULL),
    __test_segments("corrupt-data", corrupt_data, NULL),
    __test_segments("closed-bad-index", closed_bad_index, NULL),
    __test_segments("closed-empty", closed_empty, NULL),
    __test_segments("closed-bad-format", closed_bad_format, NULL),
    __test_segments("closed-not-needed", closed_not_needed, NULL),
    __test_segments("closed", closed, NULL),
    __test_segments("open-no-access", open_no_access, NULL),
    __test_segments("open-empty", open_empty, NULL),
    __test_segments("open-zero-format", open_zero_format, NULL),
    __test_segments("open-bad-format", open_bad_format, NULL),
    __test_segments("open-all-zeros", open_all_zeros, NULL),
    __test_segments("open-not-all-zeros", open_not_all_zeros, NULL),
    __test_segments("open-truncate", open_truncate, NULL),
    __test_segments("open-partial-batch", open_partial_batch, NULL),
    __test_segments("open-second", open_second, NULL),
    __test_segments("open-second-all-zeros", open_second_all_zeros, NULL),
    __test_segments("open", open, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite raft_io_uv_loader_suites[] = {
    {"/segments", segments_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
