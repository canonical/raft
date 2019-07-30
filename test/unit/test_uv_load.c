#include "../lib/dir.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

#include "../../src/byte.h"
#include "../../src/uv_encoding.h"

#define WORD_SIZE sizeof(uint64_t)

TEST_MODULE(uv_load);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n;
    int count;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    f->snapshot = NULL;
    f->entries = NULL;
    f->n = 0;
    f->count = 0;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    if (f->snapshot != NULL) {
        raft_configuration_close(&f->snapshot->configuration);
        raft_free(f->snapshot->bufs[0].base);
        raft_free(f->snapshot->bufs);
        raft_free(f->snapshot);
    }
    if (f->entries != NULL) {
        unsigned i;
        void *batch = NULL;
        munit_assert_int(f->n, >, 0);
        for (i = 0; i < f->n; i++) {
            if (f->entries[i].batch != batch) {
                batch = f->entries[i].batch;
                raft_free(batch);
            }
        }
        raft_free(f->entries);
    }
    TEAR_DOWN_UV;
}


/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define LOAD(RV)                                                 \
    {                                                            \
        int rv;                                                  \
        raft_term term;                                          \
        unsigned voted_for;                                      \
        rv = f->io.load(&f->io, &term, &voted_for, &f->snapshot, \
                        &f->start_index, &f->entries, &f->n);    \
        munit_assert_int(rv, ==, RV);                            \
    }

/******************************************************************************
 *
 * Success scenarios.
 *
 *****************************************************************************/

TEST_SUITE(success);
TEST_SETUP(success, setup);
TEST_TEAR_DOWN(success, tear_down);

TEST_CASE(success, ignore_unknown, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    char filename1[128];
    char filename2[128];

    (void)params;

    strcpy(filename1, "1-1");
    strcpy(filename2, "open-1");

    strcat(filename1, "garbage");
    strcat(filename2, "garbage");

    memset(buf, 0, sizeof buf);

    test_dir_write_file(f->dir, "garbage", buf, sizeof buf);
    test_dir_write_file(f->dir, filename1, buf, sizeof buf);
    test_dir_write_file(f->dir, filename2, buf, sizeof buf);

    LOAD(0);

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. */
TEST_CASE(success, closed_not_needed, NULL)
{
    /* TODO: We should support a trailing amount */
    return MUNIT_SKIP;
    struct fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 2, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 2, 123, buf, sizeof buf);
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);

    LOAD(0);

    /* The segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "1-1"));

    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
TEST_CASE(success, closed, NULL)
{
    struct fixture *f = data;

    (void)params;

    UV_WRITE_CLOSED_SEGMENT(1, 2, 1);
    UV_WRITE_CLOSED_SEGMENT(3, 1, 1);
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    LOAD(0);

    munit_assert_int(f->n, ==, 4);

    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
TEST_CASE(success, open_empty, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, "open-1", NULL, 0);

    LOAD(0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
TEST_CASE(success, open_all_zeros, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, "open-1", 256);

    LOAD(0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
TEST_CASE(success, open_not_all_zeros, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE + /* CRC32 checksum */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE + /* Entry term */
                WORD_SIZE + /* Entry type and data size */
                WORD_SIZE /* Entry data */];
    void *cursor = buf;

    (void)params;

    bytePut64(&cursor, 123);         /* Invalid checksums */
    bytePut64(&cursor, 1);           /* Number of entries */
    bytePut64(&cursor, 1);           /* Entry term */
    bytePut8(&cursor, RAFT_COMMAND); /* Entry type */
    bytePut8(&cursor, 0);            /* Unused */
    bytePut8(&cursor, 0);            /* Unused */
    bytePut8(&cursor, 0);            /* Unused */
    bytePut32(&cursor, 8);           /* Size of entry data */

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_append_file(f->dir, "open-1", buf, sizeof buf);

    LOAD(0);

    /* The segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    /* The first batch has been loaded */
    // munit_assert_int(f->loaded.n, ==, 1);

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
TEST_CASE(success, open_truncate, NULL)
{
    struct fixture *f = data;
    uint8_t buf[256];

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, "open-1", buf, sizeof buf);

    LOAD(0);

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
TEST_CASE(success, open_partial_bach, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE + /* Format version */
                WORD_SIZE + /* CRC32 checksums */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE /* Batch data */];
    void *cursor = buf;

    (void)params;

    bytePut64(&cursor, 1); /* Format version */
    bytePut64(&cursor, 0); /* CRC32 checksum */
    bytePut64(&cursor, 0); /* Number of entries */
    bytePut64(&cursor, 0); /* Batch data */

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    LOAD(0);

    /* The partially written segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
TEST_CASE(success, open_second, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    /* Second segment */
    UV_WRITE_OPEN_SEGMENT(2, 1, 1);

    LOAD(0);

    /* The first and second segments have been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));
    munit_assert_true(test_dir_has_file(f->dir, "2-2"));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
TEST_CASE(success, open_second_all_zeroes, NULL)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, "open-2", 256);

    LOAD(0);

    /* The first segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    /* The second segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
TEST_CASE(success, open, NULL)
{
    struct fixture *f = data;

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    LOAD(0);

    return MUNIT_OK;
}

/******************************************************************************
 *
 * Failure scenarios.
 *
 *****************************************************************************/

TEST_SUITE(error);
TEST_SETUP(error, setup);
TEST_TEAR_DOWN(error, tear_down);

/* The data directory has an open segment which has incomplete format data. */
TEST_CASE(error, short_format, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, "open-1", WORD_SIZE / 2);

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
TEST_CASE(error, short_preamble, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */ + WORD_SIZE /* Checksums */;

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
TEST_CASE(error, short_header, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE /* Partial batch header */;

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
TEST_CASE(error, short_data, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE + /* Entry term */
                    WORD_SIZE + /* Entry type and data size */
                    WORD_SIZE / 2 /* Partial entry data */;

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
TEST_CASE(error, corrupt_header, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */;
    uint8_t buf[WORD_SIZE];
    void *cursor = &buf;

    (void)params;

    /* Render invalid checksums */
    bytePut64(&cursor, 123);

    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "1-1", buf, sizeof buf, offset);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
TEST_CASE(error, corrupt_data, NULL)
{
    struct fixture *f = data;
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[WORD_SIZE / 2];
    void *cursor = buf;

    (void)params;

    /* Render an invalid data checksum. */
    bytePut32(&cursor, 123456789);

    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "1-1", buf, sizeof buf, offset);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
TEST_CASE(error, closed_bad_index, NULL)
{
    struct fixture *f = data;

    (void)params;

    UV_WRITE_CLOSED_SEGMENT(2, 1, 1);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
TEST_CASE(error, closed_empty, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, "1-1", NULL, 0);

    LOAD(RAFT_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
TEST_CASE(error, closed_bad_format, NULL)
{
    struct fixture *f = data;

    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};

    (void)params;

    test_dir_write_file(f->dir, "1-1", buf, sizeof buf);

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
TEST_CASE(error, open_no_access, NULL)
{
    struct fixture *f = data;

    (void)params;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_unreadable_file(f->dir, "open-1");

    LOAD(RAFT_IOERR);

    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
TEST_CASE(error, open_zero_format, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    bytePut64(&cursor, 0); /* Format version */

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    LOAD(RAFT_MALFORMED);

    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
TEST_CASE(error, open_bad_format, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;
    (void)params;

    bytePut64(&cursor, 2); /* Format version */

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    LOAD(RAFT_MALFORMED);

    return MUNIT_OK;
}
