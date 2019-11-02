#include "../../src/byte.h"
#include "../../src/entry.h"
#include "../../src/snapshot.h"
#include "../../src/uv_encoding.h"
#include "../lib/dir.h"
#include "../lib/heap.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

#define WORD_SIZE sizeof(uint64_t)

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    unsigned trailing;
    raft_term term;
    unsigned voted_for;
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
    f->trailing = 10;
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
        snapshotDestroy(f->snapshot);
    }
    if (f->entries != NULL) {
        entryBatchesDestroy(f->entries, f->n);
    }
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Invoke raft_io->load(). */
#define LOAD_RV                                                            \
    f->io.load(&f->io, f->trailing, &f->term, &f->voted_for, &f->snapshot, \
               &f->start_index, &f->entries, &f->n)
#define LOAD munit_assert_int(LOAD_RV, ==, 0)
#define LOAD_ERROR(RV) munit_assert_int(LOAD_RV, ==, RV)

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

#define CLOSED_SEGMENT_FILENAME(START, END) \
    "000000000000000" #START                \
    "-"                                     \
    "000000000000000" #END

/* Check if closed segment file exists. */
#define HAS_CLOSED_SEGMENT_FILE(START, END) \
    test_dir_has_file(f->dir, CLOSED_SEGMENT_FILENAME(START, END))

/* Check if open segment file exists. */
#define HAS_OPEN_SEGMENT_FILE(COUNT) test_dir_has_file(f->dir, "open-" #COUNT)

/* Check if snapshot files exist. */
#define HAS_SNAPSHOT_META_FILE(TERM, INDEX, TIMESTAMP) \
    test_dir_has_file(f->dir,                          \
                      "snapshot-" #TERM "-" #INDEX "-" #TIMESTAMP ".meta")
#define HAS_SNAPSHOT_DATA_FILE(TERM, INDEX, TIMESTAMP) \
    test_dir_has_file(f->dir, "snapshot-" #TERM "-" #INDEX "-" #TIMESTAMP)

SUITE(UvLoad)

/******************************************************************************
 *
 * UvLoad
 *
 *****************************************************************************/

/* File that are not part of the raft state are ignored. */
TEST(UvLoad, ignoreUnknownFiles, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_write_file_with_zeros(f->dir, "garbage", 128);
    test_dir_write_file_with_zeros(
        f->dir,
        "0000000000000000000000000001-0000000000000000000000000001garbage",
        128);
    test_dir_write_file_with_zeros(f->dir, "open-1garbage", 128);
    LOAD;
    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
TEST(UvLoad, bothOpenAndClosedSegments, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    UV_WRITE_CLOSED_SEGMENT(1, 2, 1);
    UV_WRITE_CLOSED_SEGMENT(3, 1, 1);
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    LOAD;
    munit_assert_int(f->n, ==, 4);
    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
TEST(UvLoad, emptyOpenSegment, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_write_file(f->dir, "open-1", NULL, 0);
    LOAD;
    /* The empty segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
TEST(UvLoad, openSegmentWithTrailingZeros, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_write_file_with_zeros(f->dir, "open-1", 256);
    LOAD;
    /* The empty segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
TEST(UvLoad, openSegmentWithNonZeroData, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE + /* CRC32 checksum */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE + /* Entry term */
                WORD_SIZE + /* Entry type and data size */
                WORD_SIZE /* Entry data */];
    void *cursor = buf;

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

    LOAD;

    /* The segment has been renamed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));

    /* The first batch has been loaded */
    // munit_assert_int(f->loaded.n, ==, 1);

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
TEST(UvLoad, openSegmentWithIncompleteBatch, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[256];

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, "open-1", buf, sizeof buf);

    LOAD;

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
TEST(UvLoad, openSegmentWithIncompleteFirstBatch, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE + /* Format version */
                WORD_SIZE + /* CRC32 checksums */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE /* Batch data */];
    void *cursor = buf;

    bytePut64(&cursor, 1); /* Format version */
    bytePut64(&cursor, 0); /* CRC32 checksum */
    bytePut64(&cursor, 0); /* Number of entries */
    bytePut64(&cursor, 0); /* Batch data */

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    LOAD;

    /* The partially written segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
TEST(UvLoad, twoOpenSegments, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    /* First segment. */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    /* Second segment */
    UV_WRITE_OPEN_SEGMENT(2, 1, 1);

    LOAD;

    /* The first and second segments have been renamed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(2));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(2, 2));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
TEST(UvLoad, secondOpenSegmentIsAllZeros, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    /* First segment. */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, "open-2", 256);

    LOAD;

    /* The first segment has been renamed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(1));
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));

    /* The second segment has been removed. */
    munit_assert_false(HAS_OPEN_SEGMENT_FILE(2));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
TEST(UvLoad, openSegment, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);

    LOAD;

    return MUNIT_OK;
}

/* There are several snapshots, including an incomplete one. The last one is
 * loaded and the incomplete or older ones are removed.  */
TEST(UvLoad, manySnapshots, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_SNAPSHOT_META(f->dir, 1 /* term */, 8 /* index */,
                           123 /* timestamp */, 1 /* n servers */,
                           1 /* index */);

    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 8 /* index */, 456 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);

    UV_WRITE_SNAPSHOT(f->dir, 2 /* term */, 6 /* index */, 789 /* timestamp */,
                      2 /* n servers */, 3 /* conf index */, buf /* data */,
                      sizeof buf);

    UV_WRITE_SNAPSHOT(f->dir, 2 /* term */, 9 /* index */, 999 /* timestamp */,
                      2 /* n servers */, 3 /* conf index */, buf /* data */,
                      sizeof buf);
    LOAD;

    /* Only the last two snapshot files are kept. */
    munit_assert_false(HAS_SNAPSHOT_META_FILE(1, 8, 123));

    munit_assert_false(HAS_SNAPSHOT_META_FILE(1, 8, 456));
    munit_assert_false(HAS_SNAPSHOT_DATA_FILE(1, 8, 456));

    munit_assert_true(HAS_SNAPSHOT_META_FILE(2, 6, 789));
    munit_assert_true(HAS_SNAPSHOT_DATA_FILE(2, 6, 789));

    munit_assert_true(HAS_SNAPSHOT_META_FILE(2, 9, 999));
    munit_assert_true(HAS_SNAPSHOT_DATA_FILE(2, 9, 999));

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. We still keep those segments
 * and just let the next snapshot logic delete them. */
TEST(UvLoad, closedSegmentWithEntriesBehindSnapshot, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 2 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);
    LOAD;
    munit_assert_true(HAS_CLOSED_SEGMENT_FILE(1, 1));
    munit_assert_int(f->start_index, ==, 3);
    munit_assert_int(f->n, ==, 0);
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. However it also has an open
 * segment that has enough entries to reach the snapshot last index. */
TEST(UvLoad, openSegmentWithEntriesPastSnapshot, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 2 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    UV_WRITE_CLOSED_SEGMENT(1 /* first index */, 1 /* n entries */,
                            1 /* data */);
    UV_WRITE_OPEN_SEGMENT(1 /* counter */, 1 /* n entries */, 2 /* data */);
    LOAD;
    munit_assert_int(f->start_index, ==, 1);
    munit_assert_int(f->n, ==, 2);
    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. It also has an open segment,
 * however that does not have enough entries to reach the snapshot last
 * index. */
TEST(UvLoad, openSegmentWithEntriesBehindSnapshot, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 3 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    UV_WRITE_CLOSED_SEGMENT(1 /* first index */, 1 /* n entries */,
                            1 /* data */);
    UV_WRITE_OPEN_SEGMENT(1 /* counter */, 1 /* n entries */, 2 /* data */);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has several closed segments, all with entries compatible
 * with the snapshot. */
TEST(UvLoad, closedSegmentsOverlappingWithSnapshot, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);
    UV_WRITE_CLOSED_SEGMENT(2, 2, 2);
    UV_WRITE_CLOSED_SEGMENT(4, 3, 4);
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 4 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    LOAD;
    munit_assert_int(f->start_index, ==, 1);
    munit_assert_int(f->n, ==, 6);
    return MUNIT_OK;
}

/* The data directory has several closed segments, some of which have a gap,
 * which is still compatible with the snapshot. */
TEST(UvLoad, nonContiguousClosedSegments, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);
    UV_WRITE_CLOSED_SEGMENT(4, 3, 4);
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 4 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    LOAD;
    munit_assert_int(f->start_index, ==, 4);
    munit_assert_int(f->n, ==, 3);
    return MUNIT_OK;
}

/* If the data directory has a closed segment whose start index is beyond the
 * snapshot's last index, an error is returned. */
TEST(UvLoad, closedSegmentWithEntriesPastSnapshot, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8];
    UV_WRITE_CLOSED_SEGMENT(6, 3, 6);
    UV_WRITE_SNAPSHOT(f->dir, 1 /* term */, 4 /* index */, 123 /* timestamp */,
                      1 /* n servers */, 1 /* conf index */, buf /* data */,
                      sizeof buf);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete format data. */
TEST(UvLoad, openSegmentWithIncompleteFormat, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_write_file_with_zeros(f->dir, "open-1", WORD_SIZE / 2);
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
TEST(UvLoad, openSegmentWithIncompletePreamble, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */ + WORD_SIZE /* Checksums */;
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_truncate_file(f->dir, "open-1", offset);
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
TEST(UvLoad, openSegmentWithIncompleteBatchHeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE /* Partial batch header */;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_truncate_file(f->dir, "open-1", offset);
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
TEST(UvLoad, openSegmentWithIncompleteBatchData, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE + /* Entry term */
                    WORD_SIZE + /* Entry type and data size */
                    WORD_SIZE / 2 /* Partial entry data */;

    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_truncate_file(f->dir, "open-1", offset);
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
TEST(UvLoad, openSegmentWithCorruptedBatchHeader, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */;
    uint8_t buf[WORD_SIZE];
    void *cursor = &buf;
    /* Render invalid checksums */
    bytePut64(&cursor, 123);
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);
    test_dir_overwrite_file(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), buf,
                            sizeof buf, offset);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
TEST(UvLoad, openSegmentWithCorruptedBatchData, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[WORD_SIZE / 2];
    void *cursor = buf;
    /* Render an invalid data checksum. */
    bytePut32(&cursor, 123456789);
    UV_WRITE_CLOSED_SEGMENT(1, 1, 1);
    test_dir_overwrite_file(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), buf,
                            sizeof buf, offset);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
TEST(UvLoad, closedSegmentWithBadIndex, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    UV_WRITE_CLOSED_SEGMENT(2, 1, 1);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
TEST(UvLoad, emptyClosedSegment, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    test_dir_write_file(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), NULL, 0);
    LOAD_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
TEST(UvLoad, closedSegmentWithBadFormat, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};
    test_dir_write_file(f->dir, CLOSED_SEGMENT_FILENAME(1, 1), buf, sizeof buf);
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
TEST(UvLoad, openSegmentWithNoAccessPermission, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_unreadable_file(f->dir, "open-1");
    LOAD_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
TEST(UvLoad, openSegmentWithZeroFormatAndThenData, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;
    bytePut64(&cursor, 0); /* Format version */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);
    LOAD_ERROR(RAFT_MALFORMED);
    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
TEST(UvLoad, openSegmentWithBadFormat, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;
    bytePut64(&cursor, 2); /* Format version */
    UV_WRITE_OPEN_SEGMENT(1, 1, 1);
    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);
    LOAD_ERROR(RAFT_MALFORMED);
    return MUNIT_OK;
}
