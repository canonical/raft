#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/io_uv.h"
#include "../lib/runner.h"

#include "../../src/byte.h"
#include "../../src/io_uv_encoding.h"
#include "../../src/io_uv_load.h"

#define WORD_SIZE sizeof(uint64_t)

TEST_MODULE(io_uv_load);

/**
 * io_uv__load_list
 */

TEST_SUITE(list);
TEST_GROUP(list, success);

struct list__fixture
{
    IO_UV_FIXTURE;
    struct io_uv__snapshot_meta *snapshots;
    size_t n_snapshots;
    struct io_uv__segment_meta *segments;
    size_t n_segments;
};

TEST_SETUP(list)
{
    struct list__fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->snapshots = NULL;
    f->segments = NULL;
    return f;
}

TEST_TEAR_DOWN(list)
{
    struct list__fixture *f = data;
    if (f->snapshots != NULL) {
        raft_free(f->snapshots);
    }
    if (f->segments != NULL) {
        raft_free(f->segments);
    }
    IO_UV_TEAR_DOWN;
}

#define list__invoke(RV)                                             \
    {                                                                \
        int rv;                                                      \
        rv = io_uv__load_list(f->uv, &f->snapshots, &f->n_snapshots, \
                              &f->segments, &f->n_segments);         \
        munit_assert_int(rv, ==, RV);                                \
    }

#define list__assert_snapshot(I, TERM, INDEX, TIMESTAMP) \
    munit_assert_int(f->snapshots[I].term, ==, TERM);    \
    munit_assert_int(f->snapshots[I].index, ==, INDEX);  \
    munit_assert_int(f->snapshots[I].timestamp, ==, TIMESTAMP);

/* There are no snapshot metadata files */
TEST_CASE(list, success, no_snapshots, NULL)
{
    struct list__fixture *f = data;

    (void)params;

    list__invoke(0);

    munit_assert_ptr_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 0);

    return MUNIT_OK;
}

/* There is a single snapshot metadata file */
TEST_CASE(list, success, one_snapshot, NULL)
{
    struct list__fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 123, buf, sizeof buf);

    list__invoke(0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 1);

    list__assert_snapshot(0, 1, 8, 123);

    return MUNIT_OK;
    return 0;
}

TEST_CASE(list, success, many_snapshots, NULL)
{
    struct list__fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 456, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 456, buf, sizeof buf);

    test_io_uv_write_snapshot_meta_file(f->dir, 2, 6, 789, 2, 3);
    test_io_uv_write_snapshot_data_file(f->dir, 2, 6, 789, buf, sizeof buf);

    test_io_uv_write_snapshot_meta_file(f->dir, 2, 9, 999, 2, 3);
    test_io_uv_write_snapshot_data_file(f->dir, 2, 9, 999, buf, sizeof buf);

    list__invoke(0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 3);

    list__assert_snapshot(0, 1, 8, 456);
    list__assert_snapshot(1, 2, 6, 789);
    list__assert_snapshot(2, 2, 9, 999);

    return MUNIT_OK;
}

/**
 * io_uv__load_snapshot
 */

TEST_SUITE(load_snapshot);
TEST_GROUP(load_snapshot, success);
TEST_GROUP(load_snapshot, error);

struct load_snapshot__fixture
{
    IO_UV_FIXTURE;
    struct io_uv__snapshot_meta meta;
    uint8_t data[8];
    struct raft_snapshot snapshot;
};

TEST_SETUP(load_snapshot)
{
    struct load_snapshot__fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->meta.term = 1;
    f->meta.index = 5;
    f->meta.timestamp = 123;
    sprintf(f->meta.filename, "snapshot-%llu-%llu-%llu.meta", f->meta.term,
            f->meta.index, f->meta.timestamp);
    raft_configuration_init(&f->snapshot.configuration);
    return f;
}

TEST_TEAR_DOWN(load_snapshot)
{
    struct load_snapshot__fixture *f = data;
    raft_configuration_close(&f->snapshot.configuration);
    if (f->snapshot.bufs != NULL) {
        raft_free(f->snapshot.bufs[0].base);
        raft_free(f->snapshot.bufs);
    }
    IO_UV_TEAR_DOWN;
}

#define load_snapshot__invoke(RV)                                 \
    {                                                             \
        int rv;                                                   \
        rv = io_uv__load_snapshot(f->uv, &f->meta, &f->snapshot); \
        munit_assert_int(rv, ==, RV);                             \
    }

#define load_snapshot_write_meta                                             \
    test_io_uv_write_snapshot_meta_file(f->dir, f->meta.term, f->meta.index, \
                                        f->meta.timestamp, 1, 1)

#define load_snapshot__write_data                                            \
    test_io_uv_write_snapshot_data_file(f->dir, f->meta.term, f->meta.index, \
                                        f->meta.timestamp, f->data,          \
                                        sizeof f->data)

/* There are no snapshot metadata files */
TEST_CASE(load_snapshot, success, first, NULL)
{
    struct load_snapshot__fixture *f = data;
    (void)params;

    *(uint64_t *)f->data = 123;

    load_snapshot_write_meta;
    load_snapshot__write_data;
    load_snapshot__invoke(0);

    munit_assert_int(f->snapshot.term, ==, 1);
    munit_assert_int(f->snapshot.index, ==, 5);
    munit_assert_int(f->snapshot.configuration_index, ==, 1);
    munit_assert_int(f->snapshot.configuration.n, ==, 1);
    munit_assert_int(f->snapshot.configuration.servers[0].id, ==, 1);

    munit_assert_string_equal(f->snapshot.configuration.servers[0].address,
                              "1");

    munit_assert_int(f->snapshot.n_bufs, ==, 1);
    munit_assert_int(*(uint64_t *)f->snapshot.bufs[0].base, ==, 123);

    return MUNIT_OK;
}

/* The snapshot metadata file is missing */
TEST_CASE(load_snapshot, error, no_metadata, NULL)
{
    struct load_snapshot__fixture *f = data;

    (void)params;

    load_snapshot__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The snapshot metadata file is shorter than the mandatory header size. */
TEST_CASE(load_snapshot, error, no_header, NULL)
{
    struct load_snapshot__fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, f->meta.filename, buf, sizeof buf);
    load_snapshot__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The snapshot metadata file has an unexpected format. */
TEST_CASE(load_snapshot, error, format, NULL)
{
    struct load_snapshot__fixture *f = data;
    uint64_t format = 666;

    (void)params;

    load_snapshot_write_meta;

    test_dir_overwrite_file(f->dir, f->meta.filename, &format, sizeof format,
                            0);

    load_snapshot__invoke(RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is too big. */
TEST_CASE(load_snapshot, error, configuration_too_big, NULL)
{
    struct load_snapshot__fixture *f = data;
    uint64_t size = byte__flip64(2 * 1024 * 1024);

    (void)params;

    load_snapshot_write_meta;

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    load_snapshot__invoke(RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is zero. */
TEST_CASE(load_snapshot, error, no_configuration, NULL)
{
    struct load_snapshot__fixture *f = data;
    uint64_t size = 0;

    (void)params;

    load_snapshot_write_meta;

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    load_snapshot__invoke(RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot data file is missing */
TEST_CASE(load_snapshot, error, no_data, NULL)
{
    struct load_snapshot__fixture *f = data;

    (void)params;

    load_snapshot_write_meta;
    load_snapshot__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

static char *load_snapshot_error_oom_heap_fault_delay[] = {"0", "1", "2",
                                                           "3", "4", NULL};
static char *load_snapshot_error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum load_snapshot_error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, load_snapshot_error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, load_snapshot_error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST_CASE(load_snapshot, error, oom, load_snapshot_error_oom_params)
{
    struct load_snapshot__fixture *f = data;

    (void)params;

    load_snapshot_write_meta;
    load_snapshot__write_data;

    test_heap_fault_enable(&f->heap);

    load_snapshot__invoke(RAFT_ENOMEM);

    return MUNIT_OK;
}

/**
 * raft__io_uv_loader_load_all
 */

TEST_SUITE(load_all);
TEST_GROUP(load_all, success);
TEST_GROUP(load_all, error);

struct load_all__fixture
{
    IO_UV_FIXTURE;
    struct raft_snapshot *snapshot;
    struct raft_entry *entries;
    size_t n;
    int count;
};

TEST_SETUP(load_all)
{
    struct load_all__fixture *f = munit_malloc(sizeof *f);
    IO_UV_SETUP;
    f->snapshot = NULL;
    f->entries = NULL;
    f->n = 0;
    f->count = 0;
    return f;
}

TEST_TEAR_DOWN(load_all)
{
    struct load_all__fixture *f = data;
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
    IO_UV_TEAR_DOWN;
}

#define __load_all_trigger(F, RV)                                      \
    {                                                                  \
        int rv;                                                        \
        rv = io_uv__load_all(F->uv, &F->snapshot, &F->entries, &F->n); \
        munit_assert_int(rv, ==, RV);                                  \
    }

TEST_CASE(load_all, success, ignore_unknown, NULL)
{
    struct load_all__fixture *f = data;
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

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. */
TEST_CASE(load_all, success, closed_not_needed, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 2, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 2, 123, buf, sizeof buf);
    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    /* The segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "1-1"));

    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
TEST_CASE(load_all, success, closed, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_io_uv_write_closed_segment_file(f->dir, 1, 2, 1);
    test_io_uv_write_closed_segment_file(f->dir, 3, 1, 1);
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    munit_assert_int(f->n, ==, 4);

    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
TEST_CASE(load_all, success, open_empty, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, "open-1", NULL, 0);

    __load_all_trigger(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
TEST_CASE(load_all, success, open_all_zeros, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, "open-1", 256);

    __load_all_trigger(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
TEST_CASE(load_all, success, open_not_all_zeros, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[WORD_SIZE + /* CRC32 checksum */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE + /* Entry term */
                WORD_SIZE + /* Entry type and data size */
                WORD_SIZE /* Entry data */];
    void *cursor = buf;

    (void)params;

    byte__put64(&cursor, 123);             /* Invalid checksums */
    byte__put64(&cursor, 1);               /* Number of entries */
    byte__put64(&cursor, 1);               /* Entry term */
    byte__put8(&cursor, RAFT_COMMAND); /* Entry type */
    byte__put8(&cursor, 0);                /* Unused */
    byte__put8(&cursor, 0);                /* Unused */
    byte__put8(&cursor, 0);                /* Unused */
    byte__put32(&cursor, 8);               /* Size of entry data */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_append_file(f->dir, "open-1", buf, sizeof buf);

    __load_all_trigger(f, 0);

    /* The segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    /* The first batch has been loaded */
    // munit_assert_int(f->loaded.n, ==, 1);

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
TEST_CASE(load_all, success, open_truncate, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[256];

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, "open-1", buf, sizeof buf);

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
TEST_CASE(load_all, success, open_partial_bach, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[WORD_SIZE + /* Format version */
                WORD_SIZE + /* CRC32 checksums */
                WORD_SIZE + /* Number of entries */
                WORD_SIZE /* Batch data */];
    void *cursor = buf;

    (void)params;

    byte__put64(&cursor, 1); /* Format version */
    byte__put64(&cursor, 0); /* CRC32 checksum */
    byte__put64(&cursor, 0); /* Number of entries */
    byte__put64(&cursor, 0); /* Batch data */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    __load_all_trigger(f, 0);

    /* The partially written segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
TEST_CASE(load_all, success, open_second, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    /* First segment. */
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    /* Second segment */
    test_io_uv_write_open_segment_file(f->dir, 2, 1, 1);

    __load_all_trigger(f, 0);

    /* The first and second segments have been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));
    munit_assert_true(test_dir_has_file(f->dir, "2-2"));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
TEST_CASE(load_all, success, open_second_all_zeroes, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    /* First segment. */
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, "open-2", 256);

    __load_all_trigger(f, 0);

    /* The first segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-1"));
    munit_assert_true(test_dir_has_file(f->dir, "1-1"));

    /* The second segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, "open-2"));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
TEST_CASE(load_all, success, open, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete format data. */
TEST_CASE(load_all, error, short_format, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, "open-1", WORD_SIZE / 2);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
TEST_CASE(load_all, error, short_preamble, NULL)
{
    struct load_all__fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */ + WORD_SIZE /* Checksums */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
TEST_CASE(load_all, error, short_header, NULL)
{
    struct load_all__fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE /* Partial batch header */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
TEST_CASE(load_all, error, short_data, NULL)
{
    struct load_all__fixture *f = data;
    size_t offset = WORD_SIZE + /* Format version */
                    WORD_SIZE + /* Checksums */
                    WORD_SIZE + /* Number of entries */
                    WORD_SIZE + /* Entry term */
                    WORD_SIZE + /* Entry type and data size */
                    WORD_SIZE / 2 /* Partial entry data */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, "open-1", offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
TEST_CASE(load_all, error, corrupt_header, NULL)
{
    struct load_all__fixture *f = data;
    size_t offset = WORD_SIZE /* Format version */;
    uint8_t buf[WORD_SIZE];
    void *cursor = &buf;

    (void)params;

    /* Render invalid checksums */
    byte__put64(&cursor, 123);

    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, "1-1", buf, sizeof buf, offset);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
TEST_CASE(load_all, error, corrupt_data, NULL)
{
    struct load_all__fixture *f = data;
    size_t offset =
        WORD_SIZE /* Format version */ + WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[WORD_SIZE / 2];
    void *cursor = buf;

    (void)params;

    /* Render an invalid data checksum. */
    byte__put32(&cursor, 123456789);

    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, "1-1", buf, sizeof buf, offset);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
TEST_CASE(load_all, error, closed_bad_index, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_io_uv_write_closed_segment_file(f->dir, 2, 1, 1);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
TEST_CASE(load_all, error, closed_empty, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, "1-1", NULL, 0);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
TEST_CASE(load_all, error, closed_bad_format, NULL)
{
    struct load_all__fixture *f = data;

    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};

    (void)params;

    test_dir_write_file(f->dir, "1-1", buf, sizeof buf);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
TEST_CASE(load_all, error, open_no_access, NULL)
{
    struct load_all__fixture *f = data;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_unreadable_file(f->dir, "open-1");

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
TEST_CASE(load_all, error, open_zero_format, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    byte__put64(&cursor, 0); /* Format version */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
TEST_CASE(load_all, error, open_bad_format, NULL)
{
    struct load_all__fixture *f = data;
    uint8_t buf[WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    byte__put64(&cursor, 2); /* Format version */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, "open-1", buf, sizeof buf, 0);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}
