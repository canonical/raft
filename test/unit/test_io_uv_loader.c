#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/io_uv.h"
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
 * Helpers
 */

#define __FIXTURE              \
    struct raft_heap heap;     \
    struct raft_logger logger; \
    char *dir;                 \
    struct raft__io_uv_loader loader;

#define __SETUP                               \
    (void)user_data;                          \
    test_heap_setup(params, &f->heap);        \
    test_logger_setup(params, &f->logger, 1); \
    f->dir = test_dir_setup(params);          \
    raft__io_uv_loader_init(&f->loader, &f->logger, f->dir);

#define __FIXTURE_TEAR_DOWN            \
    test_dir_tear_down(f->dir);        \
    test_logger_tear_down(&f->logger); \
    test_heap_tear_down(&f->heap);

#define __test(NAME, FUNC, SETUP, TEAR_DOWN, PARAMS)       \
    {                                                      \
        "/" NAME, test_##FUNC, SETUP, TEAR_DOWN, 0, PARAMS \
    }

/**
 * raft__io_uv_loader_list
 */

struct list_fixture
{
    __FIXTURE;
    struct raft__io_uv_loader_snapshot *snapshots;
    size_t n_snapshots;
    struct raft__io_uv_loader_segment *segments;
    size_t n_segments;
};

static void *list_setup(const MunitParameter params[], void *user_data)
{
    struct list_fixture *f = munit_malloc(sizeof *f);
    __SETUP;
    return f;
}

static void list_tear_down(void *data)
{
    struct list_fixture *f = data;
    if (f->snapshots != NULL) {
        raft_free(f->snapshots);
    }
    if (f->segments != NULL) {
        raft_free(f->segments);
    }
    __FIXTURE_TEAR_DOWN;
}

#define __list_trigger(F, RV)                                       \
    {                                                               \
        int rv;                                                     \
        rv = raft__io_uv_loader_list(&F->loader, &F->snapshots,     \
                                     &F->n_snapshots, &F->segments, \
                                     &F->n_segments);               \
        munit_assert_int(rv, ==, RV);                               \
    }

#define __list_assert_snapshot(F, I, TERM, INDEX, TIMESTAMP) \
    munit_assert_int(F->snapshots[I].term, ==, TERM);        \
    munit_assert_int(F->snapshots[I].index, ==, INDEX);      \
    munit_assert_int(F->snapshots[I].timestamp, ==, TIMESTAMP);

#define __test_list(NAME, FUNC, PARAMS) \
    __test(NAME, list_##FUNC, list_setup, list_tear_down, PARAMS)

/* There are no snapshot metadata files */
static MunitResult test_list_success_no_snapshots(const MunitParameter params[],
                                                  void *data)
{
    struct list_fixture *f = data;

    (void)params;

    __list_trigger(f, 0);

    munit_assert_ptr_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 0);

    return MUNIT_OK;
}

/* There is a single snapshot metadata file */
static MunitResult test_list_success_one_snapshot(const MunitParameter params[],
                                                  void *data)
{
    struct list_fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 123, buf, sizeof buf);

    __list_trigger(f, 0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 1);

    __list_assert_snapshot(f, 0, 1, 8, 123);

    return MUNIT_OK;
}

/* There are many snapshot metadata files */
static MunitResult test_list_success_many_snapshots(
    const MunitParameter params[],
    void *data)
{
    struct list_fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 123, 1, 1);

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 8, 456, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 8, 456, buf, sizeof buf);

    test_io_uv_write_snapshot_meta_file(f->dir, 2, 6, 789, 2, 3);
    test_io_uv_write_snapshot_data_file(f->dir, 2, 6, 789, buf, sizeof buf);

    __list_trigger(f, 0);

    munit_assert_ptr_not_null(f->snapshots);
    munit_assert_int(f->n_snapshots, ==, 2);

    __list_assert_snapshot(f, 0, 1, 8, 456);
    __list_assert_snapshot(f, 1, 2, 6, 789);

    return MUNIT_OK;
}

#define __test_list_success(NAME, FUNC, PARAMS) \
    __test_list(NAME, success_##FUNC, PARAMS)

static MunitTest list_success_tests[] = {
    __test_list_success("no-snapshots", no_snapshots, NULL),
    __test_list_success("one-snapshot", one_snapshot, NULL),
    __test_list_success("many-snapshots", many_snapshots, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite list_suites[] = {
    {"/success", list_success_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * raft__io_uv_loader_load_snapshot
 */

struct load_snapshot_fixture
{
    __FIXTURE;
    struct raft__io_uv_loader_snapshot meta;
    uint8_t data[8];
    struct raft_snapshot snapshot;
};

static void *load_snapshot_setup(const MunitParameter params[], void *user_data)
{
    struct load_snapshot_fixture *f = munit_malloc(sizeof *f);
    __SETUP;
    f->meta.term = 1;
    f->meta.index = 5;
    f->meta.timestamp = 123;
    sprintf(f->meta.filename, "snapshot-%020llu-%020llu-%020llu.meta",
            f->meta.term, f->meta.index, f->meta.timestamp);
    raft_configuration_init(&f->snapshot.configuration);
    return f;
}

static void load_snapshot_tear_down(void *data)
{
    struct load_snapshot_fixture *f = data;
    raft_configuration_close(&f->snapshot.configuration);
    if (f->snapshot.bufs != NULL) {
        raft_free(f->snapshot.bufs[0].base);
        raft_free(f->snapshot.bufs);
    }
    __FIXTURE_TEAR_DOWN;
}

#define __load_snapshot_trigger(F, RV)                              \
    {                                                               \
        int rv;                                                     \
        rv = raft__io_uv_loader_load_snapshot(&F->loader, &F->meta, \
                                              &F->snapshot);        \
        munit_assert_int(rv, ==, RV);                               \
    }

#define __load_snapshot_write_meta(F)                                        \
    test_io_uv_write_snapshot_meta_file(F->dir, F->meta.term, F->meta.index, \
                                        F->meta.timestamp, 1, 1);

#define __load_snapshot_write_data(F)                                        \
    test_io_uv_write_snapshot_data_file(F->dir, F->meta.term, F->meta.index, \
                                        F->meta.timestamp, F->data,          \
                                        sizeof F->data);

#define __test_load_snapshot(NAME, FUNC, PARAMS)            \
    __test(NAME, load_snapshot_##FUNC, load_snapshot_setup, \
           load_snapshot_tear_down, PARAMS)

/* There are no snapshot metadata files */
static MunitResult test_load_snapshot_success(const MunitParameter params[],
                                              void *data)
{
    struct load_snapshot_fixture *f = data;

    (void)params;

    *(uint64_t *)f->data = 123;

    __load_snapshot_write_meta(f);
    __load_snapshot_write_data(f);
    __load_snapshot_trigger(f, 0);

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

static MunitTest load_snapshot_success_tests[] = {
    __test_load_snapshot("", success, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* The snapshot metadata file is missing */
static MunitResult test_load_snapshot_error_no_metadata(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;

    (void)params;

    __load_snapshot_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The snapshot metadata file is shorter than the mandatory header size. */
static MunitResult test_load_snapshot_error_no_header(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, f->meta.filename, buf, sizeof buf);
    __load_snapshot_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The snapshot metadata file has an unexpected format. */
static MunitResult test_load_snapshot_error_format(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;
    uint64_t format = 666;

    (void)params;

    __load_snapshot_write_meta(f);

    test_dir_overwrite_file(f->dir, f->meta.filename, &format, sizeof format,
                            0);

    __load_snapshot_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is too big. */
static MunitResult test_load_snapshot_error_configuration_too_big(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;
    uint64_t size = raft__flip64(2 * 1024 * 1024);

    (void)params;

    __load_snapshot_write_meta(f);

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    __load_snapshot_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot metadata configuration size is zero. */
static MunitResult test_load_snapshot_error_no_configuration(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;
    uint64_t size = 0;

    (void)params;

    __load_snapshot_write_meta(f);

    test_dir_overwrite_file(f->dir, f->meta.filename, &size, sizeof size,
                            sizeof(uint64_t) * 3);

    __load_snapshot_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The snapshot data file is missing */
static MunitResult test_load_snapshot_error_no_data(
    const MunitParameter params[],
    void *data)
{
    struct load_snapshot_fixture *f = data;

    (void)params;

    __load_snapshot_write_meta(f);
    __load_snapshot_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* Out of memory conditions. */
static MunitResult test_load_snapshot_error_oom(const MunitParameter params[],
                                                void *data)
{
    struct load_snapshot_fixture *f = data;

    (void)params;

    __load_snapshot_write_meta(f);
    __load_snapshot_write_data(f);

    test_heap_fault_enable(&f->heap);

    __load_snapshot_trigger(f, RAFT_ERR_NOMEM);

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

#define __test_load_snapshot_error(NAME, FUNC, PARAMS) \
    __test_load_snapshot(NAME, error_##FUNC, PARAMS)

static MunitTest load_snapshot_error_tests[] = {
    __test_load_snapshot_error("no-metadata", no_metadata, NULL),
    __test_load_snapshot_error("no-header", no_header, NULL),
    __test_load_snapshot_error("format", format, NULL),
    __test_load_snapshot_error("configuration-too-big",
                               configuration_too_big,
                               NULL),
    __test_load_snapshot_error("no-configuration", no_configuration, NULL),
    __test_load_snapshot_error("no-data", no_data, NULL),
    __test_load_snapshot_error("oom", oom, load_snapshot_error_oom_params),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite load_snapshot_suites[] = {
    {"/success", load_snapshot_success_tests, NULL, 1, 0},
    {"/error", load_snapshot_error_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * raft__io_uv_loader_load_all
 */

struct load_all_fixture
{
    __FIXTURE;
    struct raft_snapshot *snapshot;
    struct raft_entry *entries;
    size_t n;
    int count;
};

static void *load_all_setup(const MunitParameter params[], void *user_data)
{
    struct load_all_fixture *f = munit_malloc(sizeof *f);
    __SETUP;
    f->snapshot = NULL;
    f->entries = NULL;
    f->n = 0;
    f->count = 0;
    return f;
}

static void load_all_tear_down(void *data)
{
    struct load_all_fixture *f = data;
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
    __FIXTURE_TEAR_DOWN;
}

#define __load_all_trigger(F, RV)                                  \
    {                                                              \
        int rv;                                                    \
        rv = raft__io_uv_loader_load_all(&F->loader, &F->snapshot, \
                                         &F->entries, &F->n);      \
        munit_assert_int(rv, ==, RV);                              \
    }

#define __test_load_all(NAME, FUNC, PARAMS) \
    __test(NAME, load_all_##FUNC, load_all_setup, load_all_tear_down, PARAMS)

static MunitResult test_load_all_success_ignore_unknown(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
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

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed, since they are included in a snapshot. */
static MunitResult test_load_all_success_closed_not_needed(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    uint8_t buf[8];

    (void)params;

    test_io_uv_write_snapshot_meta_file(f->dir, 1, 2, 123, 1, 1);
    test_io_uv_write_snapshot_data_file(f->dir, 1, 2, 123, buf, sizeof buf);
    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    /* The segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
static MunitResult test_load_all_success_closed(const MunitParameter params[],
                                                void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_io_uv_write_closed_segment_file(f->dir, 1, 2, 1);
    test_io_uv_write_closed_segment_file(f->dir, 3, 1, 1);
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    munit_assert_int(f->n, ==, 4);

    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
static MunitResult test_load_all_success_open_empty(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __OPEN_FILENAME_1, NULL, 0);

    __load_all_trigger(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
static MunitResult test_load_all_success_open_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, 256);

    __load_all_trigger(f, 0);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
static MunitResult test_load_all_success_open_not_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
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

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load_all_trigger(f, 0);

    /* The segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The first batch has been loaded */
    // munit_assert_int(f->loaded.n, ==, 1);

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
static MunitResult test_load_all_success_open_truncate(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    uint8_t buf[256];

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
static MunitResult test_load_all_success_open_partial_batch(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
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

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_trigger(f, 0);

    /* The partially written segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
static MunitResult test_load_all_success_open_second(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    /* First segment. */
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    /* Second segment */
    test_io_uv_write_open_segment_file(f->dir, 2, 1, 1);

    __load_all_trigger(f, 0);

    /* The first and second segments have been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
static MunitResult test_load_all_success_open_second_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    /* First segment. */
    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_2, 256);

    __load_all_trigger(f, 0);

    /* The first segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The second segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
static MunitResult test_load_all_success_open(const MunitParameter params[],
                                              void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    __load_all_trigger(f, 0);

    return MUNIT_OK;
}

#define __test_load_all_success(NAME, FUNC, PARAMS) \
    __test_load_all(NAME, success_##FUNC, PARAMS)

static MunitTest load_all_success_tests[] = {
    __test_load_all_success("ignore-unknown", ignore_unknown, NULL),
    __test_load_all_success("closed-not-needed", closed_not_needed, NULL),
    __test_load_all_success("closed", closed, NULL),
    __test_load_all_success("open-empty", open_empty, NULL),
    __test_load_all_success("open-all-zeros", open_all_zeros, NULL),
    __test_load_all_success("open-not-all-zeros", open_not_all_zeros, NULL),
    __test_load_all_success("open-truncate", open_truncate, NULL),
    __test_load_all_success("open-partial-batch", open_partial_batch, NULL),
    __test_load_all_success("open-second", open_second, NULL),
    __test_load_all_success("open-second-all-zeros",
                            open_second_all_zeros,
                            NULL),
    __test_load_all_success("open", open, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/* The data directory has an open segment which has incomplete format data. */
static MunitResult test_load_all_error_short_format(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, __WORD_SIZE / 2);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
static MunitResult test_load_all_error_short_preamble(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    size_t offset =
        __WORD_SIZE /* Format version */ + __WORD_SIZE /* Checksums */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
static MunitResult test_load_all_error_short_header(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE /* Partial batch header */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
static MunitResult test_load_all_error_short_data(const MunitParameter params[],
                                                  void *data)
{
    struct load_all_fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE + /* Entry term */
                    __WORD_SIZE + /* Entry type and data size */
                    __WORD_SIZE / 2 /* Partial entry data */;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
static MunitResult test_load_all_error_corrupt_header(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */;
    uint8_t buf[__WORD_SIZE];
    void *cursor = &buf;

    (void)params;

    /* Render invalid checksums */
    raft__put64(&cursor, 123);

    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
static MunitResult test_load_all_error_corrupt_data(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */ +
                    __WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[__WORD_SIZE / 2];
    void *cursor = buf;

    (void)params;

    /* Render an invalid data checksum. */
    raft__put32(&cursor, 123456789);

    test_io_uv_write_closed_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
static MunitResult test_load_all_error_closed_bad_index(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_io_uv_write_closed_segment_file(f->dir, 2, 1, 1);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
static MunitResult test_load_all_error_closed_empty(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, NULL, 0);

    __load_all_trigger(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
static MunitResult test_load_all_error_closed_bad_format(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
static MunitResult test_load_all_error_open_no_access(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;

    (void)params;

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_unreadable_file(f->dir, __OPEN_FILENAME_1);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
static MunitResult test_load_all_error_open_zero_format(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 0); /* Format version */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
static MunitResult test_load_all_error_open_bad_format(
    const MunitParameter params[],
    void *data)
{
    struct load_all_fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 2); /* Format version */

    test_io_uv_write_open_segment_file(f->dir, 1, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load_all_trigger(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

#define __test_load_all_error(NAME, FUNC, PARAMS) \
    __test_load_all(NAME, error_##FUNC, PARAMS)

static MunitTest load_all_error_tests[] = {
    __test_load_all_error("short-format", short_format, NULL),
    __test_load_all_error("short-preamble", short_preamble, NULL),
    __test_load_all_error("short-header", short_header, NULL),
    __test_load_all_error("short-data", short_data, NULL),
    __test_load_all_error("corrupt-header", corrupt_header, NULL),
    __test_load_all_error("corrupt-data", corrupt_data, NULL),
    __test_load_all_error("closed-bad-index", closed_bad_index, NULL),
    __test_load_all_error("closed-empty", closed_empty, NULL),
    __test_load_all_error("closed-bad-format", closed_bad_format, NULL),
    __test_load_all_error("open-no-access", open_no_access, NULL),
    __test_load_all_error("open-zero-format", open_zero_format, NULL),
    __test_load_all_error("open-bad-format", open_bad_format, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite load_all_suites[] = {
    {"/success", load_all_success_tests, NULL, 1, 0},
    {"/error", load_all_error_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_loader_suites[] = {
    {"/list", NULL, list_suites, 1, 0},
    {"/load-snapshot", NULL, load_snapshot_suites, 1, 0},
    {"/load-all", NULL, load_all_suites, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
