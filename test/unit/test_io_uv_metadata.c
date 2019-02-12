#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/munit.h"

#include "../../src/binary.h"
#include "../../src/io_uv_metadata.h"

/**
 * Test suite
 */

struct fixture
{
    struct raft_heap heap;                /* Testable allocator */
    struct raft_logger logger;            /* Test logger */
    char *dir;                            /* Data directory */
    struct raft__io_uv_metadata metadata; /* Metadata object */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);

    (void)user_data;

    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, 1);

    f->dir = test_dir_setup(params);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;

    test_dir_tear_down(f->dir);

    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Write either the metadata1 or metadata2 file, filling it with the given
 * values.
 */
#define __load_prepare(F, N, FORMAT, VERSION, TERM, VOTED_FOR, START_INDEX) \
    {                                                                       \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                              \
        void *cursor = buf;                                                 \
        char filename[strlen("metadataN") + 1];                             \
                                                                            \
        sprintf(filename, "metadata%d", N);                                 \
                                                                            \
        raft__put64(&cursor, FORMAT);                                       \
        raft__put64(&cursor, VERSION);                                      \
        raft__put64(&cursor, TERM);                                         \
        raft__put64(&cursor, VOTED_FOR);                                    \
        raft__put64(&cursor, START_INDEX);                                  \
                                                                            \
        test_dir_write_file(F->dir, filename, buf, sizeof buf);             \
    }

/**
 * Assert that @raft__io_uv_metadata_load returns the given code.
 */
#define __load_assert_result(F, RV)                                       \
    {                                                                     \
        int rv;                                                           \
                                                                          \
        rv = raft__io_uv_metadata_load(&F->logger, F->dir, &F->metadata); \
        munit_assert_int(rv, ==, RV);                                     \
    }

/**
 * Assert that the metadata of the last load request equals the given values.
 */
#define __load_assert_metadata(F, TERM, VOTED_FOR, START_INDEX)     \
    {                                                               \
        munit_assert_int(F->metadata.term, ==, TERM);               \
        munit_assert_int(F->metadata.voted_for, ==, VOTED_FOR);     \
        munit_assert_int(F->metadata.start_index, ==, START_INDEX); \
    }

/**
 * Assert that the content of either the metadata1 or metadata2 file match the
 * given values.
 */
#define __file_assert(F, N, VERSION, TERM, VOTED_FOR, START_INDEX) \
    {                                                              \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                     \
        const void *cursor = buf;                                  \
        char filename[strlen("metadataN") + 1];                    \
                                                                   \
        sprintf(filename, "metadata%d", N);                        \
                                                                   \
        test_dir_read_file(F->dir, filename, buf, sizeof buf);     \
                                                                   \
        munit_assert_int(raft__get64(&cursor), ==, 1);             \
        munit_assert_int(raft__get64(&cursor), ==, VERSION);       \
        munit_assert_int(raft__get64(&cursor), ==, TERM);          \
        munit_assert_int(raft__get64(&cursor), ==, VOTED_FOR);     \
        munit_assert_int(raft__get64(&cursor), ==, START_INDEX);   \
    }

/**
 * raft__io_uv_metadata_load
 */

/* The data directory is empty. */
static MunitResult test_load_empty_dir(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_assert_result(f, 0);
    __load_assert_metadata(f, 0, 0, 1);

    /* The metadata files were initialized. */
    __file_assert(f, 1, 1, 0, 0, 1);
    __file_assert(f, 2, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* The data directory is not executable and the metadata file can't be opened.
 */
static MunitResult test_load_open_error(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_unexecutable(f->dir);

    __load_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has a single metadata1 file. */
static MunitResult test_load_only_1(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   1, /* Format                               */
                   1, /* Version                              */
                   1, /* Term                                 */
                   0, /* Voted for                            */
                   1 /* Start index                          */);

    __load_assert_result(f, 0);
    __load_assert_metadata(f, 1, 0, 1);

    /* The metadata1 file got updated and the second one got created. */
    __file_assert(f, 1, 3, 1, 0, 1);
    __file_assert(f, 2, 2, 1, 0, 1);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
static MunitResult test_load_1(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   1, /* Format                               */
                   3, /* Version                              */
                   3, /* Term                                 */
                   0, /* Voted for                            */
                   1 /* Start index                          */);

    __load_prepare(f, /*                                      */
                   2, /* Metadata file index                  */
                   1, /* Format                               */
                   2, /* Version                              */
                   2, /* Term                                 */
                   0, /* Voted for                            */
                   1 /* Start index                          */);

    __load_assert_result(f, 0);

    __load_assert_metadata(f, 3, 0, 1);

    /* The metadata files got updated. */
    __file_assert(f, 1, 5, 3, 0, 1);
    __file_assert(f, 2, 4, 3, 0, 1);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
static MunitResult test_load_2(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   1, /* Format                               */
                   1, /* Version                              */
                   1, /* Term                                 */
                   0, /* Voted for                            */
                   1 /* Start index                          */);

    __load_prepare(f, /*                                      */
                   2, /* Metadata file index                  */
                   1, /* Format                               */
                   2, /* Version                              */
                   2, /* Term                                 */
                   0, /* Voted for                            */
                   1 /* Start index                          */);

    __load_assert_result(f, 0);

    __load_assert_metadata(f, 2, 0, 1);

    /* The metadata files got updated. */
    __file_assert(f, 1, 3, 2, 0, 1);
    __file_assert(f, 2, 4, 2, 0, 1);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
static MunitResult test_load_short(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);

    __load_assert_result(f, 0);

    /* Term is 0, voted for is 0 and start index is 1. */
    __load_assert_metadata(f, 0, 0, 1);

    /* Both metadata files got created. */
    __file_assert(f, 1, 1, 0, 0, 1);
    __file_assert(f, 2, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
static MunitResult test_load_bad_format(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   2, /* Format                               */
                   1, /* Version                              */
                   1, /* Term                                 */
                   0, /* Voted for                            */
                   0 /* Start index                          */);

    __load_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
static MunitResult test_load_bad_version(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   1, /* Format                               */
                   0, /* Version                              */
                   1, /* Term                                 */
                   0, /* Voted for                            */
                   0 /* Start index                          */);

    __load_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* No space is left for writing the initial metadata file. */
static MunitResult test_load_no_space(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_fill(f->dir, 4);

    __load_assert_result(f, RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
static MunitResult test_load_same_version(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    __load_prepare(f, /*                                      */
                   1, /* Metadata file index                  */
                   1, /* Format                               */
                   2, /* Version                              */
                   3, /* Term                                 */
                   0, /* Voted for                            */
                   0 /* Start index                          */);

    __load_prepare(f, /*                                      */
                   2, /* Metadata file index                  */
                   1, /* Format                               */
                   2, /* Version                              */
                   2, /* Term                                 */
                   0, /* Voted for                            */
                   0 /* Start index                          */);

    __load_assert_result(f, RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}

#define __test_load(NAME, FUNC, PARAMS)                         \
    {                                                           \
        "/" NAME, test_load_##FUNC, setup, tear_down, 0, PARAMS \
    }

static MunitTest load_tests[] = {
    __test_load("empty-dir", empty_dir, NULL),
    __test_load("open-error", open_error, NULL),
    __test_load("only-1", only_1, NULL),
    __test_load("/1", 1, NULL),
    __test_load("/2", 2, NULL),
    __test_load("/short", short, NULL),
    __test_load("/bad-format", bad_format, NULL),
    __test_load("/bad-version", bad_version, NULL),
    __test_load("/no-space", no_space, NULL),
    __test_load("/same-version", same_version, NULL),
    {NULL, NULL, NULL, NULL, 0, NULL},
};

MunitSuite raft_io_uv_metadata_suites[] = {
    {"/load", load_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
