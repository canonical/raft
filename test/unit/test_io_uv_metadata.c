#include <stdio.h>

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/logger.h"
#include "../lib/runner.h"
#include "../lib/io.h"

#include "../../include/raft/io_uv.h"

#include "../../src/byte.h"
#include "../../src/io_uv_metadata.h"

TEST_MODULE(io_uv__metadata);

/**
 * Helpers.
 */

struct fixture
{
    struct raft_heap heap;           /* Testable allocator */
    struct raft_logger logger;       /* Test logger */
    struct raft_io io;               /* Test I/O */
    char *dir;                       /* Data directory */
    struct io_uv__metadata metadata; /* Metadata object */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    test_heap_setup(params, &f->heap);
    test_logger_setup(params, &f->logger, 1);
    test_io_setup(params, &f->io);
    f->dir = test_dir_setup(params);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    test_dir_tear_down(f->dir);
    test_io_tear_down(&f->io);
    test_logger_tear_down(&f->logger);
    test_heap_tear_down(&f->heap);
    free(f);
}

/**
 * io_uv__metadata_load
 */

TEST_SUITE(load);

TEST_SETUP(load, setup);
TEST_TEAR_DOWN(load, tear_down);

/* Write either the metadata1 or metadata2 file, filling it with the given
 * values. */
#define load__write(N, FORMAT, VERSION, TERM, VOTED_FOR)        \
    {                                                           \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                  \
        void *cursor = buf;                                     \
        char filename[strlen("metadataN") + 1];                 \
        sprintf(filename, "metadata%d", N);                     \
        byte__put64(&cursor, FORMAT);                           \
        byte__put64(&cursor, VERSION);                          \
        byte__put64(&cursor, TERM);                             \
        byte__put64(&cursor, VOTED_FOR);                        \
        test_dir_write_file(f->dir, filename, buf, sizeof buf); \
    }

/* Assert that @io_uv__metadata_load returns the given code. */
#define load__invoke(RV)                                         \
    {                                                            \
        int rv;                                                  \
                                                                 \
        rv = io_uv__metadata_load(&f->io, f->dir, &f->metadata); \
        munit_assert_int(rv, ==, RV);                            \
    }

/* Assert that the metadata of the last load request equals the given values. */
#define load__assert_metadata(TERM, VOTED_FOR)                  \
    {                                                           \
        munit_assert_int(f->metadata.term, ==, TERM);           \
        munit_assert_int(f->metadata.voted_for, ==, VOTED_FOR); \
    }

/* Assert that the content of either the metadata1 or metadata2 file match the
 * given values. */
#define load__assert_file(N, VERSION, TERM, VOTED_FOR)         \
    {                                                          \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                 \
        const void *cursor = buf;                              \
        char filename[strlen("metadataN") + 1];                \
        sprintf(filename, "metadata%d", N);                    \
        test_dir_read_file(f->dir, filename, buf, sizeof buf); \
        munit_assert_int(byte__get64(&cursor), ==, 1);         \
        munit_assert_int(byte__get64(&cursor), ==, VERSION);   \
        munit_assert_int(byte__get64(&cursor), ==, TERM);      \
        munit_assert_int(byte__get64(&cursor), ==, VOTED_FOR); \
    }

/* The data directory is empty. */
TEST_CASE(load, empty_dir, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__invoke(0);
    load__assert_metadata(0, 0);

    /* The metadata files were initialized. */
    load__assert_file(1, 1, 0, 0);
    load__assert_file(2, 2, 0, 0);

    return MUNIT_OK;
}

/* The data directory is not executable and the metadata file can't be opened.
 */
TEST_CASE(load, open_error, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_unexecutable(f->dir);

    load__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has a single metadata1 file. */
TEST_CASE(load, only_1, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                1, /* Format                               */
                1, /* Version                              */
                1, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(0);
    load__assert_metadata(1, 0);

    /* The metadata1 file got updated and the second one got created. */
    load__assert_file(1, 3, 1, 0);
    load__assert_file(2, 2, 1, 0);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
TEST_CASE(load, 1, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                1, /* Format                               */
                3, /* Version                              */
                3, /* Term                                 */
                0 /* Voted for                            */);

    load__write(2, /* Metadata file index                  */
                1, /* Format                               */
                2, /* Version                              */
                2, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(0);

    load__assert_metadata(3, 0);

    /* The metadata files got updated. */
    load__assert_file(1, 5, 3, 0);
    load__assert_file(2, 4, 3, 0);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
TEST_CASE(load, 2, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                1, /* Format                               */
                1, /* Version                              */
                1, /* Term                                 */
                0 /* Voted for                            */);

    load__write(2, /* Metadata file index                  */
                1, /* Format                               */
                2, /* Version                              */
                2, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(0);

    load__assert_metadata(2, 0);

    /* The metadata files got updated. */
    load__assert_file(1, 3, 2, 0);
    load__assert_file(2, 4, 2, 0);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
TEST_CASE(load, short_file, NULL)
{
    struct fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);

    load__invoke(0);

    /* Term is 0, voted for is 0 and start index is 1. */
    load__assert_metadata(0, 0);

    /* Both metadata files got created. */
    load__assert_file(1, 1, 0, 0);
    load__assert_file(2, 2, 0, 0);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST_CASE(load, bad_format, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                2, /* Format                               */
                1, /* Version                              */
                1, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST_CASE(load, bad_version, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                1, /* Format                               */
                0, /* Version                              */
                1, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* No space is left for writing the initial metadata file. */
TEST_CASE(load, no_space, NULL)
{
    struct fixture *f = data;

    (void)params;

    test_dir_fill(f->dir, 4);

    load__invoke(RAFT_ERR_IO);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST_CASE(load, same_version, NULL)
{
    struct fixture *f = data;

    (void)params;

    load__write(1, /* Metadata file index                  */
                1, /* Format                               */
                2, /* Version                              */
                3, /* Term                                 */
                0 /* Voted for                            */);

    load__write(2, /* Metadata file index                  */
                1, /* Format                               */
                2, /* Version                              */
                2, /* Term                                 */
                0 /* Voted for                            */);

    load__invoke(RAFT_ERR_IO_CORRUPT);

    return MUNIT_OK;
}
