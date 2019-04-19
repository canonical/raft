#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

#include "../../include/raft/uv.h"

#include "../../src/byte.h"

TEST_MODULE(uv_metadata);

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    char *dir;
    struct raft_uv_transport transport;
    struct raft_io io;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;
    (void)user_data;
    SETUP_HEAP;
    SETUP_LOOP;
    f->dir = test_dir_setup(params);
    rv = raft_uv_tcp_init(&f->transport, &f->loop);
    munit_assert_int(rv, ==, 0);
    rv = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    LOOP_STOP;
    raft_uv_close(&f->io);
    raft_uv_tcp_close(&f->transport);
    test_dir_tear_down(f->dir);
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    free(f);
}

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

/* Write either the metadata1 or metadata2 file, filling it with the given
 * values. */
#define WRITE(N, FORMAT, VERSION, TERM, VOTED_FOR)              \
    {                                                           \
        uint8_t buf[8 * 4];                                     \
        void *cursor = buf;                                     \
        char filename[strlen("metadataN") + 1];                 \
        sprintf(filename, "metadata%d", N);                     \
        byte__put64(&cursor, FORMAT);                           \
        byte__put64(&cursor, VERSION);                          \
        byte__put64(&cursor, TERM);                             \
        byte__put64(&cursor, VOTED_FOR);                        \
        test_dir_write_file(f->dir, filename, buf, sizeof buf); \
    }

/* Invoke io->init() and assert that it returns the given value */
#define INIT(RV)                          \
    {                                     \
        int rv2;                          \
        rv2 = f->io.init(&f->io, 1, "1"); \
        munit_assert_int(rv2, ==, RV);    \
    }

/* Invoke io->close() */
#define CLOSE                            \
    {                                    \
        int rv2;                         \
        rv2 = f->io.close(&f->io, NULL); \
        munit_assert_int(rv2, ==, 0);    \
    }

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the content of either the metadata1 or metadata2 file match the
 * given values. */
#define ASSERT_CONTENT(N, VERSION, TERM, VOTED_FOR)              \
    {                                                            \
        uint8_t buf2[8 * 4];                                     \
        const void *cursor = buf2;                               \
        char filename[strlen("metadataN") + 1];                  \
        sprintf(filename, "metadata%d", N);                      \
        test_dir_read_file(f->dir, filename, buf2, sizeof buf2); \
        munit_assert_int(byte__get64(&cursor), ==, 1);           \
        munit_assert_int(byte__get64(&cursor), ==, VERSION);     \
        munit_assert_int(byte__get64(&cursor), ==, TERM);        \
        munit_assert_int(byte__get64(&cursor), ==, VOTED_FOR);   \
    }

/******************************************************************************
 *
 * Load metadata files
 *
 *****************************************************************************/

TEST_SUITE(load);

TEST_SETUP(load, setup);
TEST_TEAR_DOWN(load, tear_down);

/* If the data directory is empty, the metadata files get initialized. */
TEST_CASE(load, empty_dir, NULL)
{
    struct fixture *f = data;
    (void)params;
    INIT(0);
    ASSERT_CONTENT(1 /* n */, 1 /* version */, 0 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 0 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* If the data directory has a single metadata1 file, its version gets updated
 * and the second metadata file gets created. */
TEST_CASE(load, only_1, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          1, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT(0);
    ASSERT_CONTENT(1 /* n */, 3 /* version */, 1 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 1 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
TEST_CASE(load, 1, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          3, /* Version                              */
          3, /* Term                                 */
          0 /* Voted for                            */);
    WRITE(2, /* Metadata file index                  */
          1, /* Format                               */
          2, /* Version                              */
          2, /* Term                                 */
          0 /* Voted for                            */);
    INIT(0);
    ASSERT_CONTENT(1 /* n */, 5 /* version */, 3 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 4 /* version */, 3 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
TEST_CASE(load, 2, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          1, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    WRITE(2, /* Metadata file index                  */
          1, /* Format                               */
          2, /* Version                              */
          2, /* Term                                 */
          0 /* Voted for                            */);
    INIT(0);
    ASSERT_CONTENT(1 /* n */, 3 /* version */, 2 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 4 /* version */, 2 /* term */, 0 /* voted for */);
    CLOSE;
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
    INIT(0);
    ASSERT_CONTENT(1 /* n */, 1 /* version */, 0 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 0 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

TEST_GROUP(load, error);

/* The data directory is not executable. */
TEST_CASE(load, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_unexecutable(f->dir);
    INIT(RAFT_IOERR);
    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST_CASE(load, error, bad_format, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          2, /* Format                               */
          1, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT(RAFT_MALFORMED);
    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST_CASE(load, error, bad_version, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          0, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* No space is left for writing the initial metadata file. */
TEST_CASE(load, error, no_space, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_fill(f->dir, 4);
    INIT(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST_CASE(load, same_version, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          2, /* Version                              */
          3, /* Term                                 */
          0 /* Voted for                            */);
    WRITE(2, /* Metadata file index                  */
          1, /* Format                               */
          2, /* Version                              */
          2, /* Term                                 */
          0 /* Voted for                            */);
    INIT(RAFT_CORRUPT);
    return MUNIT_OK;
}
