#include "../../include/raft/uv.h"
#include "../../src/byte.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

TEST_MODULE(uv_init)

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    (void)user_data;
    SETUP_UV_NO_INIT;
    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
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
        bytePut64(&cursor, FORMAT);                             \
        bytePut64(&cursor, VERSION);                            \
        bytePut64(&cursor, TERM);                               \
        bytePut64(&cursor, VOTED_FOR);                          \
        test_dir_write_file(f->dir, filename, buf, sizeof buf); \
    }

/* Invoke io->init() */
#define INIT UV_INIT
#define INIT_ERROR(RV) UV_INIT_ERROR(RV)

/* Invoke io->close() */
#define CLOSE UV_CLOSE

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
        munit_assert_int(byteGet64(&cursor), ==, 1);             \
        munit_assert_int(byteGet64(&cursor), ==, VERSION);       \
        munit_assert_int(byteGet64(&cursor), ==, TERM);          \
        munit_assert_int(byteGet64(&cursor), ==, VOTED_FOR);     \
    }

/******************************************************************************
 *
 * Ensure data directory.
 *
 *****************************************************************************/

TEST_SUITE(ensure_dir)

TEST_SETUP(ensure_dir, setup)
TEST_TEAR_DOWN(ensure_dir, tear_down)

TEST_GROUP(ensure_dir, error)

/* The data directory can't be created. */
TEST_CASE(ensure_dir, error, cant_create, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->uv->dir, "/foo/bar");
    INIT_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The given path is not a directory. */
TEST_CASE(ensure_dir, error, not_a_dir, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->uv->dir, "/dev/null");
    INIT_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* Data directory not accessible */
TEST_CASE(ensure_dir, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    strcpy(f->uv->dir, "/root/foo");
    INIT_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Loading metadata
 *
 *****************************************************************************/

TEST_SUITE(metadata)

TEST_SETUP(metadata, setup)
TEST_TEAR_DOWN(metadata, tear_down)

/* If the data directory is empty, the metadata files get initialized. */
TEST_CASE(metadata, empty_dir, NULL)
{
    struct fixture *f = data;
    (void)params;
    INIT;
    ASSERT_CONTENT(1 /* n */, 1 /* version */, 0 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 0 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* If the data directory has a single metadata1 file, its version gets updated
 * and the second metadata file gets created. */
TEST_CASE(metadata, only_1, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          1, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT;
    ASSERT_CONTENT(1 /* n */, 3 /* version */, 1 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 1 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
TEST_CASE(metadata, 1, NULL)
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
    INIT;
    ASSERT_CONTENT(1 /* n */, 5 /* version */, 3 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 4 /* version */, 3 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
TEST_CASE(metadata, 2, NULL)
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
    INIT;
    ASSERT_CONTENT(1 /* n */, 3 /* version */, 2 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 4 /* version */, 2 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
TEST_CASE(metadata, short_file, NULL)
{
    struct fixture *f = data;
    uint8_t buf[16];
    (void)params;
    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);
    INIT;
    ASSERT_CONTENT(1 /* n */, 1 /* version */, 0 /* term */, 0 /* voted for */);
    ASSERT_CONTENT(2 /* n */, 2 /* version */, 0 /* term */, 0 /* voted for */);
    CLOSE;
    return MUNIT_OK;
}

TEST_GROUP(metadata, error)

/* The data directory is not executable. */
TEST_CASE(metadata, error, no_access, NULL)
{
    struct fixture *f = data;
    (void)params;
    test_dir_unexecutable(f->dir);
    INIT_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST_CASE(metadata, error, bad_format, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          2, /* Format                               */
          1, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT_ERROR(RAFT_MALFORMED);
    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST_CASE(metadata, error, bad_version, NULL)
{
    struct fixture *f = data;
    (void)params;
    WRITE(1, /* Metadata file index                  */
          1, /* Format                               */
          0, /* Version                              */
          1, /* Term                                 */
          0 /* Voted for                            */);
    INIT_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}

/* No space is left for writing the initial metadata file. */
TEST(metadata, noSpace, setup, tear_down, 0, dir_tmpfs_params)
{
    struct fixture *f = data;
    SKIP_IF_NO_FIXTURE;
    test_dir_fill(f->dir, 4);
    INIT_ERROR(RAFT_IOERR);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST_CASE(metadata, same_version, NULL)
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
    INIT_ERROR(RAFT_CORRUPT);
    return MUNIT_OK;
}
