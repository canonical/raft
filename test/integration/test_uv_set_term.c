#include "../lib/runner.h"
#include "../lib/uv.h"

/******************************************************************************
 *
 * Fixture with a pristine libuv-based raft_io instance.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
};

static void *setupUv(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    return f;
}

static void tearDownUv(void *data)
{
    struct fixture *f = data;
    TEAR_DOWN_UV;
    free(f);
}

/******************************************************************************
 *
 * raft_io->set_term()
 *
 *****************************************************************************/

/* Invoke f->io->set_term() and assert that no error occurs. */
#define SET_TERM(TERM)                      \
    do {                                    \
        int _rv;                            \
        _rv = f->io.set_term(&f->io, TERM); \
        munit_assert_int(_rv, ==, 0);       \
    } while (0)

/* Invoke f->io->set_term() and assert that the given error code is returned and
 * the given error message set. */
#define SET_TERM_ERROR(TERM, RV, ERRMSG)                          \
    do {                                                          \
        int _rv;                                                  \
        _rv = f->io.set_term(&f->io, TERM);                       \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg_(&f->io), ERRMSG); \
    } while (0)

/* Write either the metadata1 or metadata2 file, filling it with the given
 * values. */
#define WRITE_METADATA_FILE(N, FORMAT, VERSION, TERM, VOTED_FOR) \
    {                                                            \
        uint8_t buf[8 * 4];                                      \
        void *cursor = buf;                                      \
        char filename[strlen("metadataN") + 1];                  \
        sprintf(filename, "metadata%d", N);                      \
        bytePut64(&cursor, FORMAT);                              \
        bytePut64(&cursor, VERSION);                             \
        bytePut64(&cursor, TERM);                                \
        bytePut64(&cursor, VOTED_FOR);                           \
        test_dir_write_file(f->dir, filename, buf, sizeof buf);  \
    }

/* Assert that the content of either the metadata1 or metadata2 file match the
 * given values. */
#define ASSERT_METADATA_FILE(N, VERSION, TERM, VOTED_FOR)        \
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

SUITE(set_term)

/* The very first time set_term() is called, the metadata1 file gets written. */
TEST(set_term, first, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    munit_assert_false(test_dir_has_file(f->dir, "metadata2"));
    return MUNIT_OK;
}

/* The second time set_term() is called, the metadata2 file gets written. */
TEST(set_term, second, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The third time set_term() is called, the metadata1 file gets overwritten. */
TEST(set_term, third, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    SET_TERM(3);
    ASSERT_METADATA_FILE(1, 3, 3, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The fourth time set_term() is called, the metadata2 file gets overwritten. */
TEST(set_term, fourth, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    SET_TERM(1);
    SET_TERM(2);
    SET_TERM(3);
    SET_TERM(4);
    ASSERT_METADATA_FILE(1, 3, 3, 0);
    ASSERT_METADATA_FILE(2, 4, 4, 0);
    return MUNIT_OK;
}

/* If the data directory has a single metadata1 file, the first time set_data()
 * is called, the second metadata file gets created. */
TEST(set_term, metadataOneExists, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM(2);
    ASSERT_METADATA_FILE(1, 1, 1, 0);
    ASSERT_METADATA_FILE(2, 2, 2, 0);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
TEST(set_term, metadataOneIsGreater, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        3, /* Version                              */
                        3, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        1, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM(4);
    ASSERT_METADATA_FILE(1 /* n */, 3 /* version */, 3 /* term */,
                         0 /* voted for */);
    ASSERT_METADATA_FILE(2 /* n */, 4 /* version */, 4 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
TEST(set_term, metadataTwoIsGreater, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        1, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM(2);
    ASSERT_METADATA_FILE(1 /* n */, 3 /* version */, 2 /* term */,
                         0 /* voted for */);
    ASSERT_METADATA_FILE(2 /* n */, 2 /* version */, 2 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
TEST(set_term, metadataOneTooShort, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    uint8_t buf[16];
    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);
    SET_TERM(1);
    ASSERT_METADATA_FILE(1 /* n */, 1 /* version */, 1 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST(set_term, metadataOneBadFormat, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        2, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM_ERROR(1, RAFT_MALFORMED, "load metadata1: bad format version 2");
    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST(set_term, metadataOneBadVersion, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        0, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM_ERROR(1, RAFT_CORRUPT, "load metadata1: version is set to zero");
    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST(set_term, metadataOneAndTwoSameVersion, setupUv, tearDownUv, 0, NULL)
{
    struct fixture *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        2, /* Version                              */
                        3, /* Term                                 */
                        0 /* Voted for                            */);
    WRITE_METADATA_FILE(2, /* Metadata file index                  */
                        1, /* Format                               */
                        2, /* Version                              */
                        2, /* Term                                 */
                        0 /* Voted for                            */);
    SET_TERM_ERROR(4, RAFT_CORRUPT,
                   "metadata1 and metadata2 are both at version 2");
    return MUNIT_OK;
}
