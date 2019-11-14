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

/* Re-initialize the raft_io instance, to refresh the metadata cache. */
#define REINIT                                                       \
    do {                                                             \
        int _rv;                                                     \
        f->io.stop(&f->io);                                          \
        f->io.close(&f->io, NULL);                                   \
        LOOP_RUN(2);                                                 \
        raft_uv_close(&f->io);                                       \
        _rv = raft_uv_init(&f->io, &f->loop, f->dir, &f->transport); \
        munit_assert_int(_rv, ==, 0);                                \
        f->io.config(&f->io, 1, "127.0.0.1:9000");                   \
    } while (0)

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
    REINIT;
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
    REINIT;
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
    REINIT;
    SET_TERM(2);
    ASSERT_METADATA_FILE(1 /* n */, 3 /* version */, 2 /* term */,
                         0 /* voted for */);
    ASSERT_METADATA_FILE(2 /* n */, 2 /* version */, 2 /* term */,
                         0 /* voted for */);
    return MUNIT_OK;
}
