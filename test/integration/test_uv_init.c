#include "../../include/raft/uv.h"
#include "../../src/byte.h"
#include "../lib/dir.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a non-initialized raft_io instance and uv dependencies.
 *
 *****************************************************************************/

struct uv
{
    FIXTURE_DIR;
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    struct raft_uv_transport transport;
    struct raft_io io;
};

static void *setupUv(const MunitParameter params[], void *user_data)
{
    struct uv *f = munit_malloc(sizeof *f);
    int rv;
    SETUP_DIR;
    if (f->dir == NULL) { /* Desired fs not available, skip test. */
        free(f);
        return NULL;
    }
    SETUP_HEAP;
    SETUP_LOOP;
    rv = raft_uv_tcp_init(&f->transport, &f->loop);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tearDownUv(void *data)
{
    struct uv *f = data;
    if (f == NULL) {
        return;
    }
    raft_uv_tcp_close(&f->transport, NULL);
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    TEAR_DOWN_DIR;
    free(f);
}

/******************************************************************************
 *
 * raft_uv_init
 *
 *****************************************************************************/

/* Invoke raft_uv_init() and assert that no error occurs. */
#define INIT(DIR)                                                 \
    do {                                                          \
        int _rv;                                                  \
        _rv = raft_uv_init(&f->io, &f->loop, DIR, &f->transport); \
        munit_assert_int(_rv, ==, 0);                             \
    } while (0)

/* Invoke raft_uv_init() and assert that the given error code is returned and
 * the given error message set. */
#define INIT_ERROR(DIR, RV, ERRMSG)                               \
    do {                                                          \
        int _rv;                                                  \
        _rv = raft_uv_init(&f->io, &f->loop, DIR, &f->transport); \
        munit_assert_int(_rv, ==, RV);                            \
        munit_assert_string_equal(f->io.errmsg, ERRMSG);          \
    } while (0)

/* Invoke raft_uv_close(). */
#define CLOSE raft_uv_close(&f->io)

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

SUITE(raft_uv_init)

#define LONG_DIR                                                               \
    "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
    "/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
    "/ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" \
    "/ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" \
    "/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" \
    "/fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" \
    "/ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg" \
    "/hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh" \
    "/iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii" \
    "/jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj" \
    "/kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk" \
    "/lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll" \
    "/mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"

TEST(raft_uv_init, dirTooLong, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    INIT_ERROR(LONG_DIR, RAFT_NAMETOOLONG, "directory path too long");
    return 0;
}

#if defined(RWF_NOWAIT)
static char *oom_heap_fault_delay[] = {"2", NULL};
#else
static char *oom_heap_fault_delay[] = {"1", NULL};
#endif
static char *oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(raft_uv_init, oom, setupUv, tearDownUv, 0, oom_params)
{
    struct uv *f = data;
    HEAP_FAULT_ENABLE;
    INIT_ERROR(f->dir, RAFT_NOMEM, "out of memory");
    return 0;
}

/* The given directory does not exist. */
TEST(raft_uv_init, dirDoesNotExist, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    INIT_ERROR("/foo/bar/egg/baz", RAFT_NOTFOUND,
               "directory '/foo/bar/egg/baz' does not exist");
    return MUNIT_OK;
}

/* The given directory not accessible */
TEST(raft_uv_init, dirNotAccessible, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    sprintf(errmsg, "directory '%s' is not writable", f->dir);
    test_dir_unexecutable(f->dir);
    INIT_ERROR(f->dir, RAFT_INVALID, errmsg);
    return MUNIT_OK;
}

/* No space is left for probing I/O capabilities. */
TEST(raft_uv_init, noSpace, setupUv, tearDownUv, 0, dir_tmpfs_params)
{
    struct uv *f = data;
    SKIP_IF_NO_FIXTURE;
    test_dir_fill(f->dir, 4);
    INIT_ERROR(f->dir, RAFT_NOSPACE,
               "not enough space to create I/O capabilities probe file");
    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
TEST(raft_uv_init, metadataOneTooShort, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    uint8_t buf[16];
    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);
    INIT(f->dir);
    CLOSE;
    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
TEST(raft_uv_init, metadataOneBadFormat, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        2, /* Format                               */
                        1, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    INIT_ERROR(f->dir, RAFT_MALFORMED,
               "decode content of metadata1: bad format version 2");
    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
TEST(raft_uv_init, metadataOneBadVersion, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
    WRITE_METADATA_FILE(1, /* Metadata file index                  */
                        1, /* Format                               */
                        0, /* Version                              */
                        1, /* Term                                 */
                        0 /* Voted for                            */);
    INIT_ERROR(f->dir, RAFT_CORRUPT,
               "decode content of metadata1: version is set to zero");
    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
TEST(raft_uv_init, metadataOneAndTwoSameVersion, setupUv, tearDownUv, 0, NULL)
{
    struct uv *f = data;
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
    INIT_ERROR(f->dir, RAFT_CORRUPT,
               "metadata1 and metadata2 are both at version 2");
    return MUNIT_OK;
}
