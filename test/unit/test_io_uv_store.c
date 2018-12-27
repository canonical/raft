/**
 * Helpers
 */

#include "../../src/binary.h"
#include "../../src/checksum.h"
#include "../../src/io_uv_store.h"

#include "../lib/fs.h"
#include "../lib/heap.h"
#include "../lib/munit.h"

/**
 * Helpers
 */

struct fixture
{
    struct raft_heap heap;          /* Testable allocator */
    char *dir;                      /* Data directory */
    char errmsg[RAFT_ERRMSG_SIZE];  /* Error message buffer */
    struct raft_io_request request; /* I/O request buffer */
    int count;                      /* To generate deterministic entry data */
    struct raft_io_uv_store store;  /* Store under test */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;

    (void)user_data;

    test_heap_setup(params, &f->heap);

    f->dir = test_dir_setup(params);

    memset(&f->request, 0, sizeof f->request);
    memset(f->errmsg, 0, sizeof f->errmsg);

    f->count = 0;

    rv = raft_io_uv_store__init(&f->store, f->dir, f->errmsg);
    munit_assert_int(rv, ==, 0);

    return f;
}

static void tear_down(void *data)
{
    struct fixture *f = data;
    struct raft_entry *entries;
    void *batch = NULL;
    unsigned n;
    unsigned i;

    /* Free any loaded entry */
    entries = f->request.result.read_state.entries;
    n = f->request.result.read_state.n;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];

        if (entry->batch != batch) {
            batch = entry->batch;
            raft_free(batch);
        }
    }

    if (entries != NULL) {
        raft_free(entries);
    }

    raft_io_uv_store__close(&f->store);

    test_dir_tear_down(f->dir);

    test_heap_tear_down(&f->heap);

    free(f);
}

/**
 * Write either the metadata1 or metadata2 file, filling it with the given
 * values.
 */
#define __write_metadata(F, N, FORMAT, VERSION, TERM, VOTED_FOR, FIRST_INDEX) \
    {                                                                         \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                                \
        void *cursor = buf;                                                   \
        char filename[strlen("metadataN") + 1];                               \
                                                                              \
        sprintf(filename, "metadata%d", N);                                   \
                                                                              \
        raft__put64(&cursor, FORMAT);                                         \
        raft__put64(&cursor, VERSION);                                        \
        raft__put64(&cursor, TERM);                                           \
        raft__put64(&cursor, VOTED_FOR);                                      \
        raft__put64(&cursor, FIRST_INDEX);                                    \
                                                                              \
        test_dir_write_file(F->dir, filename, buf, sizeof buf);               \
    }

#define __WORD_SIZE sizeof(uint64_t)

/**
 * Write a valid segment with #M batches.
 */
#define __write_segment(F, FILENAME, M)                                       \
    {                                                                         \
        size_t size = __WORD_SIZE /* Format version */;                       \
        int i;                                                                \
        uint8_t *buf;                                                         \
        void *cursor;                                                         \
        unsigned crc1;                                                        \
        unsigned crc2;                                                        \
        uint8_t *batch; /* Start of the batch */                              \
        size_t header_size = raft_batch_header_size(1);                       \
        size_t data_size = __WORD_SIZE;                                       \
                                                                              \
        size += (__WORD_SIZE /* Checksums */ + header_size + data_size) * M;  \
        buf = munit_malloc(size);                                             \
        cursor = buf;                                                         \
        raft__put64(&cursor, 1); /* Format version */                         \
        batch = cursor;                                                       \
                                                                              \
        for (i = 0; i < M; i++) {                                             \
            F->count++;                                                       \
                                                                              \
            raft__put64(&cursor, 0);               /* CRC sums placeholder */ \
            raft__put64(&cursor, 1);               /* Number of entries */    \
            raft__put64(&cursor, 1);               /* Entry term */           \
            raft__put8(&cursor, RAFT_LOG_COMMAND); /* Entry type */           \
            raft__put8(&cursor, 0);                /* Entry type */           \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put8(&cursor, 0);                /* Unused */               \
            raft__put32(&cursor, 8);               /* Size of entry data */   \
            raft__put64(&cursor, F->count);        /* Entry data */           \
                                                                              \
            cursor = batch + __WORD_SIZE;                                     \
            crc1 = raft__crc32(cursor, header_size);                          \
            crc2 = raft__crc32(cursor + header_size, data_size);              \
            cursor = batch;                                                   \
            raft__put32(&cursor, crc1); /* Header checksum */                 \
            raft__put32(&cursor, crc2); /* Data checksum */                   \
            batch += __WORD_SIZE + header_size + data_size;                   \
            cursor = batch;                                                   \
        }                                                                     \
                                                                              \
        test_dir_write_file(F->dir, FILENAME, buf, size);                     \
        free(buf);                                                            \
    }

/**
 * Filename of the first closed segment.
 */
#define __CLOSED_FILENAME_1 "00000000000000000001-00000000000000000001"
#define __CLOSED_FILENAME_2 "00000000000000000002-00000000000000000002"

/**
 * Write a closed segment with first index #N and #M batches.
 */
#define __write_closed_segment(F, N, M)                     \
    {                                                       \
        char filename[strlen(__CLOSED_FILENAME_1) + 1];     \
                                                            \
        sprintf(filename, "%020llu-%020llu", (raft_index)N, \
                (raft_index)(N + M - 1));                   \
                                                            \
        __write_segment(F, filename, M);                    \
    }

/**
 * Filename of the first and second open segments.
 */
#define __OPEN_FILENAME_1 "open-1"
#define __OPEN_FILENAME_2 "open-2"

/**
 * Write a open segment with index #N and #M batches.
 */
#define __write_open_segment(F, N, M)                 \
    {                                                 \
        char filename[strlen(__OPEN_FILENAME_1) + 1]; \
                                                      \
        sprintf(filename, "open-%d", N);              \
                                                      \
        __write_segment(F, filename, M);              \
    }

/**
 * Submit a load I/O request and check that no error occurred.
 */
#define __load(F)                                                       \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft_io_uv_store__load(&F->store, &F->request, F->errmsg); \
        if (rv != 0) {                                                  \
            munit_logf(MUNIT_LOG_ERROR, "load: %s", F->errmsg);         \
        }                                                               \
    }

/**
 * Submit a store term I/O request and check that no error occurred.
 */
#define __term(F)                                                       \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft_io_uv_store__term(&F->store, &F->request, F->errmsg); \
        if (rv != 0) {                                                  \
            munit_logf(MUNIT_LOG_ERROR, "term: %s", F->errmsg);         \
        }                                                               \
    }

/**
 * Submit a store vote I/O request and check that no error occurred.
 */
#define __vote(F)                                                       \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft_io_uv_store__vote(&F->store, &F->request, F->errmsg); \
        if (rv != 0) {                                                  \
            munit_logf(MUNIT_LOG_ERROR, "term: %s", F->errmsg);         \
        }                                                               \
    }

/**
 * Initialize a pristine store and check that the given error occurs.
 */
#define __assert_init_error(F, DIR, RV, ERRMSG)              \
    {                                                        \
        int rv;                                              \
        struct raft_io_uv_store store;                       \
                                                             \
        rv = raft_io_uv_store__init(&store, DIR, f->errmsg); \
        munit_assert_int(rv, ==, RV);                        \
                                                             \
        munit_assert_string_equal(f->errmsg, ERRMSG);        \
    }

/**
 * Submit a load state request and check that the given error occurs.
 */
#define __assert_load_error(F, RV, ERRMSG)                              \
    {                                                                   \
        int rv;                                                         \
                                                                        \
        rv = raft_io_uv_store__load(&f->store, &f->request, f->errmsg); \
        munit_assert_int(rv, ==, RV);                                   \
                                                                        \
        munit_assert_string_equal(f->errmsg, ERRMSG);                   \
    }

/**
 * Assert that the metadata in the given RAFT_IO_READ_STATE result equals the
 * given values.
 */
#define __assert_result_metadata(F, TERM, VOTED_FOR, FIRST_INDEX)      \
    {                                                                  \
        munit_assert_int(F->request.result.read_state.term, ==, TERM); \
        munit_assert_int(F->request.result.read_state.voted_for, ==,   \
                         VOTED_FOR);                                   \
        munit_assert_int(F->request.result.read_state.first_index, ==, \
                         FIRST_INDEX);                                 \
    }

/**
 * Assert that there are N entries in the given RAFT_IO_READ_STATE result, each
 * one with the expected payload.
 */
#define __assert_result_entries(F, N)                                      \
    {                                                                      \
        int i;                                                             \
        unsigned n = F->request.result.read_state.n;                       \
        struct raft_entry *entries = F->request.result.read_state.entries; \
                                                                           \
        munit_assert_int(n, ==, N);                                        \
                                                                           \
        for (i = 0; i < N; i++) {                                          \
            struct raft_entry *entry = &entries[i];                        \
            uint64_t value = *(uint64_t *)entry->buf.base;                 \
                                                                           \
            munit_assert_int(value, ==, i + 1);                            \
        }                                                                  \
    }

/**
 * Assert that the content of either the metadata1 or metadata2 file match the
 * given values.
 */
#define __assert_metadata(F, N, FORMAT, VERSION, TERM, VOTED_FOR, FIRST_INDEX) \
    {                                                                          \
        uint8_t buf[RAFT_IO_UV_METADATA_SIZE];                                 \
        const void *cursor = buf;                                              \
        char filename[strlen("metadataN") + 1];                                \
                                                                               \
        sprintf(filename, "metadata%d", N);                                    \
                                                                               \
        test_dir_read_file(F->dir, filename, buf, sizeof buf);                 \
                                                                               \
        munit_assert_int(raft__get64(&cursor), ==, FORMAT);                    \
        munit_assert_int(raft__get64(&cursor), ==, VERSION);                   \
        munit_assert_int(raft__get64(&cursor), ==, TERM);                      \
        munit_assert_int(raft__get64(&cursor), ==, VOTED_FOR);                 \
        munit_assert_int(raft__get64(&cursor), ==, FIRST_INDEX);               \
    }

/**
 * raft_uv_store__init
 */

/* Data directory path is too long */
static MunitResult test_init_dir_too_long(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    char dir[1024];

    memset(dir, 'a', sizeof dir - 1);
    dir[sizeof dir - 1] = 0;

    __assert_init_error(f, dir, RAFT_ERR_IO,
                        "data directory exceeds 895 characters");

    return MUNIT_OK;
}

/* Can't create data directory */
static MunitResult test_init_cant_create_dir(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    const char *dir = "/non/existing/path";

    __assert_init_error(f, dir, RAFT_ERR_IO,
                        "create data directory '/non/existing/path': "
                        "no such file or directory");

    return MUNIT_OK;
}

/* Data directory not a directory */
static MunitResult test_init_not_a_dir(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    const char *dir = "/dev/null";

    __assert_init_error(f, dir, RAFT_ERR_IO,
                        "path '/dev/null' is not a directory");

    return MUNIT_OK;
}

/* Data directory not accessible */
static MunitResult test_init_access_error(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    const char *dir = "/root/foo";

    __assert_init_error(f, dir, RAFT_ERR_IO,
                        "access data directory '/root/foo': permission denied");

    return MUNIT_OK;
}

static char *init_oom_heap_fault_delay[] = {"0", NULL};
static char *init_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum init_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, init_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, init_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions */
static MunitResult test_init_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    test_heap_fault_enable(&f->heap);

    __assert_init_error(f, f->dir, RAFT_ERR_NOMEM,
                        "can't copy data directory path");

    return MUNIT_OK;
}

/* Create data directory if it does not exist */
static MunitResult test_init_create_dir(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    struct raft_io_uv_store store;
    int rv;
    struct stat sb;

    (void)params;

    char dir[1024];

    sprintf(dir, "%s/sub/", f->dir);

    rv = raft_io_uv_store__init(&store, dir, f->errmsg);
    munit_assert_int(rv, ==, 0);

    rv = stat(dir, &sb);
    munit_assert_int(rv, ==, 0);

    munit_assert_true((sb.st_mode & S_IFMT) == S_IFDIR);

    raft_io_uv_store__close(&store);

    return MUNIT_OK;
}

static MunitTest init_tests[] = {
    {"/dir-too-long", test_init_dir_too_long, setup, tear_down, 0, NULL},
    {"/cant-create-dir", test_init_cant_create_dir, setup, tear_down, 0, NULL},
    {"/not-a-dir", test_init_not_a_dir, setup, tear_down, 0, NULL},
    {"/access-error", test_init_access_error, setup, tear_down, 0, NULL},
    {"/oom", test_init_oom, setup, tear_down, 0, init_oom_params},
    {"/create-dir", test_init_create_dir, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv_store___load
 */

/* The data directory is empty. */
static MunitResult test_load_empty_dir(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    __load(f);

    __assert_result_metadata(f, 0, 0, 1);

    /* The metadata files were initialized. */
    __assert_metadata(f, 1, 1, 1, 0, 0, 1);
    __assert_metadata(f, 2, 1, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* The data directory is not executable and metadata files can't be opened. */
static MunitResult test_load_md_open_err(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_unexecutable(f->dir);

    __assert_load_error(f, RAFT_ERR_IO,
                        "load metadata1: open: permission denied");

    return MUNIT_OK;
}

/* The data directory has a single metadata1 file. */
static MunitResult test_load_md_only_1(const MunitParameter params[],
                                       void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     1 /* First index                          */);

    __load(f);

    __assert_result_metadata(f, 1, 0, 1);

    /* The metadata1 file got updated and the second created. */
    __assert_metadata(f, 1, 1, 3, 1, 0, 1);
    __assert_metadata(f, 2, 1, 2, 1, 0, 1);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata1 is greater. */
static MunitResult test_load_md_1(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     3, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     1 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     1 /* First index                          */);

    __load(f);

    __assert_result_metadata(f, 3, 0, 1);

    /* The metadata files got updated. */
    __assert_metadata(f, 1, 1, 5, 3, 0, 1);
    __assert_metadata(f, 2, 1, 4, 3, 0, 1);

    return MUNIT_OK;
}

/* The data directory has both metadata files, but metadata2 is greater. */
static MunitResult test_load_md_2(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     1 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     1 /* First index                          */);

    __load(f);

    __assert_result_metadata(f, 2, 0, 1);

    /* The metadata files got updated. */
    __assert_metadata(f, 1, 1, 3, 2, 0, 1);
    __assert_metadata(f, 2, 1, 4, 2, 0, 1);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected number of bytes. In this case the
 * file is not considered at all, and the effect is as if this was a brand new
 * server. */
static MunitResult test_load_md_short(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    uint8_t buf[16];

    (void)params;

    test_dir_write_file(f->dir, "metadata1", buf, sizeof buf);

    __load(f);

    /* Term is 0, voted for is 0 and first index is 1. */
    __assert_result_metadata(f, 0, 0, 1);

    /* Both metadata files got created. */
    __assert_metadata(f, 1, 1, 1, 0, 0, 1);
    __assert_metadata(f, 2, 1, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* The metadata1 file has not the expected format. */
static MunitResult test_load_md_bad_format(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     2, /* Format                               */
                     1, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __assert_load_error(f, RAFT_ERR_IO, "load metadata1: unknown format 2");

    return MUNIT_OK;
}

/* The metadata1 file has not a valid version. */
static MunitResult test_load_md_bad_version(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     0, /* Version                              */
                     1, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __assert_load_error(f, RAFT_ERR_IO,
                        "load metadata1: version is set to zero");

    return MUNIT_OK;
}

/* The metadata1 file has not a valid term. */
static MunitResult test_load_md_bad_term(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     1, /* Version                              */
                     0, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __assert_load_error(f, RAFT_ERR_IO, "load metadata1: term is set to zero");

    return MUNIT_OK;
}

/* The data directory has both metadata files, but they have the same
 * version. */
static MunitResult test_load_md_same_version(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __write_metadata(f, /*                                      */
                     2, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     2, /* Term                                 */
                     0, /* Voted for                            */
                     0 /* First index                          */);

    __assert_load_error(f, RAFT_ERR_IO,
                        "metadata1 and metadata2 are both at version 2");

    return MUNIT_OK;
}

/* Unknown files in the data directory are ignored. */
static MunitResult test_load_ignore(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
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

    __load(f);

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete format data. */
static MunitResult test_load_short_format(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, __WORD_SIZE / 2);

    __assert_load_error(
        f, RAFT_ERR_IO,
        "open segment 1: read format: got 4 bytes instead of 8");

    return MUNIT_OK;
}

/* The data directory has an open segment which has an incomplete batch
 * preamble. */
static MunitResult test_load_short_preamble(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t offset =
        __WORD_SIZE /* Format version */ + __WORD_SIZE /* Checksums */;

    (void)params;

    __write_open_segment(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __assert_load_error(f, RAFT_ERR_IO,
                        "open segment 1: batch 1: read "
                        "preamble: got 8 bytes instead of 16");

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch header. */
static MunitResult test_load_short_header(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE /* Partial batch header */;

    (void)params;

    __write_open_segment(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __assert_load_error(f, RAFT_ERR_IO,
                        "open segment 1: batch 1: read header: got "
                        "8 bytes instead of 16");

    return MUNIT_OK;
}

/* The data directory has an open segment which has incomplete batch data. */
static MunitResult test_load_short_data(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE + /* Format version */
                    __WORD_SIZE + /* Checksums */
                    __WORD_SIZE + /* Number of entries */
                    __WORD_SIZE + /* Entry term */
                    __WORD_SIZE + /* Entry type and data size */
                    __WORD_SIZE / 2 /* Partial entry data */;

    (void)params;

    __write_open_segment(f, 1, 1);

    test_dir_truncate_file(f->dir, __OPEN_FILENAME_1, offset);

    __assert_load_error(f, RAFT_ERR_IO,
                        "open segment 1: batch 1: read data: got "
                        "4 bytes instead of 8");

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch header. */
static MunitResult test_load_corrupt_header(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */;
    uint8_t buf[__WORD_SIZE];
    void *cursor = &buf;

    (void)params;

    /* Render invalid checksums */
    raft__put64(&cursor, 123);

    __write_closed_segment(f, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __assert_load_error(f, RAFT_ERR_IO_CORRUPT,
                        "closed segment 1-1: batch 1: corrupted header");

    return MUNIT_OK;
}

/* The data directory has an open segment which has corrupted batch data. */
static MunitResult test_load_corrupt_data(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;
    size_t offset = __WORD_SIZE /* Format version */ +
                    __WORD_SIZE / 2 /* Header checksum */;
    uint8_t buf[__WORD_SIZE / 2];
    void *cursor = buf;

    (void)params;

    /* Render an invalid data checksum. */
    raft__put32(&cursor, 123456789);

    __write_closed_segment(f, 1, 1);

    test_dir_overwrite_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf,
                            offset);

    __assert_load_error(f, RAFT_ERR_IO_CORRUPT,
                        "closed segment 1-1: batch 1: corrupted data");

    return MUNIT_OK;
}

/* The data directory has a closed segment whose first index does not match what
 * we expect. */
static MunitResult test_load_closed_bad_index(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_closed_segment(f, 2, 1);

    __assert_load_error(f, RAFT_ERR_IO_CORRUPT,
                        "closed segment 2-2: expected first index to be 1");

    return MUNIT_OK;
}

/* The data directory has an empty closed segment. */
static MunitResult test_load_closed_empty(const MunitParameter params[],
                                          void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, NULL, 0);

    __assert_load_error(f, RAFT_ERR_IO_CORRUPT,
                        "closed segment 1-1: file is empty");

    return MUNIT_OK;
}

/* The data directory has a closed segment with an unexpected format. */
static MunitResult test_load_closed_bad_format(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;
    uint8_t buf[8] = {2, 0, 0, 0, 0, 0, 0, 0};

    (void)params;

    test_dir_write_file(f->dir, __CLOSED_FILENAME_1, buf, sizeof buf);

    __assert_load_error(f, RAFT_ERR_IO,
                        "closed segment 1-1: unexpected format version: 2");

    return MUNIT_OK;
}

/* The data directory has a closed segment with entries that are no longer
 * needed. */
static MunitResult test_load_closed_not_needed(const MunitParameter params[],
                                               void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_metadata(f, /*                                      */
                     1, /* Metadata file index                  */
                     1, /* Format                               */
                     2, /* Version                              */
                     3, /* Term                                 */
                     0, /* Voted for                            */
                     2 /* First index                          */);
    __write_closed_segment(f, 1, 1);

    __load(f);

    /* The segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has a valid closed and open segments. */
static MunitResult test_load_closed(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_closed_segment(f, 1, 2);
    __write_closed_segment(f, 3, 1);
    __write_open_segment(f, 1, 1);

    __load(f);

    __assert_result_entries(f, 4);

    return MUNIT_OK;
}

/* The data directory has an open segment which is not readable. */
static MunitResult test_load_open_no_access(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_open_segment(f, 1, 1);

    test_dir_unreadable_file(f->dir, __OPEN_FILENAME_1);

    __assert_load_error(f, RAFT_ERR_IO, "open segment 1: permission denied");

    return MUNIT_OK;
}

/* The data directory has an empty open segment. */
static MunitResult test_load_open_empty(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file(f->dir, __OPEN_FILENAME_1, NULL, 0);

    __load(f);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an open segment with format set to 0 and non-zero
 * content. */
static MunitResult test_load_open_zero_format(const MunitParameter params[],
                                              void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 0); /* Format version */

    __write_open_segment(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __assert_load_error(f, RAFT_ERR_IO,
                        "open segment 1: unexpected format version: 0");

    return MUNIT_OK;
}

/* The data directory has an open segment with an unexpected format. */
static MunitResult test_load_open_bad_format(const MunitParameter params[],
                                             void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE /* Format version */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 2); /* Format version */

    __write_open_segment(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __assert_load_error(f, RAFT_ERR_IO,
                        "open segment 1: unexpected format version: 2");

    return MUNIT_OK;
}

/* The data directory has a freshly allocated open segment filled with zeros. */
static MunitResult test_load_open_all_zeros(const MunitParameter params[],
                                            void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, 256);

    __load(f);

    /* The empty segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an allocated open segment which contains non-zero
 * corrupted data in its second batch. */
static MunitResult test_load_open_not_all_zeros(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
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

    __write_open_segment(f, 1, 1);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load(f);

    /* The segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The first batch has been loaded */
    munit_assert_int(f->request.result.read_state.n, ==, 1);

    return MUNIT_OK;
}

/* Out of memory while checking if an open segment is all filled with zeros. */
static MunitResult test_load_open_all_zeros_oom(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;

    (void)params;

    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_1, 256);

    test_heap_fault_config(&f->heap, 1, 1);
    test_heap_fault_enable(&f->heap);

    __assert_load_error(f, RAFT_ERR_NOMEM,
                        "open segment 1: check if segment is all "
                        "zeros: can't allocate read buffer");

    return MUNIT_OK;
}

/* The data directory has an open segment with a partially written batch that
 * needs to be truncated. */
static MunitResult test_load_open_truncate(const MunitParameter params[],
                                           void *data)
{
    struct fixture *f = data;
    uint8_t buf[256];

    (void)params;

    __write_open_segment(f, 1, 1);

    memset(buf, 0, sizeof buf);

    test_dir_append_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    __load(f);

    return MUNIT_OK;
}

/* The data directory has an open segment whose first batch is only
 * partially written. In that case the segment gets removed. */
static MunitResult test_load_open_partial_batch(const MunitParameter params[],
                                                void *data)
{
    struct fixture *f = data;
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

    __write_open_segment(f, 1, 1);

    test_dir_overwrite_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf, 0);

    __load(f);

    /* The partially written segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));

    return MUNIT_OK;
}

/* The data directory has an open segment which has a zero'ed batch preamble. An
 * out-of-memory error occurs when checking if also the rest of the file is
 * zero'ed. */
static MunitResult test_load_open_zero_entries_oom(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;
    uint8_t buf[__WORD_SIZE + /* Format version */
                __WORD_SIZE + /* CRC32 checksums */
                __WORD_SIZE + /* Size */
                __WORD_SIZE /* Batch data */];
    void *cursor = buf;

    (void)params;

    raft__put64(&cursor, 1); /* Format version */
    raft__put64(&cursor, 0); /* CRC32 checksums */
    raft__put64(&cursor, 0); /* Number of entries */

    test_dir_write_file(f->dir, __OPEN_FILENAME_1, buf, sizeof buf);

    test_heap_fault_config(&f->heap, 1, 1);
    test_heap_fault_enable(&f->heap);

    __assert_load_error(f, RAFT_ERR_NOMEM,
                        "open segment 1: check if segment is all "
                        "zeros: can't allocate read buffer");

    return MUNIT_OK;
}

/* The data directory has two segments, with the second having an entry. */
static MunitResult test_load_open_second(const MunitParameter params[],
                                         void *data)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    __write_open_segment(f, 1, 1);

    /* Second segment */
    __write_open_segment(f, 2, 1);

    __load(f);

    /* The first and second segments have been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has two segments, with the second one filled with
 * zeros. */
static MunitResult test_load_open_second_all_zeros(
    const MunitParameter params[],
    void *data)
{
    struct fixture *f = data;

    (void)params;

    /* First segment. */
    __write_open_segment(f, 1, 1);

    /* Second segment */
    test_dir_write_file_with_zeros(f->dir, __OPEN_FILENAME_2, 256);

    __load(f);

    /* The first segment has been renamed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_1));
    munit_assert_true(test_dir_has_file(f->dir, __CLOSED_FILENAME_1));

    /* The second segment has been removed. */
    munit_assert_false(test_dir_has_file(f->dir, __OPEN_FILENAME_2));

    return MUNIT_OK;
}

/* The data directory has a valid open segment. */
static MunitResult test_load_open(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __write_open_segment(f, 1, 1);

    __load(f);

    return MUNIT_OK;
}

static char *load_oom_heap_fault_delay[] = {"0", "1", "2", "3",
                                            "4", "5", "6", NULL};
static char *load_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum load_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, load_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, load_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
static MunitResult test_load_oom(const MunitParameter params[], void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __write_open_segment(f, 1, 1);
    __write_open_segment(f, 2, 1);

    test_heap_fault_enable(&f->heap);

    rv = raft_io_uv_store__load(&f->store, &f->request, f->errmsg);
    munit_assert_int(rv, ==, RAFT_ERR_NOMEM);

    return MUNIT_OK;
}

static MunitTest load_tests[] = {
    {"/empty-dir", test_load_empty_dir, setup, tear_down, 0, NULL},
    {"/md-open-err", test_load_md_open_err, setup, tear_down, 0, NULL},
    {"/md-only-1", test_load_md_only_1, setup, tear_down, 0, NULL},
    {"/md-1", test_load_md_1, setup, tear_down, 0, NULL},
    {"/md-2", test_load_md_2, setup, tear_down, 0, NULL},
    {"/md-short", test_load_md_short, setup, tear_down, 0, NULL},
    {"/md-bad-format", test_load_md_bad_format, setup, tear_down, 0, NULL},
    {"/md-bad-version", test_load_md_bad_version, setup, tear_down, 0, NULL},
    {"/md-bad-term", test_load_md_bad_term, setup, tear_down, 0, NULL},
    {"/md-same-version", test_load_md_same_version, setup, tear_down, 0, NULL},
    {"/ignore", test_load_ignore, setup, tear_down, 0, NULL},
    {"/short-format", test_load_short_format, setup, tear_down, 0, NULL},
    {"/short-preamble", test_load_short_preamble, setup, tear_down, 0, NULL},
    {"/short-header", test_load_short_header, setup, tear_down, 0, NULL},
    {"/short-data", test_load_short_data, setup, tear_down, 0, NULL},
    {"/corrupt-header", test_load_corrupt_header, setup, tear_down, 0, NULL},
    {"/corrupt-data", test_load_corrupt_data, setup, tear_down, 0, NULL},
    {"/closed-bad-index", test_load_closed_bad_index, setup, tear_down, 0,
     NULL},
    {"/closed-empty", test_load_closed_empty, setup, tear_down, 0, NULL},
    {"/closed-bad-format", test_load_closed_bad_format, setup, tear_down, 0,
     NULL},
    {"/closed-not-needed", test_load_closed_not_needed, setup, tear_down, 0,
     NULL},
    {"/closed", test_load_closed, setup, tear_down, 0, NULL},
    {"/open-no-access", test_load_open_no_access, setup, tear_down, 0, NULL},
    {"/open-empty", test_load_open_empty, setup, tear_down, 0, NULL},
    {"/open-zero-format", test_load_open_zero_format, setup, tear_down, 0,
     NULL},
    {"/open-bad-format", test_load_open_bad_format, setup, tear_down, 0, NULL},
    {"/open-all-zeros", test_load_open_all_zeros, setup, tear_down, 0, NULL},
    {"/open-not-all-zeros", test_load_open_not_all_zeros, setup, tear_down, 0,
     NULL},
    {"/open-all-zeros-oom", test_load_open_all_zeros_oom, setup, tear_down, 0,
     NULL},
    {"/open-truncate", test_load_open_truncate, setup, tear_down, 0, NULL},
    {"/open-partial-batch", test_load_open_partial_batch, setup, tear_down, 0,
     NULL},
    {"/open-zero-entries-oom", test_load_open_zero_entries_oom, setup,
     tear_down, 0, NULL},
    {"/open-second", test_load_open_second, setup, tear_down, 0, NULL},
    {"/open-second-all-zeros", test_load_open_second_all_zeros, setup,
     tear_down, 0, NULL},
    {"/open", test_load_open, setup, tear_down, 0, NULL},
    {"/oom", test_load_oom, setup, tear_down, 0, load_oom_params},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv_store___term
 */

/* The data directory is not executable and the metadata file can't be open. */
static MunitResult test_term_open_error(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    __load(f);

    /* Make the data directory not readable and try to write the term. */
    test_dir_unexecutable(f->dir);

    rv = raft_io_uv_store__term(&f->store, &f->request, f->errmsg);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->errmsg,
                              "store metadata file 1: open: permission denied");

    return MUNIT_OK;
}

/* Very first write term request after initialization. */
static MunitResult test_term_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __load(f);

    /* Write the term. */
    f->request.args.write_term.term = 1;

    __term(f);

    /* The metadata1 file got updated, while metadata2 remains behind. */
    __assert_metadata(f, 1, 1, 3, 1, 0, 1);
    __assert_metadata(f, 2, 1, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* At second write after the very first one (which writes into metadata1),
 * metadata2 is written. */
static MunitResult test_term_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    __load(f);

    /* First write term request. */
    f->request.args.write_term.term = 1;

    __term(f);

    /* Second write term request. */
    f->request.args.write_term.term = 2;

    __term(f);

    /* The metadata2 file got updated, while metadata1 remains behind. */
    __assert_metadata(f, 1, 1, 3, 1, 0, 1);
    __assert_metadata(f, 2, 1, 4, 2, 0, 1);

    return MUNIT_OK;
}

static MunitTest term_tests[] = {
    {"/open-error", test_term_open_error, setup, tear_down, 0, NULL},
    {"/first", test_term_first, setup, tear_down, 0, NULL},
    {"/second", test_term_second, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * raft_io_uv_store__vote
 */

/* The data directory is not executable and the metadata file can't be open. */
static MunitResult test_vote_open_error(const MunitParameter params[],
                                        void *data)
{
    struct fixture *f = data;
    int rv;

    (void)params;

    /* Issue the initial read state request. */
    __load(f);

    /* Make the data directory not readable and try to write the vote. */
    test_dir_unexecutable(f->dir);

    f->request.args.write_vote.server_id = 1;

    rv = raft_io_uv_store__term(&f->store, &f->request, f->errmsg);
    munit_assert_int(rv, ==, RAFT_ERR_IO);

    munit_assert_string_equal(f->errmsg,
                              "store metadata file 1: open: permission denied");

    return MUNIT_OK;
}

/* Very first write vote request after initialization. */
static MunitResult test_vote_first(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    /* Issue the initial read state request. */
    __load(f);

    /* Write vote request. */
    f->request.args.write_vote.server_id = 1;

    __vote(f);

    /* The metadata1 file got updated, while metadata2 remains behind. */
    __assert_metadata(f, 1, 1, 3, 0, 1, 1);
    __assert_metadata(f, 2, 1, 2, 0, 0, 1);

    return MUNIT_OK;
}

/* At second write after the very first one (which writes into metadata1),
 * metadata2 is written. */
static MunitResult test_vote_second(const MunitParameter params[], void *data)
{
    struct fixture *f = data;

    (void)params;

    /* Issue the initial read state request. */
    __load(f);

    /* First vote term request. */
    f->request.args.write_vote.server_id = 1;

    __vote(f);

    /* Second write vote term request. */
    f->request.args.write_vote.server_id = 2;

    __vote(f);

    /* The metadata2 file got updated, while metadata1 remains behind. */
    __assert_metadata(f, 1, 1, 3, 0, 1, 1);
    __assert_metadata(f, 2, 1, 4, 0, 2, 1);

    return MUNIT_OK;
}

static MunitTest vote_tests[] = {
    {"/open-error", test_vote_open_error, setup, tear_down, 0, NULL},
    {"/first", test_vote_first, setup, tear_down, 0, NULL},
    {"/second", test_vote_second, setup, tear_down, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_io_uv_store_suites[] = {
    {"/init", init_tests, NULL, 1, 0}, {"/load", load_tests, NULL, 1, 0},
    {"/term", term_tests, NULL, 1, 0}, {"/vote", vote_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
