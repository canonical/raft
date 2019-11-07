#include "../../src/byte.h"
#include "../../src/uv_encoding.h"
#include "../lib/runner.h"
#include "../lib/uv.h"

/* Maximum number of blocks a segment can have */
#define MAX_SEGMENT_BLOCKS 4

/* This block size should work fine for all file systems. */
#define SEGMENT_BLOCK_SIZE 4096

/******************************************************************************
 *
 * Fixture
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_UV;
    struct raft_entry *entries;
    unsigned n;
    int count;   /* To generate deterministic entry data */
    int invoked; /* Number of times append_cb was invoked */
    int status;  /* Last status passed to append_cb */
};

static void *setup(const MunitParameter params[], void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    SETUP_UV;
    raft_uv_set_block_size(&f->io, SEGMENT_BLOCK_SIZE);
    raft_uv_set_segment_size(&f->io, SEGMENT_BLOCK_SIZE * MAX_SEGMENT_BLOCKS);
    f->count = 0;
    f->invoked = 0;
    f->status = 0;
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

struct append_req
{
    struct raft_io_append req;
    struct fixture *f;
    struct raft_entry *entries;
    unsigned n;
};

static void appendCb(struct raft_io_append *req, int status)
{
    struct append_req *r = req->data;
    struct fixture *f = r->f;
    unsigned i;
    f->invoked++;
    f->status = status;
    for (i = 0; i < r->n; i++) {
        raft_free(r->entries[i].buf.base);
    }
    raft_free((struct raft_entry *)r->entries);
    free(r);
}

/* Taken from https://github.com/gcc-mirror/gcc/blob/master/libiberty/crc32.c */
static const unsigned table[] = {
    0x00000000, 0x04c11db7, 0x09823b6e, 0x0d4326d9, 0x130476dc, 0x17c56b6b,
    0x1a864db2, 0x1e475005, 0x2608edb8, 0x22c9f00f, 0x2f8ad6d6, 0x2b4bcb61,
    0x350c9b64, 0x31cd86d3, 0x3c8ea00a, 0x384fbdbd, 0x4c11db70, 0x48d0c6c7,
    0x4593e01e, 0x4152fda9, 0x5f15adac, 0x5bd4b01b, 0x569796c2, 0x52568b75,
    0x6a1936c8, 0x6ed82b7f, 0x639b0da6, 0x675a1011, 0x791d4014, 0x7ddc5da3,
    0x709f7b7a, 0x745e66cd, 0x9823b6e0, 0x9ce2ab57, 0x91a18d8e, 0x95609039,
    0x8b27c03c, 0x8fe6dd8b, 0x82a5fb52, 0x8664e6e5, 0xbe2b5b58, 0xbaea46ef,
    0xb7a96036, 0xb3687d81, 0xad2f2d84, 0xa9ee3033, 0xa4ad16ea, 0xa06c0b5d,
    0xd4326d90, 0xd0f37027, 0xddb056fe, 0xd9714b49, 0xc7361b4c, 0xc3f706fb,
    0xceb42022, 0xca753d95, 0xf23a8028, 0xf6fb9d9f, 0xfbb8bb46, 0xff79a6f1,
    0xe13ef6f4, 0xe5ffeb43, 0xe8bccd9a, 0xec7dd02d, 0x34867077, 0x30476dc0,
    0x3d044b19, 0x39c556ae, 0x278206ab, 0x23431b1c, 0x2e003dc5, 0x2ac12072,
    0x128e9dcf, 0x164f8078, 0x1b0ca6a1, 0x1fcdbb16, 0x018aeb13, 0x054bf6a4,
    0x0808d07d, 0x0cc9cdca, 0x7897ab07, 0x7c56b6b0, 0x71159069, 0x75d48dde,
    0x6b93dddb, 0x6f52c06c, 0x6211e6b5, 0x66d0fb02, 0x5e9f46bf, 0x5a5e5b08,
    0x571d7dd1, 0x53dc6066, 0x4d9b3063, 0x495a2dd4, 0x44190b0d, 0x40d816ba,
    0xaca5c697, 0xa864db20, 0xa527fdf9, 0xa1e6e04e, 0xbfa1b04b, 0xbb60adfc,
    0xb6238b25, 0xb2e29692, 0x8aad2b2f, 0x8e6c3698, 0x832f1041, 0x87ee0df6,
    0x99a95df3, 0x9d684044, 0x902b669d, 0x94ea7b2a, 0xe0b41de7, 0xe4750050,
    0xe9362689, 0xedf73b3e, 0xf3b06b3b, 0xf771768c, 0xfa325055, 0xfef34de2,
    0xc6bcf05f, 0xc27dede8, 0xcf3ecb31, 0xcbffd686, 0xd5b88683, 0xd1799b34,
    0xdc3abded, 0xd8fba05a, 0x690ce0ee, 0x6dcdfd59, 0x608edb80, 0x644fc637,
    0x7a089632, 0x7ec98b85, 0x738aad5c, 0x774bb0eb, 0x4f040d56, 0x4bc510e1,
    0x46863638, 0x42472b8f, 0x5c007b8a, 0x58c1663d, 0x558240e4, 0x51435d53,
    0x251d3b9e, 0x21dc2629, 0x2c9f00f0, 0x285e1d47, 0x36194d42, 0x32d850f5,
    0x3f9b762c, 0x3b5a6b9b, 0x0315d626, 0x07d4cb91, 0x0a97ed48, 0x0e56f0ff,
    0x1011a0fa, 0x14d0bd4d, 0x19939b94, 0x1d528623, 0xf12f560e, 0xf5ee4bb9,
    0xf8ad6d60, 0xfc6c70d7, 0xe22b20d2, 0xe6ea3d65, 0xeba91bbc, 0xef68060b,
    0xd727bbb6, 0xd3e6a601, 0xdea580d8, 0xda649d6f, 0xc423cd6a, 0xc0e2d0dd,
    0xcda1f604, 0xc960ebb3, 0xbd3e8d7e, 0xb9ff90c9, 0xb4bcb610, 0xb07daba7,
    0xae3afba2, 0xaafbe615, 0xa7b8c0cc, 0xa379dd7b, 0x9b3660c6, 0x9ff77d71,
    0x92b45ba8, 0x9675461f, 0x8832161a, 0x8cf30bad, 0x81b02d74, 0x857130c3,
    0x5d8a9099, 0x594b8d2e, 0x5408abf7, 0x50c9b640, 0x4e8ee645, 0x4a4ffbf2,
    0x470cdd2b, 0x43cdc09c, 0x7b827d21, 0x7f436096, 0x7200464f, 0x76c15bf8,
    0x68860bfd, 0x6c47164a, 0x61043093, 0x65c52d24, 0x119b4be9, 0x155a565e,
    0x18197087, 0x1cd86d30, 0x029f3d35, 0x065e2082, 0x0b1d065b, 0x0fdc1bec,
    0x3793a651, 0x3352bbe6, 0x3e119d3f, 0x3ad08088, 0x2497d08d, 0x2056cd3a,
    0x2d15ebe3, 0x29d4f654, 0xc5a92679, 0xc1683bce, 0xcc2b1d17, 0xc8ea00a0,
    0xd6ad50a5, 0xd26c4d12, 0xdf2f6bcb, 0xdbee767c, 0xe3a1cbc1, 0xe760d676,
    0xea23f0af, 0xeee2ed18, 0xf0a5bd1d, 0xf464a0aa, 0xf9278673, 0xfde69bc4,
    0x89b8fd09, 0x8d79e0be, 0x803ac667, 0x84fbdbd0, 0x9abc8bd5, 0x9e7d9662,
    0x933eb0bb, 0x97ffad0c, 0xafb010b1, 0xab710d06, 0xa6322bdf, 0xa2f33668,
    0xbcb4666d, 0xb8757bda, 0xb5365d03, 0xb1f740b4};

unsigned byteCrc32(const void *buf, const size_t size, const unsigned init)
{
    unsigned crc = init;
    uint8_t *cursor = (uint8_t *)buf;
    size_t count = size;

    while (count--) {
        crc = (crc << 8) ^ table[((crc >> 24) ^ *cursor) & 255];
        cursor++;
    }
    return crc;
}

/* Set the arguments for the next append entries call. The f->entries array will
 * be populated with N entries each of size SIZE. */
#define CREATE_ENTRIES(N, SIZE)                                  \
    {                                                            \
        int i_;                                                  \
        f->entries = raft_malloc(N * sizeof(struct raft_entry)); \
        f->n = N;                                                \
        munit_assert_ptr_not_null(f->entries);                   \
        for (i_ = 0; i_ < N; i_++) {                             \
            struct raft_entry *entry = &f->entries[i_];          \
            void *cursor;                                        \
            entry->term = 1;                                     \
            entry->type = RAFT_COMMAND;                          \
            entry->buf.base = raft_malloc(SIZE);                 \
            entry->buf.len = SIZE;                               \
            entry->batch = NULL;                                 \
            munit_assert_ptr_not_null(entry->buf.base);          \
            memset(entry->buf.base, 0, entry->buf.len);          \
            cursor = entry->buf.base;                            \
            bytePut64(&cursor, f->count);                        \
            f->count++;                                          \
        }                                                        \
    }

/* Invoke raft_io->append() and assert that it returns the given code. */
#define APPEND(RV)                                                       \
    {                                                                    \
        unsigned i_;                                                     \
        struct append_req *r = munit_malloc(sizeof *r);                  \
        int rv_;                                                         \
        r->f = f;                                                        \
        r->entries = f->entries;                                         \
        r->n = f->n;                                                     \
        r->req.data = r;                                                 \
        rv_ = f->io.append(&f->io, &r->req, f->entries, f->n, appendCb); \
        munit_assert_int(rv_, ==, RV);                                   \
        if (rv_ != 0) {                                                  \
            for (i_ = 0; i_ < f->n; i_++) {                              \
                raft_free(f->entries[i_].buf.base);                      \
            }                                                            \
            raft_free(f->entries);                                       \
            free(r);                                                     \
        }                                                                \
    }

/* Wait for the given number of append request callbacks to fire and check the
 * last status. */
#define WAIT_CB(N, STATUS)                       \
    {                                            \
        int i2;                                  \
        for (i2 = 0; i2 < 10; i2++) {            \
            LOOP_RUN(1);                         \
            if (f->invoked == N) {               \
                break;                           \
            }                                    \
        }                                        \
        munit_assert_int(f->invoked, ==, N);     \
        munit_assert_int(f->status, ==, STATUS); \
        f->invoked = 0;                          \
    }

/******************************************************************************
 *
 * Assertions
 *
 *****************************************************************************/

/* Assert that the open segment with the given counter has format version 1 and
 * N entries with a total data size of S bytes. */
#define ASSERT_SEGMENT(COUNTER, N, SIZE)                                     \
    {                                                                        \
        struct raft_buffer buf;                                              \
        const void *cursor;                                                  \
        char filename[strlen("open-N") + 1];                                 \
        unsigned i_ = 0;                                                     \
        size_t total_data_size = 0;                                          \
                                                                             \
        sprintf(filename, "open-%d", COUNTER);                               \
                                                                             \
        buf.len = MAX_SEGMENT_BLOCKS * SEGMENT_BLOCK_SIZE;                   \
        if (buf.len < 8 + /* segment header */ +SIZE) {                      \
            size_t rest;                                                     \
            buf.len = 8 /* segment header */ + SIZE;                         \
            rest = buf.len % SEGMENT_BLOCK_SIZE;                             \
            if (rest != 0) {                                                 \
                buf.len += SEGMENT_BLOCK_SIZE - rest;                        \
            }                                                                \
        }                                                                    \
        buf.base = munit_malloc(buf.len);                                    \
                                                                             \
        test_dir_read_file(f->dir, filename, buf.base, buf.len);             \
                                                                             \
        cursor = buf.base;                                                   \
        munit_assert_int(byteGet64(&cursor), ==, 1);                         \
                                                                             \
        while (i_ < N) {                                                     \
            unsigned crc1 = byteGet32(&cursor);                              \
            unsigned crc2 = byteGet32(&cursor);                              \
            const void *header = cursor;                                     \
            const void *content;                                             \
            unsigned n_ = byteGet64(&cursor);                                \
            struct raft_entry *entries = munit_malloc(n_ * sizeof *entries); \
            unsigned j_;                                                     \
            unsigned crc;                                                    \
            size_t data_size = 0;                                            \
                                                                             \
            for (j_ = 0; j_ < n_; j_++) {                                    \
                struct raft_entry *entry = &entries[j_];                     \
                                                                             \
                entry->term = byteGet64(&cursor);                            \
                entry->type = byteGet8(&cursor);                             \
                byteGet8(&cursor);                                           \
                byteGet8(&cursor);                                           \
                byteGet8(&cursor);                                           \
                entry->buf.len = byteGet32(&cursor);                         \
                                                                             \
                munit_assert_int(entry->term, ==, 1);                        \
                munit_assert_int(entry->type, ==, RAFT_COMMAND);             \
                                                                             \
                data_size += entry->buf.len;                                 \
            }                                                                \
                                                                             \
            crc = byteCrc32(header, 8 + 16 * n_, 0);                         \
            munit_assert_int(crc, ==, crc1);                                 \
                                                                             \
            content = cursor;                                                \
                                                                             \
            for (j_ = 0; j_ < n_; j_++) {                                    \
                struct raft_entry *entry = &entries[j_];                     \
                uint64_t value;                                              \
                value = byteFlip64(*(uint64_t *)cursor);                     \
                munit_assert_int(value, ==, i_);                             \
                cursor = (uint8_t *)cursor + entry->buf.len;                 \
                i_++;                                                        \
            }                                                                \
                                                                             \
            crc = byteCrc32(content, data_size, 0);                          \
            munit_assert_int(crc, ==, crc2);                                 \
                                                                             \
            free(entries);                                                   \
                                                                             \
            total_data_size += data_size;                                    \
        }                                                                    \
                                                                             \
        munit_assert_int(total_data_size, ==, SIZE);                         \
        free(buf.base);                                                      \
    }

/******************************************************************************
 *
 * uvAppend
 *
 *****************************************************************************/

SUITE(UvAppend)

/* Append the very first batch of entries. */
TEST(UvAppend, first, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);
    ASSERT_SEGMENT(1, 1, 64);
    return MUNIT_OK;
}

/* The very first batch of entries to append is bigger than the regular open
 * segment size. */
TEST(UvAppend, firstBig, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct uv *uv = f->io.impl;
    CREATE_ENTRIES(MAX_SEGMENT_BLOCKS, uv->block_size);
    APPEND(0);
    WAIT_CB(1, 0);
    ASSERT_SEGMENT(1, MAX_SEGMENT_BLOCKS, MAX_SEGMENT_BLOCKS * uv->block_size);
    return MUNIT_OK;
}

/* The second batch of entries to append is bigger than the regular open
 * segment size. */
TEST(UvAppend, secondBig, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    struct uv *uv = f->io.impl;

    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(MAX_SEGMENT_BLOCKS, uv->block_size);
    APPEND(0);
    WAIT_CB(1, 0);

    return MUNIT_OK;
}

/* Write the very first entry and then another one, both fitting in the same
 * block. */
TEST(UvAppend, fitBlock, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    ASSERT_SEGMENT(1, 2, 128);

    return MUNIT_OK;
}

/* Write an entry that fills the first block exactly and then another one. */
TEST(UvAppend, matchBlock, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t size;

    size = SEGMENT_BLOCK_SIZE;
    size -= sizeof(uint64_t) + /* Format */
            sizeof(uint64_t) + /* Checksums */
            8 + 16;            /* Header */

    CREATE_ENTRIES(1, size);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    ASSERT_SEGMENT(1, 2, size + 64);

    return MUNIT_OK;
}

/* Write an entry that exceeds the first block, then another one that fits in
 * the second block, then a third one that fills the rest of the second block
 * plus the whole third block exactly, and finally a fourth entry that fits in
 * the fourth block */
TEST(UvAppend, exceedBlock, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t written;
    size_t size1;
    size_t size2;

    size1 = SEGMENT_BLOCK_SIZE;

    CREATE_ENTRIES(1, size1);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    written = sizeof(uint64_t) +     /* Format version */
              2 * sizeof(uint32_t) + /* CRC sums of first batch */
              8 + 16 +               /* Header of first batch */
              size1 +                /* Size of first batch */
              2 * sizeof(uint32_t) + /* CRC of second batch */
              8 + 16 +               /* Header of second batch */
              64;                    /* Size of second batch */

    /* Write a third entry that fills the second block exactly */
    size2 = SEGMENT_BLOCK_SIZE - (written % SEGMENT_BLOCK_SIZE);
    size2 -= (2 * sizeof(uint32_t) + 8 + 16);
    size2 += SEGMENT_BLOCK_SIZE;

    CREATE_ENTRIES(1, size2);
    APPEND(0);
    WAIT_CB(1, 0);

    /* Write a fourth entry */
    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    ASSERT_SEGMENT(1, 4, size1 + 64 + size2 + 64);

    return MUNIT_OK;
}

/* If an append request is submitted before the write operation of the previous
 * append request is started, then a single write will be performed for both
 * requests. */
TEST(UvAppend, batch, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    WAIT_CB(2, 0);

    return MUNIT_OK;
}

/* An append request submitted while a write operation is in progress gets
 * executed only when the write completes. */
TEST(UvAppend, wait, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    LOOP_RUN(1);

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    WAIT_CB(1, 0);
    WAIT_CB(1, 0);

    return MUNIT_OK;
}

/* Several batches with different size gets appended in fast pace, which forces
 * the segment arena to grow. */
TEST(UvAppend, resizeArena, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(2, 64);
    APPEND(0);

    CREATE_ENTRIES(1, SEGMENT_BLOCK_SIZE);
    APPEND(0);

    CREATE_ENTRIES(2, 64);
    APPEND(0);

    CREATE_ENTRIES(1, SEGMENT_BLOCK_SIZE);
    APPEND(0);

    CREATE_ENTRIES(1, SEGMENT_BLOCK_SIZE);
    APPEND(0);

    WAIT_CB(5, 0);

    ASSERT_SEGMENT(1, 7, 64 * 4 + SEGMENT_BLOCK_SIZE * 3);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. */
TEST(UvAppend, truncate, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    return MUNIT_SKIP; /* FIXME: flaky */

    CREATE_ENTRIES(2, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(2, 64);

    APPEND(0);

    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    CREATE_ENTRIES(2, 64);
    APPEND(0);

    WAIT_CB(2, 0);

    return MUNIT_OK;
}

/* A few append requests get queued, then a truncate request comes in and other
 * append requests right after, before truncation is fully completed. However
 * the backend is closed before the truncation request can be processed. */
TEST(UvAppend, truncateClosing, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    int rv;

    CREATE_ENTRIES(2, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    CREATE_ENTRIES(2, 64);

    APPEND(0);

    rv = f->io.truncate(&f->io, 2);
    munit_assert_int(rv, ==, 0);

    CREATE_ENTRIES(2, 64);
    APPEND(0);

    return MUNIT_OK;
}

/* The counters of the open segments get increased as they are closed. */
TEST(UvAppend, counter, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    size_t size = SEGMENT_BLOCK_SIZE;
    int i;

    for (i = 0; i < 10; i++) {
        CREATE_ENTRIES(1, size);
        APPEND(0);
        WAIT_CB(1, 0);
    }

    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000003"));
    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000004-0000000000000006"));
    munit_assert_true(test_dir_has_file(f->dir, "open-4"));

    return MUNIT_OK;
}

/* If the I/O instance is closed, all pending append requests get canceled. */
TEST(UvAppend, cancel, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    UV_CLOSE;

    WAIT_CB(1, RAFT_CANCELED);

    return MUNIT_OK;
}

/* An error occurs while performing a write. */
TEST(UvAppend, writeError, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;
    aio_context_t ctx = 0;

    /* FIXME: doesn't fail anymore after
     * https://github.com/CanonicalLtd/raft/pull/49 */
    return MUNIT_SKIP;

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    test_aio_fill(&ctx, 0);

    WAIT_CB(1, RAFT_IOERR);

    test_aio_destroy(ctx);

    return MUNIT_OK;
}

static char *error_oom_heap_fault_delay[] = {"0", "1", NULL};
static char *error_oom_heap_fault_repeat[] = {"1", NULL};

static MunitParameterEnum error_oom_params[] = {
    {TEST_HEAP_FAULT_DELAY, error_oom_heap_fault_delay},
    {TEST_HEAP_FAULT_REPEAT, error_oom_heap_fault_repeat},
    {NULL, NULL},
};

/* Out of memory conditions. */
TEST(UvAppend, oom, setup, tear_down, 0, error_oom_params)
{
    struct fixture *f = data;
    (void)params;

    CREATE_ENTRIES(1, 64);

    test_heap_fault_enable(&f->heap);

    APPEND(RAFT_NOMEM);

    return MUNIT_OK;
}

TEST_SUITE(close)

TEST_SETUP(close, setup)
TEST_TEAR_DOWN(close, tear_down)

/* The uv instance is closed while a write request is in progress. */
TEST(UvAppend, closeDuringWrite, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(1, 64);
    APPEND(0);

    LOOP_RUN(1);

    UV_CLOSE;

    WAIT_CB(1, 0);

    return MUNIT_OK;
}

/* When the writer gets closed it tells the writer to close the segment that
 * it's currently writing. */
TEST(UvAppend, currentSegment, setup, tear_down, 0, NULL)
{
    struct fixture *f = data;

    CREATE_ENTRIES(1, 64);
    APPEND(0);
    WAIT_CB(1, 0);

    UV_CLOSE;

    LOOP_RUN(2);

    munit_assert_true(
        test_dir_has_file(f->dir, "0000000000000001-0000000000000001"));

    return MUNIT_OK;
}
