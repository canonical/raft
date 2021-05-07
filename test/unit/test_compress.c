#include "../../src/compress.h"
#include "../lib/munit.h"
#include "../lib/runner.h"

#include <sys/random.h>
#ifdef LZ4_AVAILABLE
#include <lz4frame.h>
#endif

SUITE(Compress)

struct raft_buffer getBufWithRandom(size_t len)
{
    struct raft_buffer buf = {0};
    buf.len = len;
    buf.base = munit_malloc(buf.len);
    munit_assert_ptr_not_null(buf.base);

    size_t offset = 0;
    /* Write as many random ints in buf as possible */
    for(size_t n = buf.len / sizeof(int); n > 0; n--) {
        *((int*)(buf.base) + offset) = rand();
        offset += 1;
    }

    /* Fill the remaining bytes */
    size_t rem = buf.len % sizeof(int);
    /* Offset will now be used in char* arithmetic */
    offset *= sizeof(int);
    if (rem) {
        int r_int = rand();
        for (unsigned i = 0; i < rem; i++) {
            *((char*)buf.base + offset) = *((char*)&r_int + i);
            offset++;
        }
    }

    munit_assert_ulong(offset, ==, buf.len);
    return buf;
}

struct raft_buffer getBufWithNonRandom(size_t len)
{
    struct raft_buffer buf = {0};
    buf.len = len;
    buf.base = munit_malloc(buf.len);
    munit_assert_ptr_not_null(buf.base);

    memset(buf.base, 0xAC, buf.len);
    return buf;
}

#ifdef LZ4_AVAILABLE
static char* len_one_params[] = {
/*    16B   1KB     64KB     4MB        128MB */
      "16", "1024", "65536", "4194304", "134217728",
/*    Around Blocksize*/
      "65516", "65517", "65518", "65521", "65535",
      "65537", "65551", "65555", "65556",
/*    Ugly lengths */
      "1", "9", "123450", "1337", "6655111",
      NULL
};

static MunitParameterEnum random_one_params[] = {
    { "len_one", len_one_params },
    { NULL, NULL },
};

TEST(Compress, compressDecompressRandomOne, NULL, NULL, 0,
     random_one_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};

    /* Fill a buffer with random data */
    size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    struct raft_buffer buf = getBufWithRandom(len);

    /* Assert that after compression and decompression the data is unchanged */
    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, len);
    munit_assert_int(memcmp(decompressed.base, buf.base, buf.len), ==, 0);

    raft_free(compressed.base);
    raft_free(decompressed.base);
    free(buf.base);
    return MUNIT_OK;
}

static char* len_nonrandom_one_params[] = {
/*    4KB     64KB     4MB        1GB           3GB */
      "4096", "65536", "4194304", "1073741824", "3221225472",
/*    Around Blocksize*/
      "65516", "65517", "65518", "65521", "65535",
      "65537", "65551", "65555", "65556",
/*    Ugly lengths */
      "993450", "31337", "83883825",
      NULL
};

static MunitParameterEnum nonrandom_one_params[] = {
    { "len_one", len_nonrandom_one_params },
    { NULL, NULL },
};

TEST(Compress, compressDecompressNonRandomOne, NULL, NULL, 0,
     nonrandom_one_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};

    /* Fill a buffer with non-random data */
    size_t len = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    struct raft_buffer buf = getBufWithNonRandom(len);

    /* Assert that after compression and decompression the data is unchanged and
     * that the compressed data is actually smaller */
    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);
    munit_assert_ulong(compressed.len, <, buf.len);
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, len);
    munit_assert_int(memcmp(decompressed.base, buf.base, buf.len), ==, 0);

    raft_free(compressed.base);
    raft_free(decompressed.base);
    free(buf.base);
    return MUNIT_OK;
}

static char* len_two_params[] = {
      "4194304", "13373", "66",
      NULL
};

static MunitParameterEnum random_two_params[] = {
    { "len_one", len_one_params },
    { "len_two", len_two_params },
    { NULL, NULL },
};

TEST(Compress, compressDecompressRandomTwo, NULL, NULL, 0,
     random_two_params)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};

    /* Fill two buffers with random data */
    size_t len1 = strtoul(munit_parameters_get(params, "len_one"), NULL, 0);
    struct raft_buffer buf1 = getBufWithRandom(len1);
    size_t len2 = strtoul(munit_parameters_get(params, "len_two"), NULL, 0);
    struct raft_buffer buf2 = getBufWithRandom(len2);
    struct raft_buffer bufs[2] = { buf1, buf2 };

    /* Assert that after compression and decompression the data is unchanged */
    munit_assert_int(Compress(bufs, 2, &compressed, errmsg), ==, 0);
    munit_assert_int(Decompress(compressed, &decompressed, errmsg), ==, 0);
    munit_assert_ulong(decompressed.len, ==, buf1.len + buf2.len);
    munit_assert_int(memcmp(decompressed.base, buf1.base, buf1.len), ==, 0);
    munit_assert_int(memcmp((char*)decompressed.base + buf1.len,
                            buf2.base, buf2.len), ==, 0);

    raft_free(compressed.base);
    raft_free(decompressed.base);
    free(buf1.base);
    free(buf2.base);
    return MUNIT_OK;
}

TEST(Compress, compressDecompressCorruption, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};
    struct raft_buffer decompressed = {0};

    /* Fill a buffer with random data */
    size_t len = 2048;
    struct raft_buffer buf = getBufWithRandom(len);

    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, 0);

    /* Corrupt the a data byte after the header */
    munit_assert_ulong(LZ4F_HEADER_SIZE_MAX_RAFT, <, compressed.len);
    ((char*)compressed.base)[LZ4F_HEADER_SIZE_MAX_RAFT] += 1;

    munit_assert_int(Decompress(compressed, &decompressed, errmsg), !=, 0);
    munit_assert_string_equal(errmsg, "LZ4F_decompress ERROR_contentChecksum_invalid");
    munit_assert_ptr_null(decompressed.base);

    raft_free(compressed.base);
    free(buf.base);
    return MUNIT_OK;
}

#else

TEST(Compress, lz4Disabled, NULL, NULL, 0, NULL)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE] = {0};
    struct raft_buffer compressed = {0};

    /* Fill a buffer with random data */
    size_t len = 2048;
    struct raft_buffer buf = getBufWithRandom(len);

    munit_assert_int(Compress(&buf, 1, &compressed, errmsg), ==, RAFT_INVALID);
    munit_assert_ptr_null(compressed.base);

    free(buf.base);
    return MUNIT_OK;
}

#endif /* LZ4_AVAILABLE */
