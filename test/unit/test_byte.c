#include "../../src/byte.h"

#include "../lib/runner.h"

TEST_MODULE(checksum);

/**
 * byteCrc32
 */

TEST_SUITE(crc32);

/* The same data produces the same sum. */
TEST_CASE(crc32, valid, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123456789;
    void *buf1 = &value1;
    void *buf2 = &value2;
    size_t size1 = sizeof value1;
    size_t size2 = sizeof value2;
    unsigned crc1;
    unsigned crc2;

    (void)data;
    (void)params;

    crc1 = byteCrc32(buf1, size1, 0);
    crc2 = byteCrc32(buf2, size2, 0);

    munit_assert_int(crc1, ==, crc2);

    return MUNIT_OK;
}

/* Different data produces a different sum. */
TEST_CASE(crc32, invalid, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123466789;
    void *buf1 = &value1;
    void *buf2 = &value2;
    size_t size1 = sizeof value1;
    size_t size2 = sizeof value2;
    unsigned crc1;
    unsigned crc2;

    (void)data;
    (void)params;

    crc1 = byteCrc32(buf1, size1, 0);
    crc2 = byteCrc32(buf2, size2, 0);

    munit_assert_int(crc1, !=, crc2);

    return MUNIT_OK;
}
