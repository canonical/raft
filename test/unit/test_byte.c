#include "../../src/byte.h"

#include "../lib/runner.h"

TEST_MODULE(byte);

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

#define CRC32(VALUE) byteCrc32(&(VALUE), sizeof VALUE, 0)

/******************************************************************************
 *
 * byteCrc32
 *
 *****************************************************************************/

TEST_SUITE(crc32);

/* The same data produces the same sum. */
TEST_CASE(crc32, valid, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123456789;
    (void)data;
    (void)params;
    munit_assert_int(CRC32(value1), ==, CRC32(value2));
    return MUNIT_OK;
}

/* Different data produces a different sum. */
TEST_CASE(crc32, invalid, NULL)
{
    uint64_t value1 = 123456789;
    uint64_t value2 = 123466789;
    (void)data;
    (void)params;
    munit_assert_int(CRC32(value1), !=, CRC32(value2));
    return MUNIT_OK;
}

/******************************************************************************
 *
 * Convert to little endian representation (least significant byte first).
 *
 *****************************************************************************/

TEST_SUITE(flip);

/* Convert a 32-bit number. */
TEST_CASE(flip, 32, NULL)
{
    uint32_t value;
    unsigned i;
    (void)data;
    (void)params;
    value = byteFlip32(0x03020100);
    for (i = 0; i < 4; i++) {
        munit_assert_int(*((uint8_t *)&value + i), ==, i);
    }
    return MUNIT_OK;
}

/* Convert a 64-bit number. */
TEST_CASE(flip, 64, NULL)
{
    uint64_t value;
    unsigned i;
    (void)data;
    (void)params;
    value = byteFlip64(0x0706050403020100);
    for (i = 0; i < 8; i++) {
        munit_assert_int(*((uint8_t *)&value + i), ==, i);
    }
    return MUNIT_OK;
}

/******************************************************************************
 *
 * byteGetString
 *
 *****************************************************************************/

TEST_SUITE(get_string);

TEST_CASE(get_string, success, NULL)
{
    uint8_t buf[] = {'h', 'e', 'l', 'l', 'o', 0};
    const void *cursor = buf;
    (void)data;
    (void)params;
    munit_assert_string_equal(byteGetString(&cursor, sizeof buf), "hello");
    munit_assert_ptr_equal(cursor, buf + sizeof buf);
    return MUNIT_OK;
}

TEST_CASE(get_string, malformed, NULL)
{
    uint8_t buf[] = {'h', 'e', 'l', 'l', 'o', 'w'};
    const void *cursor = buf;
    (void)data;
    (void)params;
    munit_assert_ptr_equal(byteGetString(&cursor, sizeof buf), NULL);
    munit_assert_ptr_equal(cursor, buf);
    return MUNIT_OK;
}
