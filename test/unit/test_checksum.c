#include "../../src/checksum.h"

#include "../lib/munit.h"

/**
 * raft__crc32
 */

/* The same data produces the same sum. */
static MunitResult test_crc32_valid(const MunitParameter params[], void *data)
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

    crc1 = raft__crc32(buf1, size1);
    crc2 = raft__crc32(buf2, size2);

    munit_assert_int(crc1, ==, crc2);

    return MUNIT_OK;
}

/* Different data produces a different sum. */
static MunitResult test_crc32_invalid(const MunitParameter params[], void *data)
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

    crc1 = raft__crc32(buf1, size1);
    crc2 = raft__crc32(buf2, size2);

    munit_assert_int(crc1, !=, crc2);

    return MUNIT_OK;
}

static MunitTest crc32_tests[] = {
    {"/valid", test_crc32_valid, NULL, NULL, 0, NULL},
    {"/invalid", test_crc32_invalid, NULL, NULL, 0, NULL},
    {NULL, NULL, NULL, NULL, 0, NULL},
};

/**
 * Test suite
 */

MunitSuite raft_checksum_suites[] = {
    {"/crc32", crc32_tests, NULL, 1, 0},
    {NULL, NULL, NULL, 0, 0},
};
