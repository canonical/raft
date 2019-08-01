#include "../../include/raft.h"

#include "../lib/runner.h"

TEST_MODULE(error);

/******************************************************************************
 *
 * raft_strerror
 *
 *****************************************************************************/

TEST_SUITE(strerror);

#define TEST_CASE_STRERROR(CODE, MSG)                        \
    TEST_CASE(strerror, CODE, NULL)                          \
    {                                                        \
        (void)data;                                          \
        (void)params;                                        \
        munit_assert_string_equal(raft_strerror(CODE), MSG); \
        return MUNIT_OK;                                     \
    }

RAFT_ERRNO_MAP(TEST_CASE_STRERROR);

TEST_CASE(strerror, default, NULL)
{
    (void)data;
    (void)params;
    munit_assert_string_equal(raft_strerror(666), "unknown error");
    return MUNIT_OK;
}
