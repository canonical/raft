#include "../../include/raft.h"

#include "../lib/runner.h"

TEST_MODULE(heap);

/******************************************************************************
 *
 * Default heap functions
 *
 *****************************************************************************/

TEST_SUITE(default);

TEST_CASE(default, malloc, NULL)
{
    void *p;
    (void)data;
    (void)params;
    p = raft_malloc(8);
    munit_assert_ptr_not_null(p);
    raft_free(p);
    return MUNIT_OK;
}

TEST_CASE(default, calloc, NULL)
{
    void *p;
    (void)data;
    (void)params;
    p = raft_calloc(1, 8);
    munit_assert_ptr_not_null(p);
    munit_assert_int(*(uint64_t*)p, ==, 0);
    raft_free(p);
    return MUNIT_OK;
}

TEST_CASE(default, realloc, NULL)
{
    void *p;
    (void)data;
    (void)params;
    p = raft_realloc(NULL, 8);
    munit_assert_ptr_not_null(p);
    *(uint64_t*)p = 1;
    p = raft_realloc(p, 16);
    munit_assert_ptr_not_null(p);
    munit_assert_int(*(uint64_t*)p, ==, 1);
    raft_free(p);
    return MUNIT_OK;
}

TEST_CASE(default, aligned_alloc, NULL)
{
    void *p;
    (void)data;
    (void)params;
    p = raft_aligned_alloc(1024, 2048);
    munit_assert_ptr_not_null(p);
    munit_assert_int((uintptr_t)p % 1024, ==, 0);
    raft_free(p);
    return MUNIT_OK;
}
