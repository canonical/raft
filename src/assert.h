/**
 * Define the assert() macro, either as the standard one or the test one.
 */

#ifndef RAFT_ASSERT_H
#define RAFT_ASSERT_H

#if defined(RAFT_TEST)
  #include "../test/lib/munit.h"
  #define assert(expr) munit_assert(expr)
#else
  #include <assert.h>
#endif

#endif /* RAFT_ASSERT_H */
