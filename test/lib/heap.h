/* Add support for fault injection and leak detection to stdlib's malloc()
 * family. */

#ifndef TEST_HEAP_H_
#define TEST_HEAP_H_

#include "../../include/raft.h"

#include "munit.h"

/* Munit parameter defining after how many API calls the test raft_heap
 * implementation should start failing and return errors. The default is -1,
 * meaning that no failure will ever occur. */
#define TEST_HEAP_FAULT_DELAY "heap-fault-delay"

/* Munit parameter defining how many consecutive times API calls against the
 * test raft_heap implementation should keep failing after they started
 * failing. This parameter has an effect only if 'store-fail-delay' is 0 or
 * greater. The default is 1, and -1 means "keep failing forever". */
#define TEST_HEAP_FAULT_REPEAT "heap-fault-repeat"

/* Macro helpers. */
#define FIXTURE_HEAP struct raft_heap heap
#define SETUP_HEAP test_heap_setup(params, &f->heap)
#define TEAR_DOWN_HEAP test_heap_tear_down(&f->heap)
#define HEAP_FAULT_ENABLE test_heap_fault_enable(&f->heap)

void test_heap_setup(const MunitParameter params[], struct raft_heap *h);
void test_heap_tear_down(struct raft_heap *h);

void test_heap_fault_config(struct raft_heap *h, int delay, int repeat);
void test_heap_fault_enable(struct raft_heap *h);

#endif /* TEST_HEAP_H_ */

