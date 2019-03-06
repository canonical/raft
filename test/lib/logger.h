/**
 * Test logger.
 */

#ifndef TEST_LOGGER_H
#define TEST_LOGGER_H

#include "../../include/raft.h"

#include "munit.h"

void test_logger_setup(const MunitParameter params[],
                       struct raft_logger *l,
                       uint64_t id);

void test_logger_tear_down(struct raft_logger *l);

/**
 * Set a time source for the logger.
 */
void test_logger_time(struct raft_logger *l,
                      void *data,
                      int (*time)(void *data));

#endif /* TEST_LOGGER_H */
