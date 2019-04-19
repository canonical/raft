/**
 * In-memory raft log helpers.
 */

#ifndef TEST_LOG_H
#define TEST_LOG_H

#include "../../src/log.h"

#define FIXTURE_LOG struct raft_log log;
#define SETUP_LOG logInit(&f->log)
#define TEAR_DOWN_LOG logClose(&f->log)

#endif /* TEST_LOG_H */
