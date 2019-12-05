#ifndef TEST_TRACER_H_
#define TEST_TRACER_H_

#include "../../include/raft.h"

#define FIXTURE_TRACER struct raft_tracer tracer
#define SETUP_TRACER                       \
    {                                      \
        f->tracer.emit = test_tracer_emit; \
    }
#define TEAR_DOWN_TRACER

void test_tracer_emit(struct raft_tracer *t,
                      const char *file,
                      int line,
                      const char *message);

#endif /* TEST_TRACER_H_ */
