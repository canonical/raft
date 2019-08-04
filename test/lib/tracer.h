/* Raft tracer helpers. */

#ifndef TEST_TRACER_H_
#define TEST_TRACER_H_

#include "../../include/raft.h"

#define FIXTURE_TRACER struct raft_tracer tracer;
#define SETUP_TRACER                              \
    {                                             \
        int rv_;                                  \
        rv_ = raft_tracer_init(&f->tracer, 2048); \
        munit_assert_int(rv_, ==, 0);             \
    }
#define TEAR_DOWN_TRACER                                         \
    {                                                            \
        raft_tracer_walk(&f->tracer, test_tracer_walk_cb, NULL); \
        raft_tracer_close(&f->tracer);                           \
    }

void test_tracer_walk_cb(void *data,
                         raft_time time,
                         unsigned type,
                         const char *message);

#endif /* TEST_TRACER_H_ */
