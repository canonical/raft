/**
 * Add support for using the libuv loop in tests.
 */

#ifndef TEST_UV_H
#define TEST_UV_H

#include <uv.h>

#include "munit.h"

/* Max n. of loop iterations ran by a single function call */
#define TEST_UV_MAX_LOOP_RUN 10

#define FIXTURE_UV struct uv_loop_s loop
#define SETUP_UV test_uv_setup(params, &f->loop)
#define TEAR_DOWN_UV                         \
    {                                        \
        int alive = uv_loop_alive(&f->loop); \
        if (alive) {                         \
            test_uv_stop(&f->loop);          \
        }                                    \
        test_uv_tear_down(&f->loop);         \
    }

void test_uv_setup(const MunitParameter params[], struct uv_loop_s *l);

void test_uv_tear_down(struct uv_loop_s *l);

/**
 * Run the loop until there are no pending active handles or the given amount of
 * iterations is reached.
 *
 * Return non-zero if there are pending handles.
 */
int test_uv_run(struct uv_loop_s *l, unsigned n);

/**
 * Run the loop until the given function returns true.
 *
 * If the loop exhausts all active handles or if #TEST_UV_MAX_LOOP_RUN is
 * reached without @F returning #true, the test fails.
 */
#define test_uv_run_until(LOOP, DATA, F)                                    \
    {                                                                       \
        unsigned i;                                                         \
        int rv2;                                                            \
        for (i = 0; i < TEST_UV_MAX_LOOP_RUN; i++) {                        \
            if (F(DATA)) {                                                  \
                break;                                                      \
            }                                                               \
            rv2 = uv_run(LOOP, UV_RUN_ONCE);                                \
            if (rv2 < 0) {                                                  \
                munit_errorf("uv_run: %s (%d)", uv_strerror(rv2), rv2);     \
            }                                                               \
            if (rv2 == 0) {                                                 \
                if (F(DATA)) {                                              \
                    break;                                                  \
                }                                                           \
                munit_errorf("uv_run: stopped after %u iterations", i + 1); \
            }                                                               \
        }                                                                   \
        if (i == TEST_UV_MAX_LOOP_RUN) {                                    \
            munit_errorf("uv_run: condition not met in %d iterations",      \
                         TEST_UV_MAX_LOOP_RUN);                             \
        }                                                                   \
    }

/**
 * Run the loop until there are no pending active handles.
 *
 * If there are still pending active handles after 10 loop iterations, the test
 * will fail.
 *
 * This is meant to be used in tear down functions.
 */
void test_uv_stop(struct uv_loop_s *l);

#endif /* TEST_UV_H */
