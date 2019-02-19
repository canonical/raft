/**
 * Add support for using the libuv loop in tests.
 */

#ifndef TEST_UV_H
#define TEST_UV_H

#include <uv.h>

#include "munit.h"

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
 * Run the loop until there are no pending active handles.
 *
 * If there are still pending active handles after 10 loop iterations, the test
 * will fail.
 *
 * This is meant to be used in tear down functions.
 */
void test_uv_stop(struct uv_loop_s *l);

#endif /* TEST_UV_H */
