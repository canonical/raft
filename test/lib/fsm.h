/**
 * Test implementation of the raft_fsm interface, with fault injection.
 *
 * The test FSM supports only two commands: setting x and setting y.
 */

#ifndef TEST_FSM_H
#define TEST_FSM_H

#include "../../include/raft.h"

#include "munit.h"

void test_fsm_setup(const MunitParameter params[], struct raft_fsm *fsm);

void test_fsm_tear_down(struct raft_fsm *fsm);

/**
 * Encode a command to set x to the given value.
 */
void test_fsm_encode_set_x(const int value, struct raft_buffer *buf);

/**
 * Encode a command to set y to the given value.
 */
void test_fsm_encode_set_y(const int value, struct raft_buffer *buf);

#endif /* TEST_FSM_H */
