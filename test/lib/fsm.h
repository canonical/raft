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
void test_fsm_encode_set_x(int value, struct raft_buffer *buf);

/**
 * Encode a command to set y to the given value.
 */
void test_fsm_encode_set_y(int value, struct raft_buffer *buf);

int test_fsm_get_x(struct raft_fsm *fsm);
int test_fsm_get_y(struct raft_fsm *fsm);


#endif /* TEST_FSM_H */
