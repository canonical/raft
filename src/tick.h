/**
 * Logic to be invoked periodically.
 */

#ifndef RAFT_TICK_H
#define RAFT_TICK_H

#include "../include/raft.h"

/**
 * Notify the raft instance that a certain amout of time as elapsed.
 *
 * This function needs to be called periodically, in order to process events
 * that are dependent on time passing.
 */
int tick(struct raft *r);

#endif /* RAFT_TICK_H */
