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
int raft__tick(struct raft *r, const unsigned msec_since_last_tick);

/**
 * Callback to be passed to the @raft_io implementation and that will be invoked
 * periodically. This is just a trampoline for calling @raft__tick.
 */
void raft__tick_cb(void *data, unsigned msecs);

#endif /* RAFT_TICK_H */
