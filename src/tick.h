/**
 * Logic to be invoked periodically.
 */

#ifndef RAFT_TICK_H
#define RAFT_TICK_H

#include "../include/raft.h"

/**
 * Callback to be passed to the @raft_io implementation. It notifies us that a
 * certain amount of time has elapsed and will be invoked periodically.
 */
void tick_cb(struct raft_io *io);

#endif /* RAFT_TICK_H */
