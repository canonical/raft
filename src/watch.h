/**
 * Helpers for firing callbacks registered with @raft_watch.
 */

#ifndef RAFT_WATCH_H
#define RAFT_WATCH_H

#include "../include/raft.h"

/**
 * Fire a #RAFT_EVENT_STATE_CHANGE.
 */
void raft_watch__state_change(struct raft *r, const unsigned short old_state);

/**
 * Fire a #RAFT_EVENT_COMMAND_APPLIED.
 */
void raft_watch__command_applied(struct raft *r, const raft_index index);

/**
 * Fire a #RAFT_EVENT_CONFIGURATION_APPLIED.
 */
void raft_watch__configuration_applied(struct raft *r);

/**
 * Fire a #RAFT_EVENT_PROMOTION_ABORTED.
 */
void raft_watch__promotion_aborted(struct raft *r, const unsigned id);

#endif /* RAFT_WATCH_H */
