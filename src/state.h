/**
 * Handle raft state transitions.
 */

#ifndef RAFT_STATE_H
#define RAFT_STATE_H

#include "../include/raft.h"

/**
 * Possible values for the state field of struct raft_replication.
 */
enum {
    REPLICATION__PROBE = 0, /* At most one AppendEntries per heartbeat */
    REPLICATION__PIPELINE,  /* Optimistically stream AppendEntries */
    REPLICATION__SNAPSHOT   /* Sending a snapshot */
};

/**
 * Bump the current term to the given value and reset our vote, persiting the
 * change to disk.
 */
int raft_state__bump_current_term(struct raft *r, raft_term term);

/**
 * Re-build the next/match indexes against the given new configuration.
 *
 * It must be called only by leaders.
 */
int raft_state__rebuild_next_and_match_indexes(
    struct raft *r,
    const struct raft_configuration *configuration);

#endif /* RAFT_STATE_H */
