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
 * Release any resources associated with the current state.
 */
void raft_state__clear(struct raft *r);

/**
 * Bump the current term to the given value and reset our vote, persiting the
 * change to disk.
 */
int raft_state__bump_current_term(struct raft *r, raft_term term);

/**
 * Set the initial state to follower.
 */
void raft_state__start_as_follower(struct raft *r);

/**
 * Convert from candidate or leader to follower.
 *
 * From Figure 3.1:
 *
 *   If election timeout elapses without receiving AppendEntries RPC from
 *   current leader or granting vote to candidate: convert to candidate.
 *
 * The above implies that we need to reset the election timer when converting to
 * follower.
 */
int raft_state__convert_to_follower(struct raft *r, raft_term term);

/**
 * Convert from follower to candidate, starting a new election.
 *
 * From Figure 3.1:
 *
 *   On conversion to candidate, start election:
 */
int raft_state__convert_to_candidate(struct raft *r);

/**
 * Convert from candidate to leader.
 *
 * From Figure 3.1:
 *
 *   Upon election: send initial empty AppendEntries RPC (heartbeat) to each
 *   server.
 *
 * From Section §3.4:
 *
 *   Once a candidate wins an election, it becomes leader. It then sends
 *   heartbeat messages to all of the other servers to establish its authority
 *   and prevent new elections.
 *
 * From Section §3.3:
 *
 *   The leader maintains a nextIndex for each follower, which is the index
 *   of the next log entry the leader will send to that follower. When a
 *   leader first comes to power, it initializes all nextIndex values to the
 *   index just after the last one in its log.
 */
int raft_state__convert_to_leader(struct raft *r);

/**
 * Re-build the next/match indexes against the given new configuration.
 *
 * It must be called only by leaders.
 */
int raft_state__rebuild_next_and_match_indexes(
    struct raft *r,
    const struct raft_configuration *configuration);

#endif /* RAFT_STATE_H */
