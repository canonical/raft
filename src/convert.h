/**
 * Convert from one state to another.
 */

#ifndef RAFT_CONVERT_H_
#define RAFT_CONVERT_H_

#include "../include/raft.h"

/**
 * Convert from unavailable, or candidate or leader to follower.
 *
 * From Figure 3.1:
 *
 *   If election timeout elapses without receiving AppendEntries RPC from
 *   current leader or granting vote to candidate: convert to candidate.
 *
 * The above implies that we need to reset the election timer when converting to
 * follower.
 */
void convert__to_follower(struct raft *r);

/**
 * Convert from follower to candidate, starting a new election.
 *
 * From Figure 3.1:
 *
 *   On conversion to candidate, start election:
 */
int convert__to_candidate(struct raft *r);

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
int convert__to_leader(struct raft *r);

void convert__to_unavailable(struct raft *r);

#endif /* RAFT_CONVERT_H_ */
