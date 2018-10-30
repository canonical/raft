/**
 *
 * Log replication logic and helpers.
 *
 */

#ifndef RAFT_REPLICATION_H
#define RAFT_REPLICATION_H

#include "../include/raft.h"

/**
 * Send an AppendEntries RPC to the server with the given index in the
 * configuration.
 *
 * The RPC will contain all entries in our log from next_index[<server>] onward.
 */
int raft_replication__send_append_entries(struct raft *r, size_t i);

/**
 * Send an AppendEntries RPC to all other servers
 *
 * If a remote server next_index is has up-to-date as ours, the RPC will carry
 * no entries.
 */
void raft_replication__send_heartbeat(struct raft *r);

/**
 * Append the log entries in the given request if the Log Matching Property is
 * satisfied.
 */
int raft_replication__maybe_append(struct raft *r,
                                   const struct raft_append_entries_args *args,
                                   bool *success,
                                   bool *async);

/**
 * Update the match and next indexes for the server in the given AppendEntries
 * RPC result and possibly send it a new set of entries to replicate.
 */
void raft_replication__update_server(
    struct raft *r,
    size_t server_index,
    const struct raft_append_entries_result *result);

/**
 * Check if a quorum has been reached for the given log index, and update commit
 * index accordingly if so.
 *
 * From Figure 3.1:
 *
 *   [Rules for servers] Leaders:
 *
 *   If there exists an N such that N > commitIndex, a majority of
 *   matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
 */
void raft_replication__maybe_commit(struct raft *r, raft_index index);

#endif /* RAFT_REPLICATION_H */
