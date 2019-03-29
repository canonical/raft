/**
 * Log replication logic and helpers.
 */

#ifndef RAFT_REPLICATION_H
#define RAFT_REPLICATION_H

#include "../include/raft.h"

/**
 * Trigger an AppendEntries or an InstallSnapshot RPC to the server with the
 * given index.
 *
 * The RPC will contain all entries in our log from the server's next_index
 * onward, or a snapshot if we don't have anymore the needed log suffix.
 *
 * It must be called only by leaders.
 */
int replication__trigger(struct raft *r, unsigned i);

/**
 * Helper triggering I/O requests for newly appended log entries or heartbeat.
 *
 * This function will start writing to disk all entries in the log from the
 * given index onwards, and trigger AppendEntries RPCs requests to all follower
 * servers.
 *
 * If the index is 0, no entry are written to disk, and a heartbeat
 * AppendEntries RPC with no entries (or missing entries for followers whose log
 * is behind) is sent.
 *
 * It must be called only by leaders.
 */
int raft_replication__trigger(struct raft *r, const raft_index index);

/**
 * Update the replication state (match and next indexes) for the given server
 * using the given AppendEntries RPC result.
 *
 * Possibly send to the server a new set of entries or a snapshot if the result
 * was unsuccessful because of missing entries or if new entries were added to
 * our log in the meantime.
 *
 * It must be called only by leaders.
 */
int replication__update(struct raft *r,
                        const struct raft_server *server,
                        const struct raft_append_entries_result *result);

/**
 * Append the log entries in the given request if the Log Matching Property is
 * satisfied.
 *
 * The #success output parameter will be set to true if the Log Matching
 * Property was satisfied.
 *
 * The #async output parameter will be set to true if some of the entries in the
 * request were not present in our log, and a disk write was started to persist
 * them to disk. The entries will be appended to our log only once the disk
 * write completes and the I/O callback is invoked.
 *
 * It must be called only by followers.
 */
int raft_replication__append(struct raft *r,
                             const struct raft_append_entries *args,
                             raft_index *rejected,
                             bool *async);

int raft_replication__install_snapshot(struct raft *r,
                                       const struct raft_install_snapshot *args,
                                       raft_index *rejected,
                                       bool *async);

/**
 * Apply any committed entry that was not applied yet.
 *
 * It must be called by leaders or followers.
 */
int raft_replication__apply(struct raft *r);

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
void raft_replication__quorum(struct raft *r, const raft_index index);

#endif /* RAFT_REPLICATION_H */
