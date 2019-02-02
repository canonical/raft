/**
 * I/O callbacks.
 */

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "../include/raft.h"

/**
 * Process the result of an asynchronous I/O request that involves raft entries
 * or snapshots (i.e. memory shared between a raft instance and its I/O
 * implementation).
 *
 * The @request parameter holds information about the entries or snapshosts
 * referenced in request that has been completed. The @status parameter must be
 * set to zero if the write was successful, or non-zero otherwise.
 */

/**
 * Callback to be passed to the @raft_io implementation. It will be invoked upon
 * receiving an RPC message.
 */
void raft__recv_cb(void *data, struct raft_message *message);

/**
 * Persist all entries that have been added to the in-memory log, from the given
 * index onwards. This must be called by leaders when replicating new entries.
 */
int raft_io__leader_append(struct raft *r, unsigned index);

/**
 * Persist all entries that have been sent to as by a leader using the
 * AppendEntries RPC.
 */
int raft_io__follower_append(struct raft *r,
                             struct raft_entry *entries,
                             size_t n,
                             unsigned leader_id,
                             raft_index leader_commit);

/**
 * Send an AppendEntries RPC to the given server.
 */
int raft_io__send_append_entries(struct raft *r,
                                 const struct raft_server *server,
                                 const struct raft_append_entries *args);

#endif /* RAFT_IO_H */
