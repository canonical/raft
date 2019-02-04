/**
 * AppendEntries logic.
 */

#ifndef RAFT_RPC_APPEND_ENTRIES_H
#define RAFT_RPC_APPEND_ENTRIES_H

#include "../include/raft.h"

/**
 * Process an AppendEntries RPC from the given server.
 */
int raft_rpc__recv_append_entries(struct raft *r,
                                  const unsigned id,
                                  const char *address,
                                  const struct raft_append_entries *args);

/**
 * Process an AppendEntries RPC result from the given server.
 */
int raft_rpc__recv_append_entries_result(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_append_entries_result *result);

#endif /* RAFT_RPC_APPEND_ENTRIES_H */
