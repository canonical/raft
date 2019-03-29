/**
 * Receive an AppendEntries message.
 */

#ifndef RAFT_RECV_APPEND_ENTRIES_H_
#define RAFT_RECV_APPEND_ENTRIES_H_

#include "../include/raft.h"

/**
 * Process an AppendEntries RPC from the given server.
 */
int recv__append_entries(struct raft *r,
                         const unsigned id,
                         const char *address,
                         const struct raft_append_entries *args);

/**
 * Process an AppendEntries RPC result from the given server.
 */
int recv__append_entries_result(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_append_entries_result *result);

#endif /* RAFT_RECV_APPEND_ENTRIES_H_ */
