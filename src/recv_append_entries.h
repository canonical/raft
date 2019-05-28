/* Receive an AppendEntries message. */

#ifndef RECV_APPEND_ENTRIES_H_
#define RECV_APPEND_ENTRIES_H_

#include "../include/raft.h"

/* Process an AppendEntries RPC from the given server. */
int recvAppendEntries(struct raft *r,
                      const unsigned id,
                      const char *address,
                      const struct raft_append_entries *args);

/* Process an AppendEntries RPC result from the given server. */
int recv__append_entries_result(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_append_entries_result *result);

#endif /* RECV_APPEND_ENTRIES_H_ */
