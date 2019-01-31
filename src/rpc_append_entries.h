/**
 * AppendEntries logic.
 */

#ifndef RAFT_RPC_H
#define RAFT_RPC_H

#include "../include/raft.h"

int raft_handle_append_entries(struct raft *r,
                               const unsigned id,
                               const char *address,
                               const struct raft_append_entries *args);

#endif /* RAFT_RPC_H */
