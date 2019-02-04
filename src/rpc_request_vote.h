/**
 * RequestVote RPC handlers.
 */

#ifndef RAFT_RPC_REQUEST_VOTE_H
#define RAFT_RPC_REQUEST_VOTE_H

#include "../include/raft.h"

/**
 * Process a RequestVote RPC from the given server.
 */
int raft_rpc__recv_request_vote(struct raft *r,
                                const unsigned id,
                                const char *address,
                                const struct raft_request_vote *args);

/**
 * Process a RequestVote RPC result from the given server.
 */
int raft_rpc__recv_request_vote_result(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_request_vote_result *result);

#endif /* RAFT_RPC_REQUEST_VOTE_H */
