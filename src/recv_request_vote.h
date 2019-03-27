/**
 * RequestVote RPC handlers.
 */

#ifndef RAFT_RECV_REQUEST_VOTE_H_
#define RAFT_RECV_REQUEST_VOTE_H_

#include "../include/raft.h"

/**
 * Process a RequestVote RPC from the given server.
 */
int recv__request_vote(struct raft *r,
                       const unsigned id,
                       const char *address,
                       const struct raft_request_vote *args);

#endif /* RAFT_RECV_REQUEST_VOTE_H_ */
