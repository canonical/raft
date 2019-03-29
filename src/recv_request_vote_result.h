/**
 * Receive a RequestVote result.
 */

#ifndef RAFT_RECV_REQUEST_VOTE_RESULT_H_
#define RAFT_RECV_REQUEST_VOTE_RESULT_H_

#include "../include/raft.h"

/**
 * Process a RequestVote RPC result from the given server.
 */
int recv__request_vote_result(struct raft *r,
                              unsigned id,
                              const char *address,
                              const struct raft_request_vote_result *result);

#endif /* RAFT_RECV_REQUEST_VOTE_RESULT_H_ */
