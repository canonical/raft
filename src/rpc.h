/**
 * RPC-related helpers.
 */

#ifndef RAFT_RPC_H
#define RAFT_RPC_H

#include "../include/raft.h"

/**
 * Callback to be passed to the @raft_io implementation. It will be invoked upon
 * receiving an RPC message.
 */
void raft_rpc__recv_cb(void *data, struct raft_message *message);

/**
 * Common logic for RPC handlers, comparing the request's term with the server's
 * current term and possibly deciding to reject the request or step down from
 * candidate or leader.
 *
 * From Section 3.3:
 *
 *   If a candidate or leader discovers that its term is out of date, it
 *   immediately reverts to follower state. If a server receives a request with
 *   a stale term number, it rejects the request.
 *
 * The @match output parameter will be set to 0 if the local term matches the
 * request's term, to -1 if the request's term is lower, and to 1 if the
 * request's term was higher but we have successfully bumped the local one to
 * match it (and stepped down to follower in that case, if we were not already).
 */
int raft_rpc__ensure_matching_terms(struct raft *r, raft_term term, int *match);

#endif /* RAFT_RPC_H */
