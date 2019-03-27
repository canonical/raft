/**
 * Receive an RPC message.
 */

#ifndef RAFT_RECV_H_
#define RAFT_RECV_H_

#include "../include/raft.h"

/**
 * Callback to be passed to the @raft_io implementation. It will be invoked upon
 * receiving an RPC message.
 */
void recv_cb(struct raft_io *io, struct raft_message *message);

#endif /* RAFT_RECV_H_ */

