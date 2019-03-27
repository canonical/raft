/**
 * Send heartbeat messages.
 */

#ifndef RAFT_HEARTBEAT_H_
#define RAFT_HEARTBEAT_H_

#include "../include/raft.h"

void heartbeat__send(struct raft *r);

#endif /* RAFT_HEARTBEAT_H_ */

