#ifndef RAFT_SNAPSHOT_H_
#define RAFT_SNAPSHOT_H_

#include "../include/raft.h"

void raft_snapshot__close(struct raft_snapshot *s);

#endif /* RAFT_SNAPSHOT_H */
