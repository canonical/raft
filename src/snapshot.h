#ifndef RAFT_SNAPSHOT_H_
#define RAFT_SNAPSHOT_H_

#include "../include/raft.h"

void raft_snapshot__close(struct raft_snapshot *s);

/**
 * Restore a snapshot. This will reset the current state of the server as if the
 * last entry contained in the snapshot had just been persisted, committed and
 * applied. The in-memory log must be empty when calling this function.
 */
int snapshot__restore(struct raft *r, struct raft_snapshot *snapshot);

#endif /* RAFT_SNAPSHOT_H */
