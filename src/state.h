/**
 * Handle raft state transitions.
 */

#ifndef RAFT_STATE_H
#define RAFT_STATE_H

#include "../include/raft.h"

/**
 * Possible values for the state field of struct raft_progress.
 */
enum {
    REPLICATION__PROBE = 0, /* At most one AppendEntries per heartbeat */
    REPLICATION__PIPELINE,  /* Optimistically stream AppendEntries */
    REPLICATION__SNAPSHOT   /* Sending a snapshot */
};

#endif /* RAFT_STATE_H */
