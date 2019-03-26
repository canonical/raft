/**
 * Track replication progress on followers.
 */

#ifndef RAFT_PROGRESS_H_
#define RAFT_PROGRESS_H_

#include "../include/raft.h"

/**
 * Initialize the given progress tracker. The match index will be set to zero,
 * and the last index to @last_index + 1.
 */
void progress__init(struct raft_progress *p, raft_index last_index);

#endif /* RAFT_PROGRESS_H_ */
