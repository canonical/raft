/**
 * Track replication progress on followers.
 */

#ifndef RAFT_PROGRESS_H_
#define RAFT_PROGRESS_H_

#include "../include/raft.h"

/**
 * Create and initialize an array of progress objects used by the leader to
 * track followers. The match index will be set to zero, and the last index to
 * @last_index + 1.
 */
struct raft_progress *progress__create_array(unsigned n_servers,
                                             raft_index last_index);

/**
 * Re-build the given array or project objects against a new configuration.
 */
struct raft_progress *progress__update_array(
    struct raft_progress *cur_p,
    raft_index last_index,
    const struct raft_configuration *cur_configuration,
    const struct raft_configuration *new_configuration);

#endif /* RAFT_PROGRESS_H_ */
