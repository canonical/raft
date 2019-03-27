/**
 * Track replication progress on followers.
 */

#ifndef RAFT_PROGRESS_H_
#define RAFT_PROGRESS_H_

#include "../include/raft.h"

/**
 * Create and initialize the array of progress objects used by the leader to
 * track followers. The match index will be set to zero, and the current last
 * index plus 1.
 */
int progress__create_array(struct raft *r);

/**
 * Re-build the progress array against a new configuration.
 */
int progress__update_array(struct raft *r,
                           const struct raft_configuration *configuration);

/**
 * Return true if the last_contact field of the objects in the progress array
 * indicates that the leader has been contacted by a majority of voting servers
 * in the last election_timeout milliseconds.
 */
bool progress__has_still_quorum(struct raft *r);

#endif /* RAFT_PROGRESS_H_ */
