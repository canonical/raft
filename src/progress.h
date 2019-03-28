/**
 * Track replication progress on followers.
 */

#ifndef RAFT_PROGRESS_H_
#define RAFT_PROGRESS_H_

#include "../include/raft.h"

/**
 * Possible values for the state field of struct raft_progress.
 */
enum {
    PROGRESS__PROBE = 0, /* At most one AppendEntries per heartbeat interval */
    PROGRESS__PIPELINE,  /* Optimistically stream AppendEntries */
    PROGRESS__SNAPSHOT   /* Sending a snapshot */
};

/**
 * Create and initialize the array of progress objects used by the leader to
 * track followers. The match index will be set to zero, and the current last
 * index plus 1.
 */
int progress__build_array(struct raft *r);

/**
 * Re-build the progress array against a new configuration.
 */
int progress__rebuild_array(struct raft *r,
                            const struct raft_configuration *configuration);

/**
 * Return true if a majority of voting servers have made_contact with us since
 * the last check, i.e. the recent_recv flag of the associated progress
 * object is true, and then reset the flag as well.
 */
bool progress__check_quorum(struct raft *r);

/**
 * Return the progress state code.
 */
int progress__state(struct raft *r, const struct raft_server *server);

/**
 * Return the progress next index.
 */
raft_index progress__next_index(struct raft *r,
                                const struct raft_server *server);

/**
 * Returns #false if the given @index comes from an outdated message. Otherwise
 * update the progress and returns #true. To be called when receiving a
 * successful AppendEntries RPC response.
 */
bool progress__maybe_update(struct raft *r,
                            const struct raft_server *server,
                            raft_index last_index);

/**
 * Return #false if the given @rejected index comes from an out of order
 * message. Otherwise decrease the progress next index to min(rejected,
 * last_index) and returns #true. To be called when receiving an unsuccessful
 * AppendEntries RPC response.
 */
bool progress__maybe_decrement(struct raft *r,
                               const struct raft_server *server,
                               raft_index rejected,
                               raft_index last_index);

/**
 * Set the recent send flag for the given server.
 */
void progress__mark_recent_send(struct raft *r,
                                const struct raft_server *server);

/**
 * Set the recent receive flag for the given server.
 */
void progress__mark_recent_recv(struct raft *r,
                                const struct raft_server *server);

/**
 * Convert to probe mode.
 */
void progress__to_probe(struct raft *r, const struct raft_server *server);

/**
 * Convert to snapshot mode.
 */
void progress__to_snapshot(struct raft *r,
                           const struct raft_server *server,
                           raft_index last_index);

/**
 * Abort snapshot mode because the sending has failed.
 */
void progress__abort_snapshot(struct raft *r, const struct raft_server *server);

/**
 * Return #true if match_index is equal or higher than the snapshot_index.
 */
bool progress__snapshot_done(struct raft *r, const struct raft_server *server);

#endif /* RAFT_PROGRESS_H_ */
