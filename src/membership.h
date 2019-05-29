/**
 * Membership-related APIs.
 */

#ifndef RAFT_MEMBERSHIP_H
#define RAFT_MEMBERSHIP_H

#include "../include/raft.h"

/**
 * Helper returning an error if the configuration can't be changed, either
 * because this node is not the leader or because a configuration change is
 * already in progress.
 */
int raft_membership__can_change_configuration(struct raft *r);

/**
 * Update the information about the progress that the non-voting server
 * currently being promoted is making in catching with logs.
 *
 * Return false if the server being promoted did not yet catch-up with logs, and
 * true if it did.
 *
 * This function must be called only by leaders after a @raft_promote request
 * has been submitted.
 */
bool raft_membership__update_catch_up_round(struct raft *r);

/**
 * Update the local configuration replacing it with the content of the given
 * RAFT_CHANGE entry, which has just been received in as part of an
 * AppendEntries RPC request. The uncommitted configuration index will be
 * updated accordingly.
 *
 * It must be called only by followers.
 */
int raft_membership__apply(struct raft *r,
                           const raft_index index,
                           const struct raft_entry *entry);

/* Rollback any promotion configuration change that was applied locally, but
 * failed to be committed. It must be called by followers after they receive an
 * AppendEntries RPC request that instructs them to evict the uncomitted entry
 * from their log. */
int membershipRollback(struct raft *r);

#endif /* RAFT_MEMBERSHIP_H */
