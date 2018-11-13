#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "error.h"
#include "log.h"
#include "logger.h"
#include "membership.h"
#include "queue.h"
#include "replication.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/**
 * An I/O request on the leader (such as sending an append entries request or
 * writing to the log) has been completed.
 */
static int raft_handle_io__leader(struct raft *r,
                                  struct raft_io_request *request,
                                  int status)
{
    assert(request->type == RAFT_IO_WRITE_LOG ||
           request->type == RAFT_IO_APPEND_ENTRIES);

    raft__debugf(r, "I/O completed on leader: status %d", status);

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&r->log, request->index, request->entries, request->n);

    /* If we are not leader anymore, just discard the result. */
    if (r->state != RAFT_STATE_LEADER) {
        raft__debugf(r, "local server is not leader -> ignore I/O result");
        return 0;
    }

    /* TODO: in case this is a failed disk write and we were the leader creating
     * these entries in the first place, should we truncate our log too? since
     * we have appended these entries to it. */
    if (status != 0) {
        return 0;
    }

    /* If this was a disk write, check if we have reached a quorum.
     *
     * TODO2: think about the fact that last_index might be not be matching
     * this request (e.g. other entries were accpeted and pre-emptively
     * appended to the log) so essentialy we need the same min() logic as
     * above or something similar */
    if (request->type == RAFT_IO_WRITE_LOG) {
        raft_index index;
        size_t server_index;
        int rv;

        index = raft_log__last_index(&r->log);
        server_index = raft_configuration__index(&r->configuration, r->id);

        /* Only update the next index if we are part of the current
         * configuration. The only case where this is not true is when we were
         * asked to remove ourselves from the cluster.
         *
         * From Section 4.2.2:
         *
         *   there will be a period of time (while it is committing Cnew) when a
         *   leader can manage a cluster that does not include itself; it
         *   replicates log entries but does not count itself in majorities.
         */
        if (server_index < r->configuration.n) {
            r->leader_state.match_index[server_index] = index;
        } else {
            const struct raft_entry *entry = raft_log__get(&r->log, index);
            assert(entry->type == RAFT_LOG_CONFIGURATION);
        }

        /* Check if we can commit some new entries. */
        raft_replication__quorum(r, index);

        rv = raft_replication__apply(r);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

/**
 * An I/O request on the follower (such as writing to the log new entries
 * received via AppendEntries RPC) has been completed.
 */
static int raft_handle_io__follower(struct raft *r,
                                    struct raft_io_request *request,
                                    int status)
{
    struct raft_append_entries_result result;
    const struct raft_server *leader;
    const char *leader_address;
    size_t i;
    int rv;

    /* Disk writes are the only I/O that followers perform. */
    assert(request->type == RAFT_IO_WRITE_LOG);

    raft__debugf(r, "I/O completed on follower: status %d", status);

    /* If we are not followers anymore, just discard the result. */
    if (r->state != RAFT_STATE_FOLLOWER) {
        raft__debugf(r, "local server is not follower -> ignore I/O result");
        return 0;
    }

    if (status != 0) {
        result.success = false;
        goto out;
    }

    /* Try getting the leader address from the current configuration. If no
     * server with a matching ID exists, it probably means that this is the very
     * first entry being appended and the configuration is empty. We'll retry
     * later, after we have applied configuraiton entry. Similarly, it might be
     * that we're going to apply a new configuration where the leader is
     * removing itself, so we need to save its address here. */
    leader = raft_configuration__get(&r->configuration, request->leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    result.term = r->current_term;

    /* Update the log and commit index to match the one from the leader.
     *
     * TODO: handle the case where we're not followers anymore? */
    for (i = 0; i < request->n; i++) {
        struct raft_entry *entry = &request->entries[i];
        raft_index index;
        rv = raft_log__append(&r->log, entry->term, entry->type, &entry->buf,
                              entry->batch);
        if (rv != 0) {
            /* TODO: what should we do? */
            // raft_queue__pop(r, request->id);
            return rv;
        }

        index = raft_log__last_index(&r->log);

        /* If this is a configuration change entry, check the change is about
         * promoting a non-voting server to voting, and in that case update
         * our configuration cache. */
        if (entry->type == RAFT_LOG_CONFIGURATION) {
            rv = raft_membership__apply(r, index, entry);
            if (rv != 0) {
                return rv;
            }
        }
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (request->leader_commit > r->commit_index) {
        raft_index last_index = raft_log__last_index(&r->log);
        r->commit_index = min(request->leader_commit, last_index);
        rv = raft_replication__apply(r);
        if (rv != 0) {
            return rv;
        }
    }

    result.success = true;

out:
    if (request->entries != NULL) {
        raft_free(request->entries);
    }

    /* Refresh the leader address, in case it has changed or it was added in a
     * new configuration. */
    leader = raft_configuration__get(&r->configuration, request->leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    /* TODO: are there cases were this assertion is unsafe? */
    assert(leader_address != NULL);

    result.last_log_index = raft_log__last_index(&r->log);
    rv = r->io->send_append_entries_response(r->io, request->leader_id,
                                             leader_address, &result);
    if (rv != 0) {
        raft_error__printf(r, rv, "send append entries response");
        return rv;
    }

    return 0;
}

int raft_handle_io(struct raft *r, const unsigned request_id, const int status)
{
    struct raft_io_request *request;
    int rv;

    assert(r != NULL);

    request = raft_queue__get(r, request_id);

    if (request->leader_id == r->id) {
        /* This I/O request was pushed at a time this server was a leader,
         * either to write entries to its own on-disk log or to replicate them
         * to a follower. */
        rv = raft_handle_io__leader(r, request, status);
    } else {
        /* This I/O request was pushed at a time this server was a follower, to
         * replicate entries to its own log. */
        rv = raft_handle_io__follower(r, request, status);
    }

    raft_queue__pop(r, request_id);

    if (rv != 0) {
        /* TODO: should we propagate the error? */
        raft_error__wrapf(r, "handle completed I/O request");
        return rv;
    }

    return 0;
}
