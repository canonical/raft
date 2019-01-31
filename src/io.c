#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "error.h"
#include "io.h"
#include "log.h"
#include "membership.h"
#include "replication.h"
#include "rpc_append_entries.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

int raft_handle_io(struct raft *r, const unsigned request_id, const int status)
{
    // request = raft_queue__get(r, request_id);

    // if (request->leader_id == r->id) {
    /* This I/O request was pushed at a time this server was a leader,
     * either to write entries to its own on-disk log or to replicate them
     * to a follower. */
    //    rv = raft_handle_io__leader(r, request, status);
    //} else {
    /* This I/O request was pushed at a time this server was a follower, to
     * replicate entries to its own log. */
    //    rv = raft_handle_io__follower(r, request, status);
    //}

    // raft_queue__pop(r, request_id);

    // if (rv != 0) {
    /* TODO: should we propagate the error? */
    //  raft_error__wrapf(r, "handle completed I/O request");
    //    return rv;
    //}

    return 0;
}

void raft_io__send_cb(void *data, int status)
{
    struct raft_io__send_append_entries *ctx = data;

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&ctx->raft->log, ctx->index, ctx->entries, ctx->n);

    raft_free(ctx);
}

static int raft_io__append_leader(struct raft *r,
                                  raft_index index,
                                  struct raft_entry *entries,
                                  unsigned n_entries,
                                  int status)
{
    raft_index last_index;
    size_t server_index;
    int rv;

    raft_debugf(r->logger, "write log completed on leader: status %d", status);

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&r->log, index, entries, n_entries);

    /* If we are not leader anymore, just discard the result. */
    if (r->state != RAFT_STATE_LEADER) {
        raft_debugf(r->logger,
                    "local server is not leader -> ignore write log result");
        return 0;
    }

    /* TODO: in case this is a failed disk write and we were the leader creating
     * these entries in the first place, should we truncate our log too? since
     * we have appended these entries to it. */
    if (status != 0) {
        return 0;
    }

    /* If Check if we have reached a quorum.
     *
     * TODO2: think about the fact that last_index might be not be matching
     * this request (e.g. other entries were accpeted and pre-emptively
     * appended to the log) so essentialy we need the same min() logic as
     * above or something similar */
    last_index = raft_log__last_index(&r->log);
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
        r->leader_state.match_index[server_index] = last_index;
    } else {
        const struct raft_entry *entry = raft_log__get(&r->log, last_index);
        assert(entry->type == RAFT_LOG_CONFIGURATION);
    }

    /* Check if we can commit some new entries. */
    raft_replication__quorum(r, last_index);

    rv = raft_replication__apply(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft_io__append_follower(struct raft *r,
                                    struct raft_entry *entries,
                                    unsigned n_entries,
                                    unsigned leader_id,
                                    raft_index leader_commit,
                                    int status)
{
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    const struct raft_server *leader;
    const char *leader_address;
    size_t i;
    int rv;

    raft_debugf(r->logger, "I/O completed on follower: status %d", status);

    /* If we are not followers anymore, just discard the result. */
    if (r->state != RAFT_STATE_FOLLOWER) {
        raft_debugf(r->logger,
                    "local server is not follower -> ignore I/O result");
        return 0;
    }

    if (status != 0) {
        result->success = false;
        goto out;
    }

    /* Try getting the leader address from the current configuration. If no
     * server with a matching ID exists, it probably means that this is the very
     * first entry being appended and the configuration is empty. We'll retry
     * later, after we have applied the configuraiton entry. Similarly, it might
     * be that we're going to apply a new configuration where the leader is
     * removing itself, so we need to save its address here. */
    leader = raft_configuration__get(&r->configuration, leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    result->term = r->current_term;

    /* Update the log and commit index to match the one from the leader.
     *
     * TODO: handle the case where we're not followers anymore? */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];
        raft_index index;
        rv = raft_log__append(&r->log, entry->term, entry->type, &entry->buf,
                              entry->batch);
        if (rv != 0) {
            /* TODO: what should we do? */
            // raft_queue__pop(r, request->id);
            return rv;
        }

        index = raft_log__last_index(&r->log);

        /* If this is a configuration change entry, check that the change is
         * about promoting a non-voting server to voting, and in that case
         * update our configuration cache. */
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
    if (leader_commit > r->commit_index) {
        raft_index last_index = raft_log__last_index(&r->log);
        r->commit_index = min(leader_commit, last_index);
        rv = raft_replication__apply(r);
        if (rv != 0) {
            return rv;
        }
    }

    result->success = true;

out:
    if (entries != NULL) {
        raft_free(entries);
    }

    /* Refresh the leader address, in case it has changed or it was added in a
     * new configuration. */
    leader = raft_configuration__get(&r->configuration, leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    /* TODO: are there cases were this assertion is unsafe? */
    assert(leader_address != NULL);

    result->last_log_index = raft_log__last_index(&r->log);

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = leader_id;
    message.server_address = leader_address;

    rv = r->io->send(r->io, &message, NULL, NULL);
    if (rv != 0) {
        raft_error__printf(r, rv, "send append entries response");
        return rv;
    }

    return 0;
}

void raft_io__append_cb(void *data, int status)
{
    struct raft_io__write_log *request = data;
    int rv;

    if (request->leader_id == request->raft->id) {
        rv = raft_io__append_leader(request->raft, request->index,
                                    request->entries, request->n, status);
    } else {
        rv = raft_io__append_follower(request->raft, request->entries,
                                      request->n, request->leader_id,
                                      request->leader_commit, status);
    }

    if (rv != 0) {
        /* TODO: simply log the error? */
    }

    raft_free(request);
}

void raft_io__recv(struct raft *r, struct raft_message *message)
{
    int rv;

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = raft_handle_append_entries(r, message->server_id,
                                            message->server_address,
                                            &message->append_entries);
            break;
        default:
            rv = RAFT_ERR_IO;
            break;
    };

    if (rv != 0) {
        /* TODO: log a warning */
    }
}

void raft__recv_cb(void *data, struct raft_message *message)
{
    struct raft *r;

    r = data;

    raft_io__recv(r, message);
}
