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

/**
 * Hold context for an append request that was submitted by a leader.
 */
struct raft_io__leader_append
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
};

/**
 * Hold context for an append request that was submitted by a follower.
 */
struct raft_io__follower_append
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    unsigned leader_id;         /* Leader issuing the request */
    raft_index leader_commit;   /* Commit index on the leader */
};

/**
 * Hold context for a #RAFT_IO_APPEND_ENTRIES send request that was submitted.
 */
struct raft_io__send_append_entries
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
};

void raft__recv_cb(void *data, struct raft_message *message)
{
    struct raft *r;
    int rv;

    r = data;

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

static void raft_io__leader_append_cb(void *data, int status)
{
    struct raft_io__leader_append *request = data;
    struct raft *r = request->raft;
    raft_index last_index;
    size_t server_index;
    int rv;

    raft_debugf(r->logger, "write log completed on leader: status %d", status);

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&r->log, request->index, request->entries, request->n);

    raft_free(request);

    /* If we are not leader anymore, just discard the result. */
    if (r->state != RAFT_STATE_LEADER) {
        raft_debugf(r->logger,
                    "local server is not leader -> ignore write log result");
        return;
    }

    /* TODO: in case this is a failed disk write and we were the leader creating
     * these entries in the first place, should we truncate our log too? since
     * we have appended these entries to it. */
    if (status != 0) {
        return;
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
        /* TODO: just log the error? */
    }
}

int raft_io__leader_append(struct raft *r, unsigned index)
{
    struct raft_entry *entries;
    unsigned n;
    struct raft_io__leader_append *request;
    int rv;

    assert(r->state == RAFT_STATE_LEADER);

    if (index == 0) {
        return 0;
    }

    /* Acquire all the entries from the given index onwards. */
    rv = raft_log__acquire(&r->log, index, &entries, &n);
    if (rv != 0) {
        goto err;
    }

    /* We expect this function to be called only when there are actually
     * some entries to write. */
    assert(n > 0);

    /* Allocate a new request. */
    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_entries_acquired;
    }

    request->raft = r;
    request->index = index;
    request->entries = entries;
    request->n = n;

    rv = r->io->append(r->io, entries, n, request, raft_io__leader_append_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);

err_after_entries_acquired:
    raft_log__release(&r->log, index, entries, n);

err:
    assert(rv != 0);
    return rv;
}

static void raft_io__follower_append_cb(void *data, int status)
{
    struct raft_io__follower_append *request = data;
    struct raft *r = request->raft;
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
        goto out;
    }

    if (status != 0) {
        result->success = false;
        goto respond;
    }

    /* Try getting the leader address from the current configuration. If no
     * server with a matching ID exists, it probably means that this is the very
     * first entry being appended and the configuration is empty. We'll retry
     * later, after we have applied the configuraiton entry. Similarly, it might
     * be that we're going to apply a new configuration where the leader is
     * removing itself, so we need to save its address here. */
    leader = raft_configuration__get(&r->configuration, request->leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    result->term = r->current_term;

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
            goto out;
        }

        index = raft_log__last_index(&r->log);

        /* If this is a configuration change entry, check that the change is
         * about promoting a non-voting server to voting, and in that case
         * update our configuration cache. */
        if (entry->type == RAFT_LOG_CONFIGURATION) {
            rv = raft_membership__apply(r, index, entry);
            if (rv != 0) {
                goto out;
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
            goto out;
        }
    }

    result->success = true;

respond:
    /* Refresh the leader address, in case it has changed or it was added in a
     * new configuration. */
    leader = raft_configuration__get(&r->configuration, request->leader_id);
    if (leader != NULL) {
        leader_address = leader->address;
    }

    /* TODO: are there cases were this assertion is unsafe? */
    assert(leader_address != NULL);

    result->last_log_index = raft_log__last_index(&r->log);

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = request->leader_id;
    message.server_address = leader_address;

    rv = r->io->send(r->io, &message, NULL, NULL);
    if (rv != 0) {
        goto out;
    }

out:
    if (request->entries != NULL) {
        raft_free(request->entries);
    }
    raft_free(request);
}

int raft_io__follower_append(struct raft *r,
                             struct raft_entry *entries,
                             size_t n,
                             unsigned leader_id,
                             raft_index leader_commit)
{
    struct raft_io__follower_append *request;
    int rv;

    assert(r != NULL);
    assert(entries != NULL);
    assert(n > 0);
    assert(leader_id != 0);

    assert(r->state == RAFT_STATE_FOLLOWER);

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    request->raft = r;
    request->index = 0;
    request->entries = entries;
    request->n = n;
    request->leader_id = leader_id;
    request->leader_commit = leader_commit;

    rv = r->io->append(r->io, entries, n, request, raft_io__follower_append_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);

err:
    assert(rv != 0);
    return rv;
}

/**
 * Callback invoked after request to send an AppendEntries RPC has completed.
 */
static void raft_io__send_append_entries_cb(void *data, int status)
{
    struct raft_io__send_append_entries *ctx = data;
    struct raft *r = ctx->raft;

    raft_debugf(r->logger, "send append entries completed: status %d", status);

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&r->log, ctx->index, ctx->entries, ctx->n);

    raft_free(ctx);
}

int raft_io__send_append_entries(struct raft *r,
                                 const struct raft_server *server,
                                 const struct raft_append_entries *args)
{
    struct raft_message message;
    struct raft_io__send_append_entries *request;
    int rv;

    /* Only leaders can send AppendEntries RPCs */
    assert(r->state == RAFT_STATE_LEADER);

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.append_entries = *args;
    message.server_id = server->id;
    message.server_address = server->address;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    request->raft = r;
    request->index = args->prev_log_index + 1;
    request->entries = args->entries;
    request->n = args->n_entries;

    rv = r->io->send(r->io, &message, request, raft_io__send_append_entries_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);

err:
    assert(rv != 0);
    return rv;
}
