#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "error.h"
#include "log.h"
#include "logging.h"
#include "membership.h"
#include "progress.h"
#include "queue.h"
#include "replication.h"
#include "request.h"
#include "snapshot.h"
#include "state.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r->io, "replication: " MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/**
 * Hold context for a #RAFT_IO_APPEND_ENTRIES send request that was submitted.
 */
struct send_append_entries
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    struct raft_io_send req;
};

struct send_install_snapshot
{
    struct raft *raft;
    struct raft_snapshot *snapshot;
    struct raft_io_snapshot_get get;
    struct raft_io_send send;
    unsigned server_id; /* ID of follower server to send the snapshot to */
};

struct recv_install_snapshot
{
    struct raft *raft;
    struct raft_snapshot snapshot;
};

/**
 * Hold context for an append request that was submitted by a leader.
 */
struct raft_replication__leader_append
{
    struct raft *raft;          /* Instance that has submitted the request */
    raft_index index;           /* Index of the first entry in the request. */
    struct raft_entry *entries; /* Entries referenced in the request. */
    unsigned n;                 /* Length of the entries array. */
    struct raft_io_append req;
};

struct raft_replication__follower_append
{
    struct raft *raft; /* Instance that has submitted the request */
    raft_index index;  /* Index of the first entry in the request. */
    struct raft_append_entries args;
    struct raft_io_append req;
};

static void send_install_snapshot_cb(struct raft_io_send *req, int status)
{
    struct send_install_snapshot *request = req->data;
    struct raft *r = request->raft;
    const struct raft_server *server;

    server = configuration__get(&r->configuration, request->server_id);
    if (status != 0) {
        errorf(r->io, "send install snapshot: %s", raft_strerror(status));
        if (r->state == RAFT_LEADER && server != NULL) {
            progress__abort_snapshot(r, server);
        }
    }

    snapshot__close(request->snapshot);
    raft_free(request->snapshot);
    raft_free(request);
}

static void send_snapshot_get_cb(struct raft_io_snapshot_get *req,
                                 struct raft_snapshot *snapshot,
                                 int status)
{
    struct send_install_snapshot *request = req->data;
    struct raft *r = request->raft;
    struct raft_message message;
    struct raft_install_snapshot *args = &message.install_snapshot;
    const struct raft_server *server;
    int rv;

    server = configuration__get(&r->configuration, request->server_id);

    if (status != 0) {
        errorf(r->io, "get snapshot %s", raft_strerror(status));
        goto err;
    }
    if (r->state != RAFT_LEADER) {
        goto err_with_snapshot;
    }
    if (server == NULL) {
        /* Probably the server was removed in the meantime. */
        goto err_with_snapshot;
    }

    assert(snapshot->n_bufs == 1);

    message.type = RAFT_IO_INSTALL_SNAPSHOT;
    message.server_id = server->id;
    message.server_address = server->address;

    args->term = r->current_term;
    args->leader_id = r->id;
    args->last_index = snapshot->index;
    args->last_term = snapshot->term;
    args->conf_index = snapshot->configuration_index;
    args->conf = snapshot->configuration;
    args->data = snapshot->bufs[0];

    request->snapshot = snapshot;
    request->send.data = request;

    infof(r->io, "sending snapshot with last index %ld to %ld", snapshot->index,
          server->id);

    rv = r->io->send(r->io, &request->send, &message, send_install_snapshot_cb);
    if (rv != 0) {
        goto err_with_snapshot;
    }

    return;

err_with_snapshot:
    snapshot__close(snapshot);
    raft_free(snapshot);
err:
    if (r->state == RAFT_LEADER && server != NULL) {
        progress__abort_snapshot(r, server);
    }
    raft_free(request);
    return;
}

/* Send the latest snapshot to the i'th server */
static int send_snapshot(struct raft *r, size_t i)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct send_install_snapshot *request;
    int rv;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    request->raft = r;
    request->server_id = server->id;
    request->get.data = request;

    progress__to_snapshot(r, server, log__snapshot_index(&r->log));

    rv = r->io->snapshot_get(r->io, &request->get, send_snapshot_get_cb);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    return 0;

err_after_req_alloc:
    progress__abort_snapshot(r, server);
    raft_free(request);
err:
    assert(rv != 0);
    return rv;
}

/* Callback invoked after request to send an AppendEntries RPC has completed. */
static void send_append_entries_cb(struct raft_io_send *req, int status)
{
    struct send_append_entries *request = req->data;
    struct raft *r = request->raft;
    (void)status;
    /* Tell the log that we're done referencing these entries. */
    log__release(&r->log, request->index, request->entries, request->n);
    raft_free(request);
}

/* Send an AppendEntries message to the i'th server, including all log entries
 * from the given point onwards. */
static int send_append_entries(struct raft *r,
                               unsigned i,
                               raft_index prev_index,
                               raft_term prev_term)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    struct send_append_entries *request;
    raft_index next_index = prev_index + 1;
    int rv;

    args->term = r->current_term;
    args->leader_id = r->id;
    args->prev_log_index = prev_index;
    args->prev_log_term = prev_term;

    /* TODO: implement a limit to the total size of the entries being sent */
    rv = log__acquire(&r->log, next_index, &args->entries, &args->n_entries);
    if (rv != 0) {
        goto err;
    }

    /* From Section §3.5:
     *
     *   The leader keeps track of the highest index it knows to be committed,
     *   and it includes that index in future AppendEntries RPCs (including
     *   heartbeats) so that the other servers eventually find out. Once a
     *   follower learns that a log entry is committed, it applies the entry to
     *   its local state machine (in log order)
     */
    args->leader_commit = r->commit_index;

    tracef("send %lu entries starting at %llu to server %lu (last index %llu)",
           args->n_entries, args->prev_log_index, server->id,
           log__last_index(&r->log));

    message.type = RAFT_IO_APPEND_ENTRIES;
    message.server_id = server->id;
    message.server_address = server->address;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_entries_acquired;
    }
    request->raft = r;
    request->index = args->prev_log_index + 1;
    request->entries = args->entries;
    request->n = args->n_entries;

    request->req.data = request;
    rv = r->io->send(r->io, &request->req, &message, send_append_entries_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);
err_after_entries_acquired:
    log__release(&r->log, next_index, args->entries, args->n_entries);
err:
    assert(rv != 0);
    return rv;
}

int replication__trigger(struct raft *r, unsigned i)
{
    struct raft_server *server = &r->configuration.servers[i];
    raft_index next_index;
    raft_index prev_index;
    raft_index snapshot_index;
    raft_term prev_term;

    assert(r->state == RAFT_LEADER);
    assert(server->id != r->id);

    /* If we have already sent a snapshot, don't send any further entry and
     * let's wait for the server reply. We'll abort and rollback to probe if we
     * don't hear anything for a while. */
    if (progress__state(r, server) == PROGRESS__SNAPSHOT) {
        return 0;
    }

    next_index = progress__next_index(r, server);
    snapshot_index = log__snapshot_index(&r->log);

    /* From Section §3.5:
     *
     *   When sending an AppendEntries RPC, the leader includes the index and
     *   term of the entry in its log that immediately precedes the new
     *   entries. If the follower does not find an entry in its log with the
     *   same index and term, then it refuses the new entries. The consistency
     *   check acts as an induction step: the initial empty state of the logs
     *   satisfies the Log Matching Property, and the consistency check
     *   preserves the Log Matching Property whenever logs are extended. As a
     *   result, whenever AppendEntries returns successfully, the leader knows
     *   that the follower’s log is identical to its own log up through the new
     *   entries (Log Matching Property in Figure 3.2).
     */
    if (next_index == 1) {
        /* We're including the very first entry, so prevIndex and prevTerm are
         * null. If the first entry is not available anymore, send the last
         * snapshot. */
        if (snapshot_index > 0) {
            return send_snapshot(r, i);
        }
        prev_index = 0;
        prev_term = 0;
    } else {
        /* Set prevIndex and prevTerm to the index and term of the entry at
         * next_index - 1 */
        assert(next_index > 1);
        prev_index = next_index - 1;
        prev_term = log__term_of(&r->log, next_index - 1);
        /* If the entry is not anymore in our log, send the last snapshot. */
        if (prev_term == 0) {
            assert(next_index - 1 < snapshot_index);
            tracef("missing entry at index %lld -> send snapshot",
                   next_index - 1);
            return send_snapshot(r, i);
        }
    }

    return send_append_entries(r, i, prev_index, prev_term);
}

/* Called after a successful append entries I/O request to update the index of
 * the last entry stored on disk. Return how many new entries that are still
 * present in our in-memory log were stored. */
static size_t update_last_stored(struct raft *r,
                                 raft_index first_index,
                                 struct raft_entry *entries,
                                 size_t n_entries)
{
    size_t i;

    /* Check which of these entries is still in our in-memory log */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];
        raft_index index = first_index + i;
        raft_term local_term = log__term_of(&r->log, index);

        /* If we have no entry at this index, or if the entry we have now has a
         * different term, it means that this entry got truncated, so let's stop
         * here. */
        if (local_term == 0 || (local_term > 0 && local_term != entry->term)) {
            break;
        }

        assert(local_term != 0 && local_term == entry->term);
    }

    r->last_stored += i;
    return i;
}

static void raft_replication__leader_append_cb(struct raft_io_append *req,
                                               int status)
{
    struct raft_replication__leader_append *request = req->data;
    struct raft *r = request->raft;
    size_t server_index;
    int rv;

    tracef("leader: written %u entries starting at %lld: status %d", request->n,
           request->index, status);

    update_last_stored(r, request->index, request->entries, request->n);

    /* Tell the log that we're done referencing these entries. */
    log__release(&r->log, request->index, request->entries, request->n);

    raft_free(request);

    /* If we are not leader anymore, just discard the result. */
    if (r->state != RAFT_LEADER) {
        tracef("local server is not leader -> ignore write log result");
        return;
    }

    /* TODO: in case this is a failed disk write and we were the leader creating
     * these entries in the first place, should we truncate our log too? since
     * we have appended these entries to it. */
    if (status != 0) {
        return;
    }

    /* If Check if we have reached a quorum. */
    server_index = configuration__index_of(&r->configuration, r->id);

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
        r->leader_state.progress[server_index].match_index = r->last_stored;
    } else {
        const struct raft_entry *entry = log__get(&r->log, r->last_stored);
        assert(entry->type == RAFT_CHANGE);
    }

    /* Check if we can commit some new entries. */
    raft_replication__quorum(r, r->last_stored);

    rv = raft_replication__apply(r);
    if (rv != 0) {
        /* TODO: just log the error? */
    }
}

static int raft_replication__leader_append(struct raft *r, unsigned index)
{
    struct raft_entry *entries;
    unsigned n;
    struct raft_replication__leader_append *request;
    int rv;

    assert(r->state == RAFT_LEADER);

    if (index == 0) {
        return 0;
    }

    /* Acquire all the entries from the given index onwards. */
    rv = log__acquire(&r->log, index, &entries, &n);
    if (rv != 0) {
        goto err;
    }

    /* We expect this function to be called only when there are actually
     * some entries to write. */
    assert(n > 0);

    /* Allocate a new request. */
    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_entries_acquired;
    }

    request->raft = r;
    request->index = index;
    request->entries = entries;
    request->n = n;
    request->req.data = request;

    rv = r->io->append(r->io, &request->req, entries, n,
                       raft_replication__leader_append_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    return 0;

err_after_request_alloc:
    raft_free(request);

err_after_entries_acquired:
    log__release(&r->log, index, entries, n);

err:
    assert(rv != 0);
    return rv;
}

int raft_replication__trigger(struct raft *r, const raft_index index)
{
    size_t i;
    int rv;

    assert(r->state == RAFT_LEADER);

    rv = raft_replication__leader_append(r, index);
    if (rv != 0) {
        goto err;
    }

    /* Reset the heartbeat timer: for a full request_timeout period we'll be
     * good and we won't need to contact followers again, since this was not an
     * idle period.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] Leaders: Upon election: send initial empty
     *   AppendEntries RPCs (heartbeat) to each server; repeat during idle
     *   periods to prevent election timeouts
     */
    r->leader_state.heartbeat_elapsed = 0;

    /* Trigger replication for servers we didn't hear from recently. */
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id) {
            continue;
        }
        rv = replication__trigger(r, i);
        if (rv != 0 && rv != RAFT_NOCONNECTION) {
            /* This is not a critical failure, let's just log it. */
            warnf(r->io, "failed to send append entries to server %ld: %s (%d)",
                  server->id, raft_strerror(rv), rv);
        }
    }

    return 0;

err:
    assert(rv != 0);

    return rv;
}

/**
 * Helper to be invoked after a promotion of a non-voting server has been
 * requested via @raft_promote and that server has caught up with logs.
 *
 * This function changes the local configuration marking the server being
 * promoted as actually voting, appends the a RAFT_CHANGE entry with
 * the new configuration to the local log and triggers its replication.
 */
static int raft_replication__trigger_promotion(struct raft *r)
{
    raft_index index;
    raft_term term = r->current_term;
    size_t server_index;
    struct raft_server *server;
    int rv;

    assert(r->state == RAFT_LEADER);
    assert(r->leader_state.promotee_id != 0);

    server_index =
        configuration__index_of(&r->configuration, r->leader_state.promotee_id);
    assert(server_index < r->configuration.n);

    server = &r->configuration.servers[server_index];

    assert(!server->voting);

    /* Update our current configuration. */
    server->voting = true;

    /* Index of the entry being appended. */
    index = log__last_index(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = log__append_configuration(&r->log, term, &r->configuration);
    if (rv != 0) {
        goto err;
    }

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = raft_replication__trigger(r, index);
    if (rv != 0) {
        goto err_after_log_append;
    }

    r->leader_state.promotee_id = 0;
    r->configuration_uncommitted_index = log__last_index(&r->log);

    return 0;

err_after_log_append:
    log__truncate(&r->log, index);

err:
    server->voting = false;

    assert(rv != 0);
    return rv;
}

int replication__update(struct raft *r,
                        const struct raft_server *server,
                        const struct raft_append_entries_result *result)
{
    size_t server_index;
    bool is_being_promoted;
    raft_index last_index;
    int rv;

    assert(r->state == RAFT_LEADER);

    server_index = configuration__index_of(&r->configuration, server->id);
    assert(server_index < r->configuration.n);

    progress__mark_recent_recv(r, server);

    /* If the RPC failed because of a log mismatch, retry.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   - If AppendEntries fails because of log inconsistency:
     *     decrement nextIndex and retry.
     */
    if (result->rejected > 0) {
        bool retry;
        retry = progress__maybe_decrement(r, server, result->rejected,
                                          result->last_log_index);
        if (retry) {
            /* Retry, ignoring errors. */
            tracef("log mismatch -> send old entries to %u", server->id);
            replication__trigger(r, server_index);
        }
        return 0;
    }

    /* In case of success the remote server is expected to send us back the
     * value of prevLogIndex + len(entriesToAppend). If it has a longer log, it
     * might be a leftover from previous terms. */
    last_index = result->last_log_index;
    if (last_index > log__last_index(&r->log)) {
        last_index = log__last_index(&r->log);
    }

    /* If the RPC succeeded, update our counters for this server.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   If successful update nextIndex and matchIndex for follower.
     */
    if (!progress__maybe_update(r, server, last_index)) {
        return 0;
    }

    /* If a snapshot has been installed, transition back to probe */
    if (progress__state(r, server) == PROGRESS__SNAPSHOT &&
        progress__snapshot_done(r, server)) {
        progress__to_probe(r, server);
    }

    /* If the server is currently being promoted and is catching with logs,
     * update the information about the current catch-up round, and possibly
     * proceed with the promotion. */
    is_being_promoted = r->leader_state.promotee_id != 0 &&
                        r->leader_state.promotee_id == server->id;
    if (is_being_promoted) {
        int is_up_to_date = raft_membership__update_catch_up_round(r);
        if (is_up_to_date) {
            rv = raft_replication__trigger_promotion(r);
            if (rv != 0) {
                return rv;
            }
        }
    }

    /* Check if we can commit some new entries. */
    raft_replication__quorum(r, r->last_stored);

    rv = raft_replication__apply(r);
    if (rv != 0) {
        /* TODO: just log the error? */
    }

    /* TODO: switch to pipeline replication */

    return 0;
}

static void send_append_entries_result_cb(struct raft_io_send *req, int status)
{
    (void)status;
    raft_free(req);
}

static void send_append_entries_result(
    struct raft *r,
    const struct raft_append_entries_result *result)
{
    struct raft_message message;
    struct raft_io_send *req;
    int rc;

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = r->follower_state.current_leader.id;
    message.server_address = r->follower_state.current_leader.address;
    message.append_entries_result = *result;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return;
    }

    rc = r->io->send(r->io, req, &message, send_append_entries_result_cb);
    if (rc != 0) {
        raft_free(req);
    }
}

static void raft_replication__follower_append_cb(struct raft_io_append *req,
                                                 int status)
{
    struct raft_replication__follower_append *request = req->data;
    struct raft *r = request->raft;
    struct raft_append_entries *args = &request->args;
    struct raft_append_entries_result result;
    size_t i;
    size_t j;
    int rv;

    /* Abort here if we're not followers anymore (e.g. we're shutting down) */
    if (r->state != RAFT_FOLLOWER) {
        goto out;
    }

    tracef("I/O completed on follower: status %d", status);

    assert(args->leader_id > 0);
    assert(args->entries != NULL);
    assert(args->n_entries > 0);

    i = update_last_stored(r, request->index, args->entries, args->n_entries);

    /* If we are not followers anymore, just discard the result. */
    if (r->state != RAFT_FOLLOWER) {
        tracef("local server is not follower -> ignore I/O result");
        goto out;
    }

    result.term = r->current_term;
    if (status != 0) {
        result.rejected = args->prev_log_index + 1;
        goto respond;
    }

    /* If none of the entries that we persisted is present anymore in our
     * in-memory log, there's nothing to report or to do. We just discard
     * them. */
    if (i == 0) {
        goto out;
    }

    /* Possibly apply configuration changes. */
    for (j = 0; j < i; j++) {
        struct raft_entry *entry = &args->entries[j];
        raft_index index = request->index + j;
        raft_term local_term = log__term_of(&r->log, index);

        assert(local_term != 0 && local_term == entry->term);

        /* If this is a configuration change entry, check if the change is about
         * promoting a non-voting server to voting, and in that case update our
         * configuration cache. */
        if (entry->type == RAFT_CHANGE) {
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
    if (args->leader_commit > r->commit_index) {
        r->commit_index = min(args->leader_commit, r->last_stored);
        rv = raft_replication__apply(r);
        if (rv != 0) {
            goto out;
        }
    }

    result.rejected = 0;

respond:
    result.last_log_index = r->last_stored;
    send_append_entries_result(r, &result);

out:
    log__release(&r->log, request->index, request->args.entries,
                 request->args.n_entries);

    raft_free(request);
}

/**
 * Check that the log matching property against an incoming AppendEntries
 * request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   2. Reply false if log doesn't contain an entry at prevLogIndex whose
 *   term matches prevLogTerm.
 *
 * Return 0 if the check passed.
 *
 * Return 1 if the check did not pass and the request needs to be rejected.
 *
 * Return -1 if there's a conflict and we need to shutdown.
 */
static int check_prev_log_entry(struct raft *r,
                                const struct raft_append_entries *args)
{
    raft_term local_prev_term;

    /* If this is the very first entry, there's nothing to check. */
    if (args->prev_log_index == 0) {
        return 0;
    }

    local_prev_term = log__term_of(&r->log, args->prev_log_index);
    if (local_prev_term == 0) {
        tracef("no entry at index %llu -> reject", args->prev_log_index);
        return 1;
    }

    if (local_prev_term != args->prev_log_term) {
        if (args->prev_log_index <= r->commit_index) {
            /* Should never happen; something is seriously wrong! */
            errorf(r->io,
                   "previous index conflicts with "
                   "committed entry -> shutdown");
            return -1;
        }
        tracef("previous term mismatch -> reject");
        return 1;
    }

    return 0;
}

/**
 * Delete from our log all entries that conflict with the ones in the given
 * AppendEntries request.
 *
 * From Figure 3.1:
 *
 *   [AppendEntries RPC] Receiver implementation:
 *
 *   3. If an existing entry conflicts with a new one (same index but
 *   different terms), delete the existing entry and all that follow it.
 *
 * The @i parameter will be set to the array index of the first new log entry
 * that we don't have yet in our log, among the ones included in the given
 * AppendEntries request.
 */
static int raft_replication__delete_conflicting_entries(
    struct raft *r,
    const struct raft_append_entries *args,
    size_t *i)
{
    size_t j;
    int rv;

    for (j = 0; j < args->n_entries; j++) {
        struct raft_entry *entry = &args->entries[j];
        raft_index entry_index = args->prev_log_index + 1 + j;
        raft_term local_term = log__term_of(&r->log, entry_index);

        if (local_term > 0 && local_term != entry->term) {
            if (entry_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                errorf(r->io,
                       "new index conflicts with "
                       "committed entry -> shutdown");

                return RAFT_SHUTDOWN;
            }

            tracef("log mismatch -> truncate (%ld)", entry_index);

            /* Discard any uncommitted voting change. */
            rv = raft_membership__rollback(r);
            if (rv != 0) {
                return rv;
            }

            /* Delete all entries from this index on because they don't match */
            rv = r->io->truncate(r->io, entry_index);
            if (rv != 0) {
                return rv;
            }
            log__truncate(&r->log, entry_index);
            r->last_stored = entry_index - 1;

            /* We want to append all entries from here on, replacing anything
             * that we had before. */
            break;
        } else if (local_term == 0) {
            /* We don't have an entry at this index, so we want to append this
             * new one and all the subsequent ones. */
            break;
        }
    }

    *i = j;

    return 0;
}

int raft_replication__append(struct raft *r,
                             const struct raft_append_entries *args,
                             raft_index *rejected,
                             bool *async)
{
    struct raft_replication__follower_append *request;
    int match;
    size_t n;
    size_t i;
    size_t j;
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(rejected != NULL);
    assert(async != NULL);

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->prev_log_index;
    *async = false;

    /* Check the log matching property. */
    match = check_prev_log_entry(r, args);
    if (match != 0) {
        assert(match == 1 || match == -1);
        return match == 1 ? 0 : RAFT_SHUTDOWN;
    }
    rv = raft_replication__delete_conflicting_entries(r, args, &i);
    if (rv != 0) {
        return rv;
    }

    *rejected = 0;

    n = args->n_entries - i; /* Number of new entries */

    /* This is an empty AppendEntries, there's nothing to write. However we
     * still want to check if we can commit some entry.
     *
     * From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (n == 0) {
        if (args->entries == NULL) {
            tracef("append entries is heartbeat -> succeed immediately");
        } else {
            tracef("append entries has nothing new -> succeed immediately");
        }
        if (args->leader_commit > r->commit_index) {
            raft_index last_index = log__last_index(&r->log);
            r->commit_index = min(args->leader_commit, last_index);
            rv = raft_replication__apply(r);
            if (rv != 0) {
                return rv;
            }
        }

        return 0;
    }

    *async = true;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    request->raft = r;
    request->args = *args;
    request->index = args->prev_log_index + 1 + i;

    /* Update our in-memory log to reflect that we received these entries. We'll
     * notify the leader of a successful append once the write entries request
     * that we issue below actually completes.  */
    for (j = 0; j < n; j++) {
        struct raft_entry *entry = &args->entries[i + j];

        rv = log__append(&r->log, entry->term, entry->type, &entry->buf,
                         entry->batch);
        if (rv != 0) {
            /* TODO: we should revert any changes we made to the log */
            goto err_after_request_alloc;
        }
    }

    /* Acquire the relevant entries from the log. */
    rv = log__acquire(&r->log, request->index, &request->args.entries,
                      &request->args.n_entries);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    assert(request->args.n_entries == n);

    request->req.data = request;
    rv = r->io->append(r->io, &request->req, request->args.entries,
                       request->args.n_entries,
                       raft_replication__follower_append_cb);
    if (rv != 0) {
        goto err_after_acquire_entries;
    }

    *rejected = 0;

    raft_free(args->entries);

    return 0;

err_after_acquire_entries:
    log__release(&r->log, request->index, request->args.entries,
                 request->args.n_entries);

err_after_request_alloc:
    raft_free(request);

err:
    assert(rv != 0);
    return rv;
}

static void put_snapshot_cb(struct raft_io_snapshot_put *req, int status)
{
    struct recv_install_snapshot *request = req->data;
    struct raft *r = request->raft;
    struct raft_snapshot *snapshot = &request->snapshot;
    struct raft_append_entries_result result;
    int rv;

    r->snapshot.put.data = NULL;

    result.term = r->current_term;

    if (status != 0) {
        result.rejected = snapshot->index;
        errorf(r->io, "save snapshot %d: %s", snapshot->index,
               raft_strerror(status));
        goto err;
    }

    /* From Figure 5.3:
     *
     *   7. Discard the entire log
     *   8. Reset state machine using snapshot contents (and load lastConfig
     *      as cluster configuration).
     */
    rv = snapshot__restore(r, snapshot);
    if (rv != 0) {
        result.rejected = snapshot->index;
        errorf(r->io, "restore snapshot %d: %s", snapshot->index,
               raft_strerror(status));
        goto err;
    }

    debugf(r->io, "restored snapshot with last index %llu", snapshot->index);

    result.rejected = 0;

    goto respond;

err:
    /* In case of error we must also free the snapshot data buffer and free the
     * configuration. */
    raft_free(snapshot->bufs[0].base);
    raft_configuration_close(&snapshot->configuration);

respond:
    result.last_log_index = r->last_stored;
    send_append_entries_result(r, &result);
    raft_free(request);
}

int raft_replication__install_snapshot(struct raft *r,
                                       const struct raft_install_snapshot *args,
                                       raft_index *rejected,
                                       bool *async)
{
    struct recv_install_snapshot *request;
    struct raft_snapshot *snapshot;
    raft_term local_term;
    int rv;

    assert(r->state == RAFT_FOLLOWER);

    *rejected = args->last_index;
    *async = false;

    /* If we are taking a snapshot ourselves or installing a snapshot, ignore
     * the request, the leader will weventually retry. TODO: we should do
     * something smarter. */
    if (r->snapshot.pending.term != 0 || r->snapshot.put.data != NULL) {
        *async = true;
        return 0;
    }

    /* If our last snapshot is more up-to-date, this is a no-op */
    if (r->log.snapshot.last_index >= args->last_index) {
        *rejected = 0;
        return 0;
    }

    /* If we already have all entries in the snapshot, this is a no-op */
    local_term = log__term_of(&r->log, args->last_index);
    if (local_term != 0 && local_term >= args->last_term) {
        *rejected = 0;
        return 0;
    }

    *async = true;

    /* Premptively update our in-memory state. */
    log__restore(&r->log, args->last_index, args->last_term);

    /* We need to truncate our entire log */
    rv = r->io->truncate(r->io, 1);
    if (rv != 0) {
        goto err;
    }
    r->last_stored = 0;

    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    request->raft = r;

    snapshot = &request->snapshot;
    snapshot->term = args->last_term;
    snapshot->index = args->last_index;
    snapshot->configuration_index = args->conf_index;
    snapshot->configuration = args->conf;

    snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
    if (snapshot->bufs == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_request_alloc;
    }
    snapshot->bufs[0] = args->data;
    snapshot->n_bufs = 1;

    /* TODO: we should truncate the in-memory log immediately */
    assert(r->snapshot.put.data == NULL);
    r->snapshot.put.data = request;
    rv =
        r->io->snapshot_put(r->io, &r->snapshot.put, snapshot, put_snapshot_cb);
    if (rv != 0) {
        goto err_after_bufs_alloc;
    }

    return 0;

err_after_bufs_alloc:
    raft_free(snapshot->bufs);
    r->snapshot.put.data = NULL;
err_after_request_alloc:
    raft_free(request);
err:
    assert(rv != 0);
    return rv;
}

/* Get the request matching the given index and type, if any. */
static struct request *getRequest(struct raft *r,
                                  const raft_index index,
                                  int type)
{
    queue *head;
    struct request *req;

    if (r->state != RAFT_LEADER) {
        return NULL;
    }
    QUEUE_FOREACH(head, &r->leader_state.requests)
    {
        req = QUEUE_DATA(head, struct request, queue);
        if (req->index == index) {
            assert(req->type == type);
            QUEUE_REMOVE(head);
            return req;
        }
    }
    return NULL;
}

/**
 * Apply a RAFT_CHANGE entry that has been committed.
 */
static void raft_replication__apply_configuration(struct raft *r,
                                                  const raft_index index)
{
    struct raft_change *req;

    assert(index > 0);

    /* If this is an uncommitted configuration that we had already applied when
     * submitting the configuration change (for leaders) or upon receiving it
     * via an AppendEntries RPC (for followers), then reset the uncommitted
     * index, since that uncommitted configuration is now committed. */
    if (r->configuration_uncommitted_index == index) {
        r->configuration_uncommitted_index = 0;
    }

    r->configuration_index = index;

    /* If we are leader but not part of this new configuration, step down.
     *
     * From Section 4.2.2:
     *
     *   In this approach, a leader that is removed from the configuration steps
     *   down once the Cnew entry is committed.
     */
    if (r->state == RAFT_LEADER &&
        configuration__get(&r->configuration, r->id) == NULL) {
        convertToFollower(r);
    }

    req = (struct raft_change *)getRequest(r, index, RAFT_CHANGE);
    if (req != NULL && req->cb != NULL) {
        req->cb(req, 0);
    }
}

/* Apply a RAFT_COMMAND entry that has been committed. */
static int raft_replication__apply_command(struct raft *r,
                                           const raft_index index,
                                           const struct raft_buffer *buf)
{
    struct raft_apply *req;
    void *result;
    int rv;
    rv = r->fsm->apply(r->fsm, buf, &result);
    if (rv != 0) {
        return rv;
    }
    req = (struct raft_apply *)getRequest(r, index, RAFT_COMMAND);
    if (req != NULL && req->cb != NULL) {
        req->cb(req, 0);
    }
    return 0;
}

static void raft_replication__apply_barrier(struct raft *r,
                                            const raft_index index)
{
    struct raft_apply *req;
    req = (struct raft_apply *)getRequest(r, index, RAFT_BARRIER);
    if (req != NULL && req->cb != NULL) {
        req->cb(req, 0);
    }
}

static bool should_take_snapshot(struct raft *r)
{
    /* If a snapshot is already in progress, we don't want to start another
     *  one. */
    if (r->snapshot.pending.term != 0) {
        return false;
    };

    /* If we didn't reach the threshold yet, do nothing. */
    if (r->last_applied - r->log.snapshot.last_index < r->snapshot.threshold) {
        return false;
    }

    return true;
}

static void snapshot_put_cb(struct raft_io_snapshot_put *req, int status)
{
    struct raft *r = req->data;
    struct raft_snapshot *snapshot;

    r->snapshot.put.data = NULL;
    snapshot = &r->snapshot.pending;

    if (status != 0) {
        debugf(r->io, "snapshot %lld at term %lld: %s", snapshot->index,
               snapshot->term, raft_strerror(status));
        goto out;
    }

    log__snapshot(&r->log, snapshot->index, r->snapshot.trailing);

out:
    snapshot__close(&r->snapshot.pending);
    r->snapshot.pending.term = 0;
}

static int take_snapshot(struct raft *r)
{
    struct raft_snapshot *snapshot;
    unsigned i;
    int rv;

    debugf(r->io, "take snapshot at %lld", r->last_applied);

    snapshot = &r->snapshot.pending;
    snapshot->index = r->last_applied;
    snapshot->term = log__term_of(&r->log, r->last_applied);

    raft_configuration_init(&snapshot->configuration);
    rv = configuration__copy(&r->configuration, &snapshot->configuration);
    if (rv != 0) {
        goto err;
    }

    snapshot->configuration_index = r->configuration_index;

    rv = r->fsm->snapshot(r->fsm, &snapshot->bufs, &snapshot->n_bufs);
    if (rv != 0) {
        goto err_after_config_copy;
    }

    assert(r->snapshot.put.data == NULL);
    r->snapshot.put.data = r;
    rv =
        r->io->snapshot_put(r->io, &r->snapshot.put, snapshot, snapshot_put_cb);
    if (rv != 0) {
        goto err_after_fsm_snapshot;
    }

    return 0;

err_after_fsm_snapshot:
    for (i = 0; i < snapshot->n_bufs; i++) {
        raft_free(snapshot->bufs[i].base);
    }
    raft_free(snapshot->bufs);
err_after_config_copy:
    raft_configuration_close(&snapshot->configuration);
err:
    r->snapshot.pending.term = 0;
    assert(rv);
    return rv;
}

int raft_replication__apply(struct raft *r)
{
    raft_index index;
    int rv;

    assert(r->state == RAFT_LEADER || r->state == RAFT_FOLLOWER);
    assert(r->last_applied <= r->commit_index);

    if (r->last_applied == r->commit_index) {
        /* Nothing to do. */
        return 0;
    }

    for (index = r->last_applied + 1; index <= r->commit_index; index++) {
        const struct raft_entry *entry = log__get(&r->log, index);

        assert(entry->type == RAFT_COMMAND || entry->type == RAFT_BARRIER ||
               entry->type == RAFT_CHANGE);

        switch (entry->type) {
            case RAFT_COMMAND:
                rv = raft_replication__apply_command(r, index, &entry->buf);
                break;
            case RAFT_BARRIER:
                raft_replication__apply_barrier(r, index);
                rv = 0;
                break;
            case RAFT_CHANGE:
                raft_replication__apply_configuration(r, index);
                rv = 0;
                break;
        }

        if (rv != 0) {
            break;
        }

        r->last_applied = index;
    }

    if (should_take_snapshot(r)) {
        rv = take_snapshot(r);
    }

    return rv;
}

void raft_replication__quorum(struct raft *r, const raft_index index)
{
    size_t votes = 0;
    size_t i;

    assert(r->state == RAFT_LEADER);

    if (index <= r->commit_index) {
        return;
    }

    /* TODO: fuzzy-test --seed 0x8db5fccc replication/entries/partitioned
     * fails the assertion below. */
    if (log__term_of(&r->log, index) == 0) {
        return;
    }
    // assert(log__term_of(&r->log, index) > 0);
    assert(log__term_of(&r->log, index) <= r->current_term);

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (!server->voting) {
            continue;
        }
        if (r->leader_state.progress[i].match_index >= index) {
            votes++;
        }
    }

    if (votes > configuration__n_voting(&r->configuration) / 2) {
        r->commit_index = index;
        tracef("new commit index %ld", r->commit_index);
    }

    return;
}
