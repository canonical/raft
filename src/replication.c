#include <assert.h>
#include <string.h>

#include "configuration.h"
#include "error.h"
#include "io.h"
#include "log.h"
#include "membership.h"
#include "replication.h"
#include "state.h"
#include "watch.h"

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

/* Set to 1 to enable debug logging. */
#if 0
#define __logf(MSG, ...) raft__debugf(r, MSG, __VA_ARGS__)
#else
#define __logf(MSG, ...)
#endif

int raft_replication__send_append_entries(struct raft *r, size_t i)
{
    struct raft_server *server = &r->configuration.servers[i];
    uint64_t next_index;
    struct raft_message message;
    struct raft_append_entries *args = &message.append_entries;
    struct raft_io__send_append_entries *ctx;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_LEADER);
    assert(server != NULL);
    assert(server->id != r->id);
    assert(server->id != 0);
    assert(r->leader_state.next_index != NULL);

    message.type = RAFT_IO_APPEND_ENTRIES;

    args->term = r->current_term;
    args->leader_id = r->id;

    next_index = r->leader_state.next_index[i];

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
        /* We're including the very first log entry, so prevIndex and prevTerm
         * are null. */
        args->prev_log_index = 0;
        args->prev_log_term = 0;
    } else {
        /* Set prevIndex and prevTerm to the index and term of the entry at
         * next_index - 1 */
        assert(next_index > 1);

        args->prev_log_index = next_index - 1;
        args->prev_log_term = raft_log__term_of(&r->log, next_index - 1);

        assert(args->prev_log_term > 0);
    }

    rv = raft_log__acquire(&r->log, next_index, &args->entries,
                           &args->n_entries);
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

    /* Allocate a new raft_io_request slot in the queue of inflight I/O
     * operations and fill the request fields. */
    /*rv = raft_queue__push(r, &request_id);
    if (rv != 0) {
        goto err_after_entries_acquired;
        }*/

    __logf("send %ld entries to server %ld (log size %ld)", args->n_entries, i,
           raft_log__n_entries(&r->log));

    ctx = raft_malloc(sizeof *ctx);
    if (ctx == NULL) {
        goto err_after_entries_acquired;
    }
    ctx->raft = r;
    ctx->index = next_index;
    ctx->entries = args->entries;
    ctx->n = args->n_entries;

    rv = r->io->send(r->io, &message, ctx, raft_io__send_cb);
    if (rv != 0) {
        goto err_after_ctx_alloc;
    }

    return 0;

err_after_ctx_alloc:
    raft_free(ctx);

err_after_entries_acquired:
    raft_log__release(&r->log, next_index, args->entries, args->n_entries);

err:
    assert(rv != 0);

    return rv;
}

int raft_replication__trigger(struct raft *r, const raft_index index)
{
    struct raft_io__write_log *request;
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rv;

    assert(r->state == RAFT_STATE_LEADER);

    if (index == 0) {
        goto send_append_entries;
    }

    /* Acquire all the entries from the given index onwards. */
    rv = raft_log__acquire(&r->log, index, &entries, &n);
    if (rv != 0) {
        goto err_after_log_append;
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
    request->leader_id = r->id;

    rv = r->io->append(r->io, entries, n, request, raft_io__append_cb);
    if (rv != 0) {
        goto err_after_request_alloc;
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
    r->timer = 0;

send_append_entries:

    /* Trigger replication. */
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        int rv;

        if (server->id == r->id) {
            continue;
        }

        rv = raft_replication__send_append_entries(r, i);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            raft_warnf(r->logger,
                       "failed to send append entries to server %ld: %s (%d)",
                       server->id, raft_strerror(rv), rv);
        }
    }

    return 0;

err_after_request_alloc:
    raft_free(request);

err_after_entries_acquired:
    raft_log__release(&r->log, index, entries, n);

err_after_log_append:
    raft_log__discard(&r->log, index);

    assert(rv != 0);

    return rv;
}

/**
 * Helper to be invoked after a promotion of a non-voting server has been
 * requested via @raft_promote and that server has caught up with logs.
 *
 * This function changes the local configuration marking the server being
 * promoted as actually voting, appends the a RAFT_LOG_CONFIGURATION entry with
 * the new configuration to the local log and triggers its replication.
 */
static int raft_replication__trigger_promotion(struct raft *r)
{
    raft_index index;
    raft_term term = r->current_term;
    size_t server_index;
    struct raft_server *server;
    int rv;

    assert(r->state == RAFT_STATE_LEADER);
    assert(r->leader_state.promotee_id != 0);

    server_index = raft_configuration__index(&r->configuration,
                                             r->leader_state.promotee_id);
    assert(server_index < r->configuration.n);

    server = &r->configuration.servers[server_index];

    assert(!server->voting);

    /* Update our current configuration. */
    server->voting = true;

    /* Index of the entry being appended. */
    index = raft_log__last_index(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = raft_log__append_configuration(&r->log, term, &r->configuration);
    if (rv != 0) {
        goto err;
    }

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = raft_replication__trigger(r, index);
    if (rv != 0) {
        goto err_after_log_append;
    }

    r->leader_state.promotee_id = 0;
    r->configuration_uncommitted_index = raft_log__last_index(&r->log);

    return 0;

err_after_log_append:
    raft_log__truncate(&r->log, index);

err:
    server->voting = false;

    assert(rv != 0);
    return rv;
}

int raft_replication__update(struct raft *r,
                             const struct raft_server *server,
                             const struct raft_append_entries_result *result)
{
    size_t server_index;
    raft_index *next_index;
    raft_index *match_index;
    raft_index last_log_index;
    bool is_being_promoted;
    int rv;

    assert(r->state == RAFT_STATE_LEADER);

    server_index = raft_configuration__index(&r->configuration, server->id);
    assert(server_index < r->configuration.n);

    match_index = &r->leader_state.match_index[server_index];
    next_index = &r->leader_state.next_index[server_index];

    /* If the reported index is lower than the match index, it must be an out of
     * order response for an old append entries. Ignore it. */
    if (*match_index > *next_index - 1) {
        raft_debugf(r->logger,
                    "match index higher than reported next index -> ignore");
        return 0;
    }

    last_log_index = raft_log__last_index(&r->log);

    /* If the RPC failed because of a log mismatch, retry.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   - If AppendEntries fails because of log inconsistency:
     *     decrement nextIndex and retry.
     */
    if (!result->success) {
        /* If the match index is already up-to-date then the rejection must be
         * stale and come from an out of order message. */
        if (match_index == next_index - 1) {
            raft_debugf(r->logger, "match index is up to date -> ignore ");
            return 0;
        }

        /* If the peer reports a last index lower than what we believed was its
         * next index, decrerment the next index to whatever is shorter: our log
         * or the peer log. Otherwise just blindly decrement next_index by 1. */
        if (result->last_log_index < *next_index - 1) {
            *next_index = min(result->last_log_index, last_log_index);
        } else {
            *next_index = *next_index - 1;
        }

        *next_index = max(*next_index, 1);

        raft_infof(r->logger, "log mismatch -> send old entries %ld",
                   *next_index);

        /* Retry, ignoring errors. */
        raft_replication__send_append_entries(r, server_index);

        return 0;
    }

    if (result->last_log_index <= *match_index) {
        /* Like above, this must be a stale response. */
        raft_debugf(r->logger, "match index is up to date -> ignore ");

        return 0;
    }

    /* In case of success the remote server is expected to send us back the
     * value of prevLogIndex + len(entriesToAppend). */
    assert(result->last_log_index <= last_log_index);

    /* If the RPC succeeded, update our counters for this server.
     *
     * From Figure 3.1:
     *
     *   [Rules for servers] Leaders:
     *
     *   If successful update nextIndex and matchIndex for follower.
     */
    *next_index = result->last_log_index + 1;
    *match_index = result->last_log_index;
    raft_debugf(r->logger, "match/next idx for server %ld: %ld %ld",
                server_index, *match_index, *next_index);

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

    return 0;
}

/**
 * Submit a write log request to the I/O implementation.
 *
 * It must be called only by followers.
 */
static int raft_replication__write_log(struct raft *r,
                                       struct raft_entry *entries,
                                       size_t n,
                                       unsigned leader_id,
                                       raft_index leader_commit)
{
    struct raft_io__write_log *request;
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

    rv = r->io->append(r->io, entries, n, request, raft_io__append_cb);
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

int raft_replication__append(struct raft *r,
                             const struct raft_append_entries *args,
                             bool *success,
                             bool *async)
{
    size_t i;
    struct raft_entry *entries;
    size_t n;
    int rv;

    assert(r != NULL);
    assert(args != NULL);
    assert(success != NULL);
    assert(async != NULL);

    assert(r->state == RAFT_STATE_FOLLOWER);

    *success = false;
    *async = false;

    /* If this is not the very first entry, we need to compare our last log
     * entry with the one in the request and check that we have a matching log,
     * possibly truncating it if not.
     *
     * From Figure 3.1:
     *
     *   [AppendEntries RPC] Receiver implementation:
     *
     *   2. Reply false if log doesn't contain an entry at prevLogIndex whose
     *   term matches prevLogTerm.
     */
    if (args->prev_log_index > 0) {
        raft_term local_prev_term =
            raft_log__term_of(&r->log, args->prev_log_index);

        if (local_prev_term == 0) {
            raft_debugf(r->logger, "no entry at previous index -> reject");
            return 0;
        }

        if (local_prev_term != args->prev_log_term) {
            if (args->prev_log_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                raft_errorf(r->logger,
                            "previous index conflicts with "
                            "committed entry -> shutdown");
                return RAFT_ERR_SHUTDOWN;
            }
            raft_debugf(r->logger, "previous term mismatch -> reject");
            return 0;
        }
    }

    /* Submit a write request for all new entries.
     *
     * From Figure 3.1:
     *
     *   [AppendEntries RPC] Receiver implementation:
     *
     *   3. If an existing entry conflicts with a new one (same index but
     *   different terms), delete the existing entry and all that follow it.
     */
    for (i = 0; i < args->n_entries; i++) {
        struct raft_entry *new_entry = &args->entries[i];
        raft_index new_entry_index = args->prev_log_index + 1 + i;
        raft_term local_term = raft_log__term_of(&r->log, new_entry_index);

        if (local_term > 0 && local_term != new_entry->term) {
            if (new_entry_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                raft_errorf(r->logger,
                            "new index conflicts with "
                            "committed entry -> shutdown");

                return RAFT_ERR_SHUTDOWN;
            }

            raft_debugf(r->logger, "log mismatch -> truncate (%ld)",
                        new_entry_index);

            /* Discard any uncommitted voting change. */
            rv = raft_membership__rollback(r);
            if (rv != 0) {
                return rv;
            }

            /* Delete all entries from this index on because they don't match */
            rv = r->io->truncate_log(r->io, new_entry_index);
            if (rv != 0) {
                return rv;
            }
            raft_log__truncate(&r->log, new_entry_index);

            /* We want to append all entries from here on, replacing anything
             * that we had before. */
            break;
        } else if (local_term == 0) {
            /* We don't have an entry at this index, so we want to append this
             * new one and all the subsequent ones. */
            break;
        }
    }

    *success = true;

    n = args->n_entries - i;
    if (n == 0) {
        /* This is an empty AppendEntries, there's nothing write. However we
         * still want to check if we can commit some entry.
         *
         * From Figure 3.1:
         *
         *   AppendEntries RPC: Receiver implementation: If leaderCommit >
         *   commitIndex, set commitIndex = min(leaderCommit, index of last new
         *   entry).
         */
        if (args->leader_commit > r->commit_index) {
            raft_index last_index = raft_log__last_index(&r->log);
            r->commit_index = min(args->leader_commit, last_index);
            rv = raft_replication__apply(r);
            if (rv != 0) {
                return rv;
            }
        }
        return 0;
    }

    *async = true;

    /* If we need to write only a part of the given entries, create a new array
     * and free the one from the request. */
    if (i == 0) {
        entries = args->entries;
    } else {
        size_t size = (args->n_entries - i) * sizeof *entries;
        entries = raft_malloc(size);
        if (entries == NULL) {
            return RAFT_ERR_NOMEM;
        }
        memcpy(entries, &args->entries[i], size);
        raft_free(args->entries);
    }

    rv = raft_replication__write_log(r, entries, n, args->leader_id,
                                     args->leader_commit);
    if (rv != 0) {
        return rv;
    }

    *success = true;

    return 0;
}

/**
 * Apply a RAFT_LOG_CONFIGURATION entry that has been committed.
 */
static void raft_replication__apply_configuration(struct raft *r,
                                                  const raft_index index)
{
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
    if (r->state == RAFT_STATE_LEADER &&
        raft_configuration__get(&r->configuration, r->id) == NULL) {
        /* Ignore the return value, since we can't fail in this case (no actual
         * write for the new term will be done) */
        raft_state__convert_to_follower(r, r->current_term);
    }

    raft_watch__configuration_applied(r);
}

/**
 * Apply a RAFT_LOG_COMMAND entry that has been committed.
 */
static int raft_replication__apply_command(struct raft *r,
                                           const raft_index index,
                                           const struct raft_buffer *buf)
{
    int rv;

    rv = r->fsm->apply(r->fsm, buf);
    if (rv != 0) {
        return rv;
    }

    raft_watch__command_applied(r, index);

    return 0;
}

int raft_replication__apply(struct raft *r)
{
    raft_index index;
    int rv;

    assert(r->state == RAFT_STATE_LEADER || r->state == RAFT_STATE_FOLLOWER);
    assert(r->last_applied <= r->commit_index);

    if (r->last_applied == r->commit_index) {
        /* Nothing to do. */
        return 0;
    }

    for (index = r->last_applied + 1; index <= r->commit_index; index++) {
        const struct raft_entry *entry = raft_log__get(&r->log, index);

        assert(entry->type == RAFT_LOG_COMMAND ||
               entry->type == RAFT_LOG_CONFIGURATION);

        switch (entry->type) {
            case RAFT_LOG_COMMAND:
                rv = raft_replication__apply_command(r, index, &entry->buf);
                break;
            case RAFT_LOG_CONFIGURATION:
                raft_replication__apply_configuration(r, index);
                rv = 0;
                break;
        }

        if (rv != 0) {
            raft_error__wrapf(r, "apply entry %ld at term %ld", index,
                              entry->term);
            break;
        }
    }

    return rv;
}

void raft_replication__quorum(struct raft *r, const raft_index index)
{
    size_t votes = 0;
    size_t i;

    assert(r->state == RAFT_STATE_LEADER);

    if (index <= r->commit_index) {
        return;
    }

    if (raft_log__term_of(&r->log, index) != r->current_term) {
        return;
    }

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (!server->voting) {
            continue;
        }
        if (r->leader_state.match_index[i] >= index) {
            votes++;
        }
    }

    if (votes > raft_configuration__n_voting(&r->configuration) / 2) {
        r->commit_index = index;

        __logf("new commit index %ld", r->commit_index);
    }

    return;
}
