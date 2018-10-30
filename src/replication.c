#include <assert.h>
#include <string.h>

#include "configuration.h"
#include "io.h"
#include "log.h"
#include "logger.h"
#include "replication.h"

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
    struct raft_append_entries_args args;
    uint64_t next_index;
    size_t request_id;
    struct raft_io_request *request;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_LEADER);
    assert(server != NULL);
    assert(server->id != r->id);
    assert(server->id != 0);
    assert(r->leader_state.next_index != NULL);

    args.term = r->current_term;
    args.leader_id = r->id;

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
        args.prev_log_index = 0;
        args.prev_log_term = 0;
    } else {
        /* Set prevIndex and prevTerm to the index and term of the entry at
         * next_index - 1 */
        args.prev_log_index = next_index - 1;
        args.prev_log_term = raft_log__term_of(&r->log, next_index - 1);

        assert(args.prev_log_term > 0);
    }

    rv = raft_log__acquire(&r->log, next_index, &args.entries, &args.n);
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
    args.leader_commit = r->commit_index;

    /* Allocate a new raft_io_request slot in the queue of inflight I/O
     * operations and fill the request fields. */
    rv = raft_io__queue_push(r, &request_id);
    if (rv != 0) {
        goto err_after_entries_acquired;
    }

    __logf("send %ld entries to server %ld (request ID %ld) (log size %ld)",
           args.n, i, request_id, raft_log__n_entries(&r->log));

    request = raft_io__queue_get(r, request_id);
    request->index = next_index;
    request->type = RAFT_IO_APPEND_ENTRIES;
    request->entries = args.entries;
    request->n = args.n;
    request->leader_id = r->id;

    rv = r->io->send_append_entries_request(r->io, request_id, server, &args);
    if (rv != 0) {
        goto err_after_io_queue_push;
    }

    return 0;

err_after_io_queue_push:
    raft_io__queue_pop(r, request_id);

err_after_entries_acquired:
    raft_log__release(&r->log, next_index, args.entries, args.n);

err:
    assert(rv != 0);

    return rv;
}

void raft_replication__send_heartbeat(struct raft *r)
{
    size_t i;

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        int rv;

        if (server->id == r->id) {
            continue;
        }

        rv = raft_replication__send_append_entries(r, i);
        if (rv != 0) {
            /* This is not a critical failure, let's just log it. */
            raft__warnf(r,
                        "failed to send append entries to server %ld: %s (%d)",
                        server->id, raft_strerror(rv), rv);
        }
    }
}

/**
 * Submit a write log request to the I/O implementation.
 */
static int raft_replication__write_log(struct raft *r,
                                       struct raft_entry *entries,
                                       size_t n,
                                       unsigned leader_id,
                                       raft_index leader_commit)
{
    struct raft_io_request *request;
    size_t request_id;
    int rv;

    assert(r != NULL);
    assert(entries != NULL);
    assert(n > 0);
    assert(leader_id != 0);

    /* This should be called be either followers or leaders. */
    assert(r->state == RAFT_STATE_FOLLOWER || r->state == RAFT_STATE_LEADER);

    rv = raft_io__queue_push(r, &request_id);
    if (rv != 0) {
        return rv;
    }

    request = raft_io__queue_get(r, request_id);
    request->type = RAFT_IO_WRITE_LOG;
    request->entries = entries;
    request->n = n;
    request->leader_id = leader_id;
    request->leader_commit = leader_commit;

    rv = r->io->write_log(r->io, request_id, entries, n);
    if (rv != 0) {
        raft_io__queue_pop(r, request_id);
        return rv;
    }

    return 0;
}

int raft_replication__maybe_append(struct raft *r,
                                   const struct raft_append_entries_args *args,
                                   bool *success,
                                   bool *async)
{
    size_t i;
    struct raft_entry *entries;
    size_t n;
    int rv;

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
        uint64_t our_prev_term =
            raft_log__term_of(&r->log, args->prev_log_index);

        if (our_prev_term == 0) {
            raft__debugf(r, "no entry at previous index -> reject");
            return 0;
        }

        if (our_prev_term != args->prev_log_term) {
            if (args->prev_log_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                raft__errorf(r,
                             "previous index conflicts with "
                             "committed entry -> shutdown");
                return RAFT_ERR_SHUTDOWN;
            }
            raft__debugf(r, "previous term mismatch -> reject");
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
    for (i = 0; i < args->n; i++) {
        struct raft_entry *new_entry = &args->entries[i];
        uint64_t new_entry_index = args->prev_log_index + 1 + i;
        uint64_t our_term = raft_log__term_of(&r->log, new_entry_index);

        if (our_term > 0 && our_term != new_entry->term) {
            if (new_entry_index <= r->commit_index) {
                /* Should never happen; something is seriously wrong! */
                raft__errorf(r,
                             "new index conflicts with "
                             "committed entry -> shutdown");

                return RAFT_ERR_SHUTDOWN;
            }

            raft__debugf(r, "log mismatch -> truncate (%ld)", new_entry_index);

            /* Delete all entries from this index on because they don't match */
            rv = r->io->truncate_log(r->io, new_entry_index);
            if (rv != 0) {
                return rv;
            }
            raft_log__truncate(&r->log, new_entry_index);

            /* We want to append all entries from here on, replacing anything
             * that we had before. */
            break;
        } else if (our_term == 0) {
            /* We don't an entry at this index, so we want to append the new one
             * and all the subsequent ones. */
            break;
        }
    }

    *success = true;

    n = args->n - i;
    if (n == 0) {
        /* This is an empty AppendEntries, there's nothing write. */
        return 0;
    }

    *async = true;

    if (i == 0) {
        entries = args->entries;
    } else {
        size_t size = (args->n - i) * sizeof *entries;
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

void raft_replication__update_server(
    struct raft *r,
    size_t server_index,
    const struct raft_append_entries_result *result)
{
    raft_index *next_index;
    raft_index *match_index;
    raft_index last_log_index;

    match_index = &r->leader_state.match_index[server_index];
    next_index = &r->leader_state.next_index[server_index];

    /* If the reported index is lower than the match index, it must be an out of
     * order response for an old append entries. Ignore it. */
    if (*match_index > *next_index - 1) {
        raft__debugf(r,
                     "match index higher than reported next index -> ignore");
        return;
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
            raft__debugf(r, "match index is up to date -> ignore ");
            return;
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

        raft__infof(r, "log mismatch -> send old entries %ld", *next_index);

        /* Retry, ignoring errors. */
        raft_replication__send_append_entries(r, server_index);

        return;
    }

    if (result->last_log_index <= *match_index) {
        /* Like above, this must be a stale response. */
        raft__debugf(r, "match index is up to date -> ignore ");

        return;
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
    raft__debugf(r, "match/next idx for server %ld: %ld %ld", server_index,
                 *next_index, *match_index);

    return;
}

void raft_replication__maybe_commit(struct raft *r, raft_index index)
{
    size_t votes = 0;
    size_t i;

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
    }
}
