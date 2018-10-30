#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "io.h"
#include "log.h"
#include "logger.h"
#include "replication.h"

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

void raft_io__queue_close(struct raft *r)
{
    size_t i;

    for (i = 0; i < r->io_queue.size; i++) {
        struct raft_io_request *request = &r->io_queue.requests[i];
        if (request->type != RAFT_IO_NULL) {
            if (request->leader_id == r->id) {
                /* This request was submitted while we were in leader state. The
                 * relevant entries were acquired from the log and need to be
                 * released. */
                assert(request->type == RAFT_IO_WRITE_LOG ||
                       request->type == RAFT_IO_APPEND_ENTRIES);
                raft_log__release(&r->log, request->index, request->entries,
                                  request->n);
            } else {
                /* This request was submitted while we were in follower
                 * state. The relevant entries were not acquired from the log
                 * (they were received from the network), so we just need to
                 * free the relevant memory. */
                assert(request->type == RAFT_IO_WRITE_LOG);
                if (request->entries != NULL) {
                    assert(request->entries[0].batch != NULL);
                    raft_free(request->entries[0].batch);
                    raft_free(request->entries);
                }
            }
            raft_io__queue_pop(r, i);
        }
    }

    if (r->io_queue.requests != NULL) {
        raft_free(r->io_queue.requests);
    }
}

/**
 * Grow the queue so it has room for at least one more entry.
 */
static int raft_io__queue_grow(struct raft *r)
{
    struct raft_io_request *requests;
    size_t size;
    size_t i;

    size = 2 * (r->io_queue.size + 1); /* New queue size */

    requests = raft_realloc(r->io_queue.requests, size * sizeof *requests);
    if (requests == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = r->io_queue.size; i < size; i++) {
        struct raft_io_request *request = &requests[i];
        request->type = RAFT_IO_NULL;
    }

    r->io_queue.requests = requests;
    r->io_queue.size = size;

    return 0;
}

int raft_io__queue_push(struct raft *r, size_t *id)
{
    size_t i;
    int rv;

    /* First try to see if we have free slot. */
    for (i = 0; i < r->io_queue.size; i++) {
        struct raft_io_request *request = &r->io_queue.requests[i];
        if (request->type == RAFT_IO_NULL) {
            *id = i;
            return 0;
        }
    }

    assert(i == r->io_queue.size);

    /* We need to grow the queue. */
    rv = raft_io__queue_grow(r);
    if (rv != 0) {
        return rv;
    }

    *id = i;

    return 0;
}

struct raft_io_request *raft_io__queue_get(struct raft *r, size_t id)
{
    assert(r != NULL);
    assert(id < r->io_queue.size);

    return &r->io_queue.requests[id];
}

void raft_io__queue_pop(struct raft *r, size_t id)
{
    assert(r != NULL);
    assert(id < r->io_queue.size);
    assert(r->io_queue.requests[id].type != RAFT_IO_NULL);

    r->io_queue.requests[id].type = RAFT_IO_NULL;
}

/**
 * An I/O request on the leader (such as sending an append entries request or
 * writing to the log) has been completed.
 */
static void raft_handle_io__leader(struct raft *r,
                                   struct raft_io_request *request,
                                   int status)
{
    assert(request->type == RAFT_IO_WRITE_LOG ||
           request->type == RAFT_IO_APPEND_ENTRIES);

    raft__debugf(r, "I/O completed on leader: status %d", status);

    /* Tell the log that we're done referencing these entries. */
    raft_log__release(&r->log, request->index, request->entries, request->n);

    /* TODO: in case this is a failed disk write and we were the leader creating
     * these entries in the first place, should we truncate our log too? since
     * we have appended these entries to it. */
    if (status != 0) {
        return;
    }

    /* If this was a disk write, check if we have reached a quorum.
     *
     * TODO: handle the case where we lost leadership in the meantime and
     * perhaps last_index changed.
     *
     * TODO2: think about the fact that last_index might be not be matching
     * this request (e.g. other entries were accpeted and pre-emptively
     * appended to the log) so essentialy we need the same min() logic as
     * above or something similar */
    if (request->type == RAFT_IO_WRITE_LOG) {
        uint64_t index;
        size_t server_index;

        index = raft_log__last_index(&r->log);
        server_index = raft_configuration__index(&r->configuration, r->id);

        r->leader_state.match_index[server_index] = index;

        raft_replication__maybe_commit(r, index);
    }
}

/**
 * An I/O request on the follower (such as writing to the log new entries
 * received via AppendEntries RPC) has been completed.
 */
static void raft_handle_io__follower(struct raft *r,
                                     struct raft_io_request *request,
                                     const struct raft_server *leader,
                                     int status)
{
    struct raft_append_entries_result result;
    size_t i;
    int rv;

    /* Disk writes are the only I/O that followers perform. */
    assert(request->type == RAFT_IO_WRITE_LOG);

    raft__debugf(r, "I/O completed on follower: status %d", status);

    if (status != 0) {
        result.success = false;
        goto out;
    }

    /* TODO: handle the case where the server has gone from the config? */
    leader = raft_configuration__get(&r->configuration, request->leader_id);

    result.term = r->current_term;

    /* Update the log and commit index to match the one from the leader.
     *
     * TODO: handle the case where we're not followers anymore? */
    for (i = 0; i < request->n; i++) {
        struct raft_entry *entry = &request->entries[i];
        rv = raft_log__append(&r->log, entry->term, entry->type, &entry->buf,
                              entry->batch);
        if (rv != 0) {
            /* TODO: what should we do? */
            // raft_io__queue_pop(r, request->id);
            return;
        }
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: If leaderCommit >
     *   commitIndex, set commitIndex = min(leaderCommit, index of last new
     *   entry).
     */
    if (request->leader_commit > r->commit_index) {
        uint64_t last_index = raft_log__last_index(&r->log);
        r->commit_index = min(request->leader_commit, last_index);
    }

    result.success = true;

out:
    if (request->entries != NULL) {
        raft_free(request->entries);
    }

    result.last_log_index = raft_log__last_index(&r->log);
    rv = r->io->send_append_entries_response(r->io, leader, &result);
    if (rv != 0) {
        /* Just log the error. */
        raft__errorf(
            r, "write log: failed to send append entries response (%d)", rv);
    }
}

void raft_handle_io(struct raft *r, const unsigned request_id, const int status)
{
    struct raft_io_request *request;
    const struct raft_server *server;

    assert(r != NULL);

    request = raft_io__queue_get(r, request_id);
    server = raft_configuration__get(&r->configuration, request->leader_id);

    if (server->id == r->id) {
        /* This I/O request was pushed at a time this server was a leader,
         * either to write entries to its own on-disk log or to replicate them
         * to a follower. */
        raft_handle_io__leader(r, request, status);
    } else {
        /* This I/O request was pushed at a time this server was a follower, to
         * replicate entries to its own log. */
        raft_handle_io__follower(r, request, server, status);
    }

    raft_io__queue_pop(r, request_id);
}
