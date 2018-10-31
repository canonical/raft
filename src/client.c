#include <assert.h>

#include "../include/raft.h"

#include "error.h"
#include "io.h"
#include "log.h"
#include "logger.h"
#include "replication.h"

int raft_accept(struct raft *r,
                const struct raft_buffer bufs[],
                const unsigned n)
{
    struct raft_io_request *request;
    raft_index index;
    struct raft_entry *entries;
    unsigned m;
    size_t request_id;
    unsigned i;
    int rv;

    assert(r != NULL);
    assert(bufs != NULL);
    assert(n > 0);

    if (r->state != RAFT_STATE_LEADER) {
        rv = RAFT_ERR_NOT_LEADER;
        raft_error__printf(r, rv, "can't accept entries");
        goto err;
    }

    raft__debugf(r, "client request");

    /* Index of the first entry being appended. */
    index = raft_log__last_index(&r->log) + 1;

    /* Append the new entries to the log. */
    for (i = 0; i < n; i++) {
        const struct raft_buffer *buf = &bufs[i];
        const raft_term term = r->current_term;
        rv = raft_log__append(&r->log, term, RAFT_LOG_COMMAND, buf, NULL);
        if (rv != 0) {
            return rv;
        }
    }

    /* Acquire all the entries we just appended. */
    rv = raft_log__acquire(&r->log, index, &entries, &m);
    if (rv != 0) {
        goto err_after_log_append;
    }
    assert(m == n);

    /* Allocate a new raft_io_request slot in the queue of inflight I/O
     * operations and fill the request fields. */
    rv = raft_io__queue_push(r, &request_id);
    if (rv != 0) {
        goto err_after_entries_acquired;
    }

    request = raft_io__queue_get(r, request_id);
    request->index = index;
    request->type = RAFT_IO_WRITE_LOG;
    request->entries = entries;
    request->n = n;
    request->leader_id = r->id;

    rv = r->io->write_log(r->io, request_id, entries, n);
    if (rv != 0) {
        goto err_after_io_queue_push;
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

    raft_replication__send_heartbeat(r);

    return 0;

err_after_io_queue_push:
    raft_io__queue_pop(r, request_id);

err_after_entries_acquired:
    raft_log__release(&r->log, index, entries, m);

err_after_log_append:
    raft_log__truncate(&r->log, index);

err:
    assert(rv != 0);

    return rv;
}
