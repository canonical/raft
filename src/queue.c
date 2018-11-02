#include <assert.h>
#include <string.h>

#include "../include/raft.h"

#include "queue.h"
#include "log.h"

void raft_queue__close(struct raft *r)
{
    size_t i;

    for (i = 0; i < r->io_queue.size; i++) {
        struct raft_io_request *request = &r->io_queue.requests[i];

	/* If this request slot is not in use, skip it. */
        if (request->type == RAFT_IO_NULL) {
            continue;
        }

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
        raft_queue__pop(r, i);
    }

    if (r->io_queue.requests != NULL) {
        raft_free(r->io_queue.requests);
    }
}

/**
 * Grow the queue so it has room for at least one more entry.
 */
static int raft_queue__grow(struct raft *r)
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

int raft_queue__push(struct raft *r, size_t *id)
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
    rv = raft_queue__grow(r);
    if (rv != 0) {
        return rv;
    }

    *id = i;

    return 0;
}

struct raft_io_request *raft_queue__get(struct raft *r, size_t id)
{
    assert(r != NULL);
    assert(id < r->io_queue.size);

    return &r->io_queue.requests[id];
}

void raft_queue__pop(struct raft *r, size_t id)
{
    assert(r != NULL);
    assert(id < r->io_queue.size);
    assert(r->io_queue.requests[id].type != RAFT_IO_NULL);

    r->io_queue.requests[id].type = RAFT_IO_NULL;
}
