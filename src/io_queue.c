#include <assert.h>
#include <string.h>

#include "io_queue.h"
#include "log.h"

void raft_io_queue__init(struct raft_io_queue *q)
{
    assert(q != NULL);

    q->requests = NULL;
    q->size = 0;
}

void raft_io_queue__close_(struct raft *r)
{
    size_t i;

    for (i = 0; i < r->io_.queue.size; i++) {
        struct raft_io_request *request = &r->io_.queue.requests[i];

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
        raft_io_queue__pop(r, i);
    }

    if (r->io_.queue.requests != NULL) {
        raft_free(r->io_.queue.requests);
    }
}

/**
 * Grow the queue so it has room for at least one more entry.
 */
static int raft_io_queue__grow(struct raft *r)
{
    struct raft_io_request *requests;
    size_t size;
    size_t i;

    size = 2 * (r->io_.queue.size + 1); /* New queue size */

    requests = raft_realloc(r->io_.queue.requests, size * sizeof *requests);
    if (requests == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = r->io_.queue.size; i < size; i++) {
        struct raft_io_request *request = &requests[i];
        request->type = RAFT_IO_NULL;
    }

    r->io_.queue.requests = requests;
    r->io_.queue.size = size;

    return 0;
}

int raft_io_queue__push(struct raft *r, unsigned *id)
{
    size_t i;
    int rv;

    /* First try to see if we have free slot. */
    for (i = 0; i < r->io_.queue.size; i++) {
        struct raft_io_request *request = &r->io_.queue.requests[i];
        if (request->type == RAFT_IO_NULL) {
            *id = i + 1;
            return 0;
        }
    }

    assert(i == r->io_.queue.size);

    /* We need to grow the queue. */
    rv = raft_io_queue__grow(r);
    if (rv != 0) {
        return rv;
    }

    *id = i + 1;

    return 0;
}

struct raft_io_request *raft_io_queue_get_(struct raft *r, unsigned id)
{
    size_t i;

    assert(r != NULL);
    assert(id > 0);

    i = id - 1;

    assert(i < r->io_.queue.size);

    return &r->io_.queue.requests[i];
}

void raft_io_queue__pop(struct raft *r, unsigned id)
{
    size_t i = id - 1;

    assert(r != NULL);
    assert(i < r->io_.queue.size);
    assert(r->io_.queue.requests[i].type != RAFT_IO_NULL);

    r->io_.queue.requests[i].type = RAFT_IO_NULL;
}
