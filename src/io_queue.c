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

void raft_io_queue__close(struct raft_io_queue *q)
{
    // size_t i;

    /* for (i = 0; i < q->size; i++) { */
    /*     struct raft_io_request *request = &q->requests[i]; */

    /*     /\* If this request slot is not in use, skip it. *\/ */
    /*     if (request->type == RAFT_IO_NULL) { */
    /*         continue; */
    /*     } */

    /*     if (request->leader_id == server_id) { */
    /*         /\* This request was submitted while we were in leader state. The
     */
    /*          * relevant entries were acquired from the log and need to be */
    /*          * released. *\/ */
    /*         assert(request->type == RAFT_IO_WRITE_LOG || */
    /*                request->type == RAFT_IO_APPEND_ENTRIES); */
    /*         raft_log__release(log, request->index, request->entries, */
    /*                           request->n); */
    /*     } else { */
    /*         /\* This request was submitted while we were in follower */
    /*          * state. The relevant entries were not acquired from the log */
    /*          * (they were received from the network), so we just need to */
    /*          * free the relevant memory. *\/ */
    /*         assert(request->type == RAFT_IO_WRITE_LOG); */
    /*         if (request->entries != NULL) { */
    /*             assert(request->entries[0].batch != NULL); */
    /*             raft_free(request->entries[0].batch); */
    /*             raft_free(request->entries); */
    /*         } */
    /*     } */
    /*     raft_io_queue__pop(q, i); */
    /* } */

    if (q->requests != NULL) {
        raft_free(q->requests);
    }
}

/**
 * Grow the queue so it has room for at least one more entry.
 */
static int raft_io_queue__grow(struct raft_io_queue *q)
{
    struct raft_io_request *requests;
    size_t size;
    size_t i;

    size = 2 * (q->size + 1); /* New queue size */

    requests = raft_realloc(q->requests, size * sizeof *requests);
    if (requests == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = q->size; i < size; i++) {
        struct raft_io_request *request = &requests[i];
        request->type = RAFT_IO_NULL;
    }

    q->requests = requests;
    q->size = size;

    return 0;
}

int raft_io_queue__push(struct raft_io_queue *q, unsigned *id)
{
    size_t i;
    int rv;

    /* First try to see if we have free slot. */
    for (i = 0; i < q->size; i++) {
        struct raft_io_request *request = &q->requests[i];
        if (request->type == RAFT_IO_NULL) {
            *id = i + 1;
            return 0;
        }
    }

    assert(i == q->size);

    /* We need to grow the queue. */
    rv = raft_io_queue__grow(q);
    if (rv != 0) {
        return rv;
    }

    *id = i + 1;

    return 0;
}

struct raft_io_request *raft_io_queue_get(const struct raft_io_queue *q,
                                          const unsigned id)
{
    size_t i;

    assert(q != NULL);
    assert(id > 0);

    i = id - 1;

    assert(i < q->size);

    return &q->requests[i];
}

void raft_io_queue__pop(struct raft_io_queue *q, unsigned id)
{
    size_t i = id - 1;

    assert(q != NULL);
    assert(i < q->size);
    assert(q->requests[i].type != RAFT_IO_NULL);

    q->requests[i].type = RAFT_IO_NULL;
}
