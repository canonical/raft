#include "assert.h"
#include "byte.h"
#include "queue.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_error.h"
#include "uv_writer.h"

/* The happy path for an append request is:
 *
 * - If there is a current segment and it is has enough spare capacity to hold
 *   the entries in the request, then queue the request, linking it to the
 *   current segment.
 *
 * - If there is no current segment, or it hasn't enough spare capacity to hold
 *   the entries in the request, then request a new open segment to be prepared,
 *   queue the request and link it to the newly requested segment.
 *
 * - Wait for any pending write against the current segment to complete, and
 *   also for the prepare request if we asked for a new segment.
 *
 * - Submit a write request for the entries in this append request. The write
 *   request might contain other entries that might have accumulated in the
 *   meantime.
 *
 * - Wait for the write request to finish and fire the append request's
 *   callback.
 *
 * Possible failure modes are:
 *
 * - The request to prepare a new segment fails.
 * - The write request fails.
 * - The request to finalize a new segment fails to be submitted.
 *
 * In all these cases we mark the instance as errored and fire the relevant
 * callbacks.
 **/

struct openSegment
{
    struct uv *uv;                  /* Our writer */
    struct uvPrepare prepare;       /* Prepare segment file request */
    struct UvWriter *writer;        /* Writer to perform async I/O */
    struct UvWriterReq write;       /* Write request */
    unsigned long long counter;     /* Open segment counter */
    raft_index first_index;         /* Index of the first entry written */
    raft_index last_index;          /* Index of the last entry written */
    size_t size;                    /* Total number of bytes used */
    unsigned next_block;            /* Next segment block to write */
    struct uvSegmentBuffer pending; /* Buffer for data yet to be written */
    uv_buf_t buf;                   /* Write buffer for current write */
    size_t written;                 /* Number of bytes actually written */
    queue queue;                    /* Segment queue */
    bool finalize;                  /* Finalize the segment after writing */
};

struct uvAppend
{
    struct raft_io_append *req;       /* User request */
    const struct raft_entry *entries; /* Entries to write */
    unsigned n;                       /* Number of entries */
    size_t size;                      /* Size of this batch on disk */
    struct openSegment *segment;      /* Segment to write to */
    int status;
    queue queue;
};

/* Initialize an append request object.
 *
 * In particular, calculate the number of bytes needed to store this batch in on
 * disk. */
static void uvAppendInit(struct uvAppend *a,
                         struct raft_io_append *req,
                         const struct raft_entry entries[],
                         unsigned n,
                         raft_io_append_cb cb)
{
    unsigned i;
    a->req = req;
    a->entries = entries;
    a->n = n;
    a->size = sizeof(uint32_t) * 2;       /* CRC checksums */
    a->size += uvSizeofBatchHeader(a->n); /* Batch header */
    for (i = 0; i < a->n; i++) {          /* Entries data */
        a->size += bytePad64(a->entries[i].buf.len);
    }
    req->cb = cb;
}

/* Return #true if the remaining capacity of the given segment is equal or
 * greater than @size. */
static bool openSegmentHasEnoughSpareCapacity(struct openSegment *s,
                                              size_t size)
{
    size_t cap = s->uv->block_size * uvSegmentBlocks(s->uv);
    return s->size + size <= cap;
}

/* Add @size bytes to the number of bytes that the segment will hold. The actual
 * write will happen when the previous write completes, if any. */
static void openSegmentReserveSegmentCapacity(struct openSegment *s,
                                              size_t size)
{
    s->size += size;
}

/* Extend the segment's write buffer by encoding the entries in the given
 * request into it. IOW, previous data in the write buffer will be retained, and
 * data for these new entries will be appended. */
static int openSegmentEncodeEntriesToWriteBuf(struct openSegment *s,
                                              struct uvAppend *req)
{
    int rv;
    assert(req->segment == s);

    /* If this is the very first write to the segment, we need to include the
     * format version */
    if (s->pending.n == 0 && s->next_block == 0) {
        rv = uvSegmentBufferFormat(&s->pending);
        if (rv != 0) {
            return rv;
        }
    }

    rv = uvSegmentBufferAppend(&s->pending, req->entries, req->n);
    if (rv != 0) {
        return rv;
    }

    s->last_index += req->n;

    return 0;
}

/* Submit a request to close the current open segment. */
static void finalizeSegment(struct openSegment *s)
{
    struct uv *uv = s->uv;
    int rv;

    rv = uvFinalize(uv, s->counter, s->written, s->first_index, s->last_index);
    if (rv != 0) {
        uv->errored = true;
        /* We failed to submit the finalize request, but let's still close the
         * file handle and release the segment memory. */
    }

    UvWriterClose(s->writer, (UvWriterCloseCb)raft_free);
    uvSegmentBufferClose(&s->pending);
    QUEUE_REMOVE(&s->queue);

    raft_free(s);
    uvMaybeClose(uv);
}

/* Flush the append requests in the given queue, firing their callbacks with the
 * given status. */
static void flushRequests(queue *q, int status)
{
    queue queue_copy;
    QUEUE_INIT(&queue_copy);
    while (!QUEUE_IS_EMPTY(q)) {
        queue *head;
        head = QUEUE_HEAD(q);
        QUEUE_REMOVE(head);
        QUEUE_PUSH(&queue_copy, head);
    }
    while (!QUEUE_IS_EMPTY(&queue_copy)) {
        struct uvAppend *r;
        queue *head;
        head = QUEUE_HEAD(&queue_copy);
        QUEUE_REMOVE(head);
        r = QUEUE_DATA(head, struct uvAppend, queue);
        r->req->cb(r->req, status);
        raft_free(r);
    }
}

static void uvAppendProcessRequests(struct uv *uv);
static void writeSegmentCb(struct UvWriterReq *write, const int status)
{
    struct openSegment *s = write->data;
    struct uv *uv = s->uv;
    unsigned n_blocks;
    int result = 0;

    assert(uv->state != UV__CLOSED);

    assert(s->buf.len % uv->block_size == 0);
    assert(s->buf.len >= uv->block_size);

    /* Check if the write was successful. */
    if (status != 0) {
        assert(status != UV__CANCELED); /* We never cancel write requests */
        uvErrorf(uv, "write: %s", ErrMsgString(&uv->errmsg));
        result = RAFT_IOERR;
        uv->errored = true;
    }

    s->written = s->next_block * uv->block_size + s->pending.n;

    /* Update our write markers.
     *
     * We have four cases:
     *
     * - The data fit completely in the leftover space of the first block that
     *   we wrote and there is more space left. In this case we just keep the
     *   scheduled marker unchanged.
     *
     * - The data fit completely in the leftover space of the first block that
     *   we wrote and there is no space left. In this case we advance the
     *   current block counter, reset the first write block and set the
     *   scheduled marker to 0.
     *
     * - The data did not fit completely in the leftover space of the first
     *   block that we wrote, so we wrote more than one block. The last block
     *   that we wrote was not filled completely and has leftover space. In this
     *   case we advance the current block counter and copy the memory used for
     *   the last block to the head of the write arena list, updating the
     *   scheduled marker accordingly.
     *
     * - The data did not fit completely in the leftover space of the first
     *   block that we wrote, so we wrote more than one block. The last block
     *   that we wrote was filled exactly and has no leftover space. In this
     *   case we advance the current block counter, reset the first buffer and
     *   set the scheduled marker to 0.
     */
    n_blocks = s->buf.len / uv->block_size; /* Number of blocks written. */
    if (s->pending.n < uv->block_size) {
        /* Nothing to do */
        assert(n_blocks == 1);
    } else if (s->pending.n == uv->block_size) {
        assert(n_blocks == 1);
        s->next_block++;
        uvSegmentBufferReset(&s->pending, 0);
    } else {
        assert(s->pending.n > uv->block_size);
        assert(s->buf.len > uv->block_size);

        if (s->pending.n % uv->block_size > 0) {
            s->next_block += n_blocks - 1;
            uvSegmentBufferReset(&s->pending, n_blocks - 1);
        } else {
            s->next_block += n_blocks;
            uvSegmentBufferReset(&s->pending, 0);
        }
    }

    /* Fire the callbacks of all requests that were fulfilled with this
     * write. */
    flushRequests(&uv->append_writing_reqs, result);

    /* Possibly process waiting requests. */
    uvAppendProcessRequests(uv);
}

/* Submit a file write request to append the entries encoded in the write buffer
 * of the given segment. */
static int writeSegment(struct openSegment *s)
{
    int rv;
    assert(s->writer != NULL);
    assert(s->pending.n > 0);
    uvSegmentBufferFinalize(&s->pending, &s->buf);
    rv = UvWriterSubmit(s->writer, &s->write, &s->buf, 1,
                        s->next_block * s->uv->block_size, writeSegmentCb);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Return the segment currently being written, or NULL when no segment has been
 * written yet. */
static struct openSegment *getCurrentOpenSegment(struct uv *uv)
{
    queue *head;
    if (QUEUE_IS_EMPTY(&uv->append_segments)) {
        return NULL;
    }
    head = QUEUE_HEAD(&uv->append_segments);
    return QUEUE_DATA(head, struct openSegment, queue);
}

/* Process pending append requests.
 *
 * Submit the relevant write request if the target open segment is available. */
static void uvAppendProcessRequests(struct uv *uv)
{
    struct openSegment *segment;
    struct uvAppend *req;
    queue *head;
    queue q;
    unsigned n_reqs;
    int rv;

    /* During the closing sequence we should only get called by the
     * segmentWriteCb callback after an in-flight write has been completed. */
    if (uv->closing) {
        assert(QUEUE_IS_EMPTY(&uv->append_pending_reqs));
        assert(QUEUE_IS_EMPTY(&uv->append_writing_reqs));
        segment = getCurrentOpenSegment(uv);
        assert(segment != NULL);
        assert(segment->finalize);
    }

    /* If we are already writing, let's wait. */
    if (!QUEUE_IS_EMPTY(&uv->append_writing_reqs)) {
        return;
    }

    /* If we're truncating, let's wait. */
    if (uv->truncate_work.data != NULL) {
        return;
    }

prepare:
    segment = getCurrentOpenSegment(uv);
    assert(segment != NULL);

    /* If the preparer hasn't provided the segment yet, let's wait. */
    if (segment->writer == NULL) {
        return;
    }

    /* Let's add to the segment's write buffer all pending requests targeted to
     * this segment. */
    QUEUE_INIT(&q);

    n_reqs = 0;
    while (!QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
        head = QUEUE_HEAD(&uv->append_pending_reqs);
        req = QUEUE_DATA(head, struct uvAppend, queue);
        assert(req->segment != NULL);
        if (req->segment != segment) {
            break; /* Not targeted to this segment */
        }
        QUEUE_REMOVE(head);
        QUEUE_PUSH(&q, head);
        n_reqs++;
        rv = openSegmentEncodeEntriesToWriteBuf(segment, req);
        if (rv != 0) {
            goto err;
        }
    }

    /* If we have no more requests for this segment, let's check if it has been
     * marked for closing, and in that case finalize it and possibly trigger a
     * write against the next segment (unless there is a truncate request, in
     * that case we need to wait for it). Otherwise it must mean we have
     * exhausted the queue of pending append requests. */
    if (n_reqs == 0) {
        assert(QUEUE_IS_EMPTY(&uv->append_writing_reqs));
        if (segment->finalize) {
            finalizeSegment(segment);
            if (!QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
                return;
            }
            if (!QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
                goto prepare;
            }
        }
        assert(QUEUE_IS_EMPTY(&uv->append_pending_reqs));
        return;
    }

    while (!QUEUE_IS_EMPTY(&q)) {
        head = QUEUE_HEAD(&q);
        QUEUE_REMOVE(head);
        QUEUE_PUSH(&uv->append_writing_reqs, head);
    }

    rv = writeSegment(segment);
    if (rv != 0) {
        goto err;
    }

    return;

err:
    uv->errored = true;
}

static void appendPrepareCb(struct uvPrepare *req, int status)
{
    struct openSegment *segment = req->data;
    struct uv *uv = segment->uv;
    int rv;

    /* If we have been closed, let's discard the segment. */
    if (uv->closing) {
        QUEUE_REMOVE(&segment->queue);
        if (status == 0) {
            UvOsClose(req->fd);
            /* Ignore errors, as there's nothing we can do about it. */
            uvFinalize(uv, req->counter, 0, 0, 0);
        }
        uvSegmentBufferClose(&segment->pending);
        raft_free(segment);
        return;
    }

    if (status != 0) {
        QUEUE_REMOVE(&segment->queue);
        raft_free(segment);
        uv->errored = true;
        flushRequests(&uv->append_writing_reqs, RAFT_IOERR);
        return;
    }

    assert(req->counter > 0);
    assert(req->fd >= 0);

    /* TODO: check for errors. */
    segment->writer = raft_malloc(sizeof *segment->writer);
    assert(segment->writer != NULL);
    rv = UvWriterInit(segment->writer, uv->loop, req->fd, uv->direct_io,
                      uv->async_io, 1, &uv->errmsg);
    assert(rv == 0);

    segment->counter = req->counter;
    uvAppendProcessRequests(uv);
}

/* Initialize a new open segment object. */
static void openSegmentInit(struct openSegment *s, struct uv *uv)
{
    s->uv = uv;
    s->prepare.data = s;
    s->write.data = s;
    s->counter = 0;
    s->writer = NULL;
    s->first_index = uv->append_next_index;
    s->last_index = s->first_index - 1;
    s->size = sizeof(uint64_t) /* Format version */;
    s->next_block = 0;
    uvSegmentBufferInit(&s->pending, uv->block_size);
    s->written = 0;
    s->finalize = false;
}

/* Submit a prepare request in order to get a new segment, since the append
 * request being submitted does not fit in the last segment we scheduled writes
 * for, or no segment had been previously requested at all. */
static int submitPrepareSegmentRequest(struct uv *uv)
{
    struct openSegment *segment;
    int rv;
    segment = raft_malloc(sizeof *segment);
    if (segment == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    openSegmentInit(segment, uv);
    QUEUE_PUSH(&uv->append_segments, &segment->queue);
    uvPrepare(uv, &segment->prepare, appendPrepareCb);
    return 0;
err:
    assert(rv != 0);
    return rv;
}

/* Return the last segment that we have requested to prepare. */
static struct openSegment *getLastOpenSegment(struct uv *uv)
{
    queue *tail;
    if (QUEUE_IS_EMPTY(&uv->append_segments)) {
        return NULL;
    }
    tail = QUEUE_TAIL(&uv->append_segments);
    return QUEUE_DATA(tail, struct openSegment, queue);
}

void uvAppendFixPreparedSegmentFirstIndex(struct uv *uv)
{
    struct openSegment *s = getLastOpenSegment(uv);
    if (s == NULL) {
        /* Must be the first snapshot.
         *
         * TODO: verify assumption. */
        return;
    }
    assert(s->first_index == 1);
    assert(s->last_index == 0);
    assert(s->size == sizeof(uint64_t));
    assert(s->next_block == 0);
    assert(s->written == 0);
    s->first_index = uv->append_next_index;
    s->last_index = s->first_index - 1;
}

/* Enqueue an append entries request */
static int uvAppendEnqueueRequest(struct uv *uv, struct uvAppend *append)
{
    struct openSegment *segment;
    bool fits;
    int rv;

    assert(append->entries != NULL);
    assert(append->n > 0);
    assert(uv->append_next_index > 0);

    /* If we have no segments yet, it means this is the very first append, and
     * we need to add a new segment. Otherwise we check if the last segment has
     * enough room for this batch of entries. */
    segment = getCurrentOpenSegment(uv);
    if (segment == NULL) {
        fits = false;
    } else {
        fits = openSegmentHasEnoughSpareCapacity(segment, append->size);
        if (!fits) {
            segment->finalize = true; /* Finalize when all writes are done */
        }
    }

    /* If there's no segment or if this batch does not fit in this segment, we
     * need to add a new one. */
    if (!fits) {
        rv = submitPrepareSegmentRequest(uv);
        if (rv != 0) {
            goto err;
        }
    }

    segment = getLastOpenSegment(uv); /* Get the last added segment */
    openSegmentReserveSegmentCapacity(segment, append->size);

    append->segment = segment;
    QUEUE_PUSH(&uv->append_pending_reqs, &append->queue);
    uv->append_next_index += append->n;

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int uvAppend(struct raft_io *io,
             struct raft_io_append *req,
             const struct raft_entry entries[],
             unsigned n,
             raft_io_append_cb cb)
{
    struct uv *uv;
    struct uvAppend *append;
    int rv;

    uv = io->impl;

    rv = uvMaybeInitialize(uv);
    if (rv != 0) {
        return rv;
    }

    append = raft_malloc(sizeof *append);
    if (append == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    uvAppendInit(append, req, entries, n, cb);

    rv = uvAppendEnqueueRequest(uv, append);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    uvAppendProcessRequests(uv);

    return 0;

err_after_req_alloc:
    raft_free(append);
err:
    assert(rv != 0);
    return rv;
}

/* Start finalizing the current segment, if any. */
static void finalizeCurrentSegment(struct uv *uv)
{
    struct openSegment *s;
    queue *head;
    bool has_pending_reqs;
    bool has_writing_reqs;

    s = getCurrentOpenSegment(uv);
    if (s == NULL) {
        return;
    }

    /* Check if there are pending append requests targeted to the current
     * segment. */
    has_pending_reqs = false;
    QUEUE_FOREACH(head, &uv->append_pending_reqs)
    {
        struct uvAppend *r = QUEUE_DATA(head, struct uvAppend, queue);
        if (r->segment == s) {
            has_pending_reqs = true;
            break;
        }
    }
    has_writing_reqs = !QUEUE_IS_EMPTY(&uv->append_writing_reqs);

    /* If there is no pending append request or inflight write against the
     * current segment, we can submit a request for it to be closed
     * immediately. Otherwise, we set the finalize flag.
     *
     * TODO: is it actually possible to have pending requests with no writing
     * requests? Probably no. */
    if (!has_pending_reqs && !has_writing_reqs) {
        finalizeSegment(s);
    } else {
        s->finalize = true;
    }
}

int uvAppendForceFinalizingCurrentSegment(struct uv *uv)
{
    int rv;
    finalizeCurrentSegment(uv);
    rv = submitPrepareSegmentRequest(uv);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

void uvAppendMaybeProcessRequests(struct uv *uv)
{
    if (!QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
        uvAppendProcessRequests(uv);
    }
}

void uvAppendClose(struct uv *uv)
{
    struct openSegment *s;

    flushRequests(&uv->append_pending_reqs, RAFT_CANCELED);
    finalizeCurrentSegment(uv);

    /* Also finalize the segments that we didn't write at all and are just
     * sitting in the append_segments queue waiting for writes against the
     * current segment to complete. */
    while (!QUEUE_IS_EMPTY(&uv->append_segments)) {
        s = getLastOpenSegment(uv);
        assert(s != NULL);
        if (s == getCurrentOpenSegment(uv)) {
            break; /* We reached the head of the queue */
        }
        assert(s->written == 0);
        finalizeSegment(s);
    }
}
