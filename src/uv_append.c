#include "assert.h"
#include "byte.h"
#include "heap.h"
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

/* An open segment being written or waiting to be written. */
struct uvOpenSegment
{
    struct uv *uv;                  /* Our writer */
    struct uvPrepare prepare;       /* Prepare segment file request */
    struct UvWriter writer;         /* Writer to perform async I/O */
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
    struct UvBarrier *barrier;      /* Barrier waiting on this segment */
    bool finalize;                  /* Finalize the segment after writing */
};

struct uvAppend
{
    struct raft_io_append *req;       /* User request */
    const struct raft_entry *entries; /* Entries to write */
    unsigned n;                       /* Number of entries */
    struct uvOpenSegment *segment;    /* Segment to write to */
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
    a->req = req;
    a->entries = entries;
    a->n = n;
    req->cb = cb;
}

static void uvOpenSegmentWriterCloseCb(struct UvWriter *writer)
{
    struct uvOpenSegment *segment = writer->data;
    struct uv *uv = segment->uv;
    uvSegmentBufferClose(&segment->pending);
    HeapFree(segment);
    uvMaybeFireCloseCb(uv);
}

/* Submit a request to close the current open segment. */
static void uvOpenSegmentFinalize(struct uvOpenSegment *s)
{
    struct uv *uv = s->uv;
    int rv;

    rv = uvFinalize(uv, s->counter, s->written, s->first_index, s->last_index);
    if (rv != 0) {
        uv->errored = true;
        /* We failed to submit the finalize request, but let's still close the
         * file handle and release the segment memory. */
    }

    QUEUE_REMOVE(&s->queue);
    UvWriterClose(&s->writer, uvOpenSegmentWriterCloseCb);
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
        HeapFree(r);
    }
}

/* Return the segment currently being written, or NULL when no segment has been
 * written yet. */
static struct uvOpenSegment *uvGetCurrentOpenSegment(struct uv *uv)
{
    queue *head;
    if (QUEUE_IS_EMPTY(&uv->append_segments)) {
        return NULL;
    }
    head = QUEUE_HEAD(&uv->append_segments);
    return QUEUE_DATA(head, struct uvOpenSegment, queue);
}

/* Extend the segment's write buffer by encoding the entries in the given
 * request into it. IOW, previous data in the write buffer will be retained, and
 * data for these new entries will be appended. */
static int uvOpenSegmentEncodeEntriesToWriteBuf(struct uvOpenSegment *s,
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

static void uvAppendProcessRequests(struct uv *uv);
static void uvOpenSegmentWriteCb(struct UvWriterReq *write, const int status)
{
    struct uvOpenSegment *s = write->data;
    struct uv *uv = s->uv;
    unsigned n_blocks;
    int result = 0;

    assert(uv->state != UV__CLOSED);

    assert(s->buf.len % uv->block_size == 0);
    assert(s->buf.len >= uv->block_size);

    /* Check if the write was successful. */
    if (status != 0) {
        assert(status != UV__CANCELED); /* We never cancel write requests */
        Tracef(uv->tracer, "write: %s", uv->io->errmsg);
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
static int uvOpenSegmentWrite(struct uvOpenSegment *s)
{
    int rv;
    assert(s->counter != 0);
    assert(s->pending.n > 0);
    uvSegmentBufferFinalize(&s->pending, &s->buf);
    rv =
        UvWriterSubmit(&s->writer, &s->write, &s->buf, 1,
                       s->next_block * s->uv->block_size, uvOpenSegmentWriteCb);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Process pending append requests.
 *
 * Submit the relevant write request if the target open segment is available. */
static void uvAppendProcessRequests(struct uv *uv)
{
    struct uvOpenSegment *segment;
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
        segment = uvGetCurrentOpenSegment(uv);
        /* If the current segment is not there anymore it means that was already
         * finalized. */
        if (segment == NULL) {
            return;
        }
        assert(segment != NULL);
        assert(segment->finalize);
    }

    /* If we are already writing, let's wait. */
    if (!QUEUE_IS_EMPTY(&uv->append_writing_reqs)) {
        return;
    }

prepare:
    segment = uvGetCurrentOpenSegment(uv);
    assert(segment != NULL);

    /* If the preparer isn't done yet, let's wait. */
    if (segment->counter == 0) {
        return;
    }

    /* If there's a barrier in progress, and it's not waiting for this segment
     * to be finalized, let's wait. */
    if (uv->barrier != NULL && segment->barrier != uv->barrier) {
        return;
    }

    /* If there's no barrier in progress and this segment is marked with a
     * barrier, it means that this was a pending barrier, which we can become
     * the current barrier now. */
    if (uv->barrier == NULL && segment->barrier != NULL) {
        uv->barrier = segment->barrier;
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
        rv = uvOpenSegmentEncodeEntriesToWriteBuf(segment, req);
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
            uvOpenSegmentFinalize(segment);
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

    rv = uvOpenSegmentWrite(segment);
    if (rv != 0) {
        goto err;
    }

    return;

err:
    uv->errored = true;
}

/* Invoked when a newly added open segment becomes ready for writing, after the
 * associated UvPrepare request completes (either synchronously or
 * asynchronously). */
static int uvOpenSegmentReady(struct uv *uv,
                              uv_file fd,
                              uvCounter counter,
                              struct uvOpenSegment *segment)
{
    int rv;
    rv = UvWriterInit(&segment->writer, uv->loop, fd, uv->direct_io,
                      uv->async_io, 1, uv->io->errmsg);
    if (rv != 0) {
        return rv;
    }
    segment->counter = counter;
    return 0;
}

static void uvOpenSegmentPrepareCb(struct uvPrepare *req, int status)
{
    struct uvOpenSegment *segment = req->data;
    struct uv *uv = segment->uv;
    int rv;

    assert(segment->counter == 0);
    assert(segment->written == 0);

    /* If we have been closed, let's discard the segment. */
    if (uv->closing) {
        QUEUE_REMOVE(&segment->queue);
        assert(status == RAFT_CANCELED); /* UvPrepare cancels pending reqs */
        uvSegmentBufferClose(&segment->pending);
        HeapFree(segment);
        return;
    }

    if (status != 0) {
        QUEUE_REMOVE(&segment->queue);
        HeapFree(segment);
        uv->errored = true;
        flushRequests(&uv->append_pending_reqs, RAFT_IOERR);
        return;
    }

    assert(req->counter > 0);
    assert(req->fd >= 0);

    rv = uvOpenSegmentReady(uv, req->fd, req->counter, segment);
    /* TODO: check for errors. */
    assert(rv == 0);

    uvAppendProcessRequests(uv);
}

/* Initialize a new open segment object. */
static void uvOpenSegmentInit(struct uvOpenSegment *s, struct uv *uv)
{
    s->uv = uv;
    s->prepare.data = s;
    s->writer.data = s;
    s->write.data = s;
    s->counter = 0;
    s->first_index = uv->append_next_index;
    s->last_index = s->first_index - 1;
    s->size = sizeof(uint64_t) /* Format version */;
    s->next_block = 0;
    uvSegmentBufferInit(&s->pending, uv->block_size);
    s->written = 0;
    s->barrier = NULL;
    s->finalize = false;
}

/* Add a new active open segment, since the append request being submitted does
 * not fit in the last segment we scheduled writes for, or no segment had been
 * previously requested at all. */
static int uvAppendPushOpenSegment(struct uv *uv)
{
    struct uvOpenSegment *segment;
    uv_file fd;
    uvCounter counter;
    int rv;

    segment = HeapMalloc(sizeof *segment);
    if (segment == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    uvOpenSegmentInit(segment, uv);

    QUEUE_PUSH(&uv->append_segments, &segment->queue);

    rv =
        UvPrepare(uv, &fd, &counter, &segment->prepare, uvOpenSegmentPrepareCb);
    if (rv != 0) {
        goto err_after_alloc;
    }

    /* If we've been returned a ready prepared segment right away, start writing
     * to it immediately. */
    if (fd != -1) {
        rv = uvOpenSegmentReady(uv, fd, counter, segment);
        if (rv != 0) {
            goto err_after_prepare;
        }
    }
    return 0;

err_after_prepare:
    UvOsClose(fd);
    uvFinalize(uv, counter, 0, 0, 0);
err_after_alloc:
    QUEUE_REMOVE(&segment->queue);
    HeapFree(segment);
err:
    assert(rv != 0);
    return rv;
}

/* Return the last segment that we have requested to prepare. */
static struct uvOpenSegment *uvGetLastOpenSegment(struct uv *uv)
{
    queue *tail;
    if (QUEUE_IS_EMPTY(&uv->append_segments)) {
        return NULL;
    }
    tail = QUEUE_TAIL(&uv->append_segments);
    return QUEUE_DATA(tail, struct uvOpenSegment, queue);
}

/* Return #true if the remaining capacity of the given segment is equal or
 * greater than @size. */
static bool uvOpenSegmentHasEnoughSpareCapacity(struct uvOpenSegment *s,
                                                size_t size)
{
    return s->size + size <= s->uv->segment_size;
}

/* Add @size bytes to the number of bytes that the segment will hold. The actual
 * write will happen when the previous write completes, if any. */
static void uvOpenSegmentReserveSegmentCapacity(struct uvOpenSegment *s,
                                                size_t size)
{
    s->size += size;
}

/* Return the number of bytes needed to store the batch of entries of this
 * append request on disk. */
static size_t uvAppendSize(struct uvAppend *a)
{
    size_t size = sizeof(uint32_t) * 2; /* CRC checksums */
    unsigned i;
    size += uvSizeofBatchHeader(a->n); /* Batch header */
    for (i = 0; i < a->n; i++) {       /* Entries data */
        size += bytePad64(a->entries[i].buf.len);
    }
    return size;
}

/* Enqueue an append entries request, assigning it to the appropriate active
 * open segment. */
static int uvAppendEnqueueRequest(struct uv *uv, struct uvAppend *append)
{
    struct uvOpenSegment *segment;
    size_t size;
    bool fits;
    int rv;

    assert(append->entries != NULL);
    assert(append->n > 0);
    assert(uv->append_next_index > 0);

    size = uvAppendSize(append);

    /* If we have no segments yet, it means this is the very first append, and
     * we need to add a new segment. Otherwise we check if the last segment has
     * enough room for this batch of entries. */
    segment = uvGetCurrentOpenSegment(uv);
    if (segment == NULL || segment->finalize) {
        fits = false;
    } else {
        fits = uvOpenSegmentHasEnoughSpareCapacity(segment, size);
        if (!fits) {
            segment->finalize = true; /* Finalize when all writes are done */
        }
    }

    /* If there's no segment or if this batch does not fit in this segment, we
     * need to add a new one. */
    if (!fits) {
        rv = uvAppendPushOpenSegment(uv);
        if (rv != 0) {
            goto err;
        }
    }

    segment = uvGetLastOpenSegment(uv); /* Get the last added segment */
    uvOpenSegmentReserveSegmentCapacity(segment, size);

    append->segment = segment;
    QUEUE_PUSH(&uv->append_pending_reqs, &append->queue);
    uv->append_next_index += append->n;

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int UvAppend(struct raft_io *io,
             struct raft_io_append *req,
             const struct raft_entry entries[],
             unsigned n,
             raft_io_append_cb cb)
{
    struct uv *uv;
    struct uvAppend *append;
    int rv;

    uv = io->impl;
    assert(!uv->closing);

    append = HeapMalloc(sizeof *append);
    if (append == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    uvAppendInit(append, req, entries, n, cb);

    rv = uvAppendEnqueueRequest(uv, append);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    assert(append->segment != NULL);

    /* If the requet's segment has already been prepared, try to write
     * immediately. */
    if (append->segment->counter != 0) {
        uvAppendProcessRequests(uv);
    }

    return 0;

err_after_req_alloc:
    HeapFree(append);
err:
    assert(rv != 0);
    return rv;
}

/* Finalize the current segment as soon as all its pending or inflight append
 * requests get completed. */
static void uvFinalizeCurrentOpenSegmentOnceIdle(struct uv *uv)
{
    struct uvOpenSegment *s;
    queue *head;
    bool has_pending_reqs;
    bool has_writing_reqs;

    s = uvGetCurrentOpenSegment(uv);
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
        uvOpenSegmentFinalize(s);
    } else {
        s->finalize = true;
    }
}

int UvBarrier(struct uv *uv,
              raft_index next_index,
              struct UvBarrier *barrier,
              UvBarrierCb cb)
{
    queue *head;

    assert(!uv->closing);

    /* The next entry will be appended at this index. */
    uv->append_next_index = next_index;

    /* Arrange for all open segments not already involved in other barriers to
     * be finalized as soon as their append requests get completed and mark them
     * as involved in this specific barrier request.  */
    QUEUE_FOREACH(head, &uv->append_segments)
    {
        struct uvOpenSegment *segment;
        segment = QUEUE_DATA(head, struct uvOpenSegment, queue);
        if (segment->barrier != NULL) {
            continue;
        }
        segment->barrier = barrier;
        if (segment == uvGetCurrentOpenSegment(uv)) {
            uvFinalizeCurrentOpenSegmentOnceIdle(uv);
            continue;
        }
        segment->finalize = true;
    }

    barrier->cb = cb;

    if (uv->barrier == NULL) {
        uv->barrier = barrier;
        /* If there's no pending append-related activity, we can fire the
         * callback immediately.
         *
         * TODO: find a way to avoid invoking this synchronously. */
        if (QUEUE_IS_EMPTY(&uv->append_segments) &&
            QUEUE_IS_EMPTY(&uv->finalize_reqs) &&
            uv->finalize_work.data == NULL) {
            barrier->cb(barrier);
        }
    }

    return 0;
}

void UvUnblock(struct uv *uv)
{
    assert(uv->barrier != NULL);
    uv->barrier = NULL;
    if (uv->closing) {
        uvMaybeFireCloseCb(uv);
        return;
    }
    uvAppendMaybeProcessRequests(uv);
}

int uvAppendForceFinalizingCurrentSegment(struct uv *uv)
{
    int rv;
    uvFinalizeCurrentOpenSegmentOnceIdle(uv);
    rv = uvAppendPushOpenSegment(uv);
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

/* Fire all pending barrier requests, the barrier callback will notice that
 * we're closing and abort there. */
static void uvBarrierClose(struct uv *uv)
{
    struct UvBarrier *barrier = NULL;
    queue *head;
    assert(uv->closing);
    QUEUE_FOREACH(head, &uv->append_segments)
    {
        struct uvOpenSegment *segment;
        segment = QUEUE_DATA(head, struct uvOpenSegment, queue);
        if (segment->barrier != NULL && segment->barrier != barrier) {
            barrier = segment->barrier;
            barrier->cb(barrier);
            if (segment->barrier == uv->barrier) {
                uv->barrier = NULL;
            }
        }
        segment->barrier = NULL;
    }

    /* There might still still be a current barrier set on uv->barrier, meaning
     * that the open segment it was associated with has started to be finalized
     * and is not anymore in the append_segments queue. Let's cancel that
     * too. */
    if (uv->barrier != NULL) {
        uv->barrier->cb(uv->barrier);
        uv->barrier = NULL;
    }
}

void uvAppendClose(struct uv *uv)
{
    struct uvOpenSegment *segment;
    assert(uv->closing);

    uvBarrierClose(uv);
    UvPrepareClose(uv);

    flushRequests(&uv->append_pending_reqs, RAFT_CANCELED);

    uvFinalizeCurrentOpenSegmentOnceIdle(uv);

    /* Also finalize the segments that we didn't write at all and are just
     * sitting in the append_segments queue waiting for writes against the
     * current segment to complete. */
    while (!QUEUE_IS_EMPTY(&uv->append_segments)) {
        segment = uvGetLastOpenSegment(uv);
        assert(segment != NULL);
        if (segment == uvGetCurrentOpenSegment(uv)) {
            break; /* We reached the head of the queue */
        }
        assert(segment->written == 0);
        uvOpenSegmentFinalize(segment);
    }
}
