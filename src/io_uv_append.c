#include <stdlib.h>
#include <string.h>

#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv.h"
#include "io_uv_encoding.h"
#include "queue.h"

/* The happy path for an append request is:
 *
 * - If there is a current segment and it is has enough spare capacity to hold
 *   the entries in the request, then queue the request, linking it to the
 *   current segment.
 *
 * - If there is no current segment, or it hasn't enough spare capacity to hold
 *   the entries in the request, the request a new open segment to be prepared,
 *   and link, then queue the request and link it to the newly requested
 *segment.
 *
 * - Wait for any pending write against the current segment to complete, and
 *also for the prepare request if we asked for a new segment.
 *
 * - Submit a write request for the entries in this append request. The write
 *   request might contain other entries that might have accumulated in the
 *   meantime.
 *
 * - Wait for the write request to finish and fire the append request's
 *callback.
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

struct segment
{
    struct io_uv *uv;              /* Our writer */
    struct io_uv__prepare prepare; /* Prepare segment file request */
    struct uv__file *file;         /* File to write to */
    struct uv__file_write write;   /* Write request */
    unsigned long long counter;    /* Open segment counter */
    raft_index first_index;        /* Index of the first entry written */
    raft_index last_index;         /* Index of the last entry written */
    size_t size;                   /* Total number of bytes used */
    unsigned next_block;           /* Next segment block to write */
    uv_buf_t arena;                /* Memory for the write buffer */
    uv_buf_t buf;                  /* Write buffer for current write */
    size_t scheduled;              /* Number of bytes of the next write */
    size_t written;                /* Number of bytes actually written */
    raft__queue queue;             /* Segment queue */
    bool finalize;                 /* Finalize the segment after writing */
};

struct append
{
    void *data;                       /* User data */
    const struct raft_entry *entries; /* Entries to write */
    unsigned n;                       /* Number of entries */
    size_t size;                      /* Size of this batch on disk */
    struct segment *segment;          /* Segment to write to */
    int status;
    void (*cb)(void *data, int status);
    raft__queue queue;
};

/* Initialize an append request object. In particular, calculate the number of
 * bytes needed to store this batch in on disk. */
static void init_append_req(struct append *r,
                            const struct raft_entry entries[],
                            unsigned n,
                            void *data,
                            void (*cb)(void *data, int status))
{
    unsigned i;

    r->entries = entries;
    r->n = n;
    r->data = data;
    r->cb = cb;

    r->size = sizeof(uint32_t) * 2;              /* CRC checksums */
    r->size += io_uv__sizeof_batch_header(r->n); /* Batch header */
    for (i = 0; i < r->n; i++) {                 /* Entries data */
        size_t len = r->entries[i].buf.len;
        r->size += len;
        if (len % 8 != 0) {
            r->size += 8 - (len % 8); /* Add padding */
        }
    }
}

/* Return #true if the remaining capacity of the given segment is equal or
 * greater than @size. */
static bool segment_has_enough_spare_capacity(struct segment *s, size_t size)
{
    size_t cap = s->uv->block_size * s->uv->n_blocks;
    return s->size + size <= cap;
}

/* Add @size bytes to the number of bytes that the segment will hold. The actual
 * write will happen when the previous write completes, if any. */
static void reserve_segment_capacity(struct segment *s, size_t size)
{
    assert(segment_has_enough_spare_capacity(s, size));
    s->size += size;
}

/* Ensure that the write buffer of the given segment is large enough to hold the
 * the given number of bytes size. */
static int ensure_segment_write_buf_is_large_enough(struct segment *s,
                                                    size_t size)
{
    unsigned n = (size / s->uv->block_size);
    void *base;
    size_t len;

    if (s->arena.len >= size) {
        assert(s->arena.base != NULL);
        return 0;
    }

    if (size % s->uv->block_size != 0) {
        n++;
    }

    len = s->uv->block_size * n;
    base = aligned_alloc(s->uv->block_size, len);
    if (base == NULL) {
        return RAFT_ENOMEM;
    }
    memset(base, 0, len);

    /* If the current arena is initialized, we need to copy its first block,
     * since it might have data that we want to retain in the next write. */
    if (s->arena.base != NULL) {
        assert(s->arena.len >= s->uv->block_size);
        memcpy(base, s->arena.base, s->arena.len);
        free(s->arena.base);
    }

    s->arena.base = base;
    s->arena.len = len;

    return 0;
}

/* Extend the segment's write buffer by encoding the entries in the given
 * request into it. IOW, previous data in the write buffeer will be retained,
 * and data for these new entries will be appendede. */
static int encode_entries_to_segment_write_buf(struct segment *s,
                                               struct append *req)
{
    size_t size;
    void *cursor;
    unsigned crc1; /* Header checksum */
    unsigned crc2; /* Data checksum */
    void *crc1_p;  /* Pointer to header checksum slot */
    void *crc2_p;  /* Pointer to data checksum slot */
    void *header;  /* Pointer to the header section */
    unsigned i;
    int rv;

    assert(req->segment == s);

    size = req->size;

    /* If this is the very first write to the segment, we need to include the
     * format version */
    if (s->scheduled == 0 && s->next_block == 0) {
        size += sizeof(uint64_t);
    }

    assert(s->written + size <= s->uv->block_size * s->uv->n_blocks);

    rv = ensure_segment_write_buf_is_large_enough(s, s->scheduled + size);
    if (rv != 0) {
        return rv;
    }

    cursor = s->arena.base + s->scheduled;

    if (s->scheduled == 0 && s->next_block == 0) {
        byte__put64(&cursor, IO_UV__DISK_FORMAT);
    }

    /* Placeholder of the checksums */
    crc1_p = cursor;
    byte__put32(&cursor, 0);
    crc2_p = cursor;
    byte__put32(&cursor, 0);

    /* Batch header */
    header = cursor;
    io_uv__encode_batch_header(req->entries, req->n, cursor);
    crc1 = byte__crc32(header, io_uv__sizeof_batch_header(req->n), 0);
    cursor += io_uv__sizeof_batch_header(req->n);

    /* Batch data */
    crc2 = 0;
    for (i = 0; i < req->n; i++) {
        const struct raft_entry *entry = &req->entries[i];

        /* TODO: enforce the requirment of 8-byte aligment also in the
         * higher-level APIs. */
        assert(entry->buf.len % sizeof(uint64_t) == 0);

        memcpy(cursor, entry->buf.base, entry->buf.len);
        crc2 = byte__crc32(cursor, entry->buf.len, crc2);

        cursor += entry->buf.len;
    }

    byte__put32(&crc1_p, crc1);
    byte__put32(&crc2_p, crc2);

    s->scheduled += size;
    s->last_index += req->n;

    return 0;
}

/* Submit a request to close the current open segment. */
static void finalize_segment(struct segment *s)
{
    struct io_uv *uv = s->uv;
    int rv;

    rv = io_uv__finalize(uv, s->counter, s->written, s->first_index,
                         s->last_index);
    if (rv != 0) {
        uv->errored = true;
        /* We failed to submit the finalize request, but let's still close the
         * file handle and release the segment memory. */
    }

    uv__file_close(s->file, (uv__file_close_cb)raft_free);

    if (s->arena.base != NULL) {
        free(s->arena.base);
    }
    RAFT__QUEUE_REMOVE(&s->queue);

    raft_free(s);
    io_uv__maybe_close(uv);
}

/* Flush the append requests in the given queue, firing their callbacks with the
 * given status. */
static void flush_append_requests(raft__queue *queue, int status)
{
    while (!RAFT__QUEUE_IS_EMPTY(queue)) {
        struct append *r;
        raft__queue *head;
        head = RAFT__QUEUE_HEAD(queue);
        RAFT__QUEUE_REMOVE(head);
        r = RAFT__QUEUE_DATA(head, struct append, queue);
        r->cb(r->data, status);
        raft_free(r);
    }
}

static void process_requests(struct io_uv *uv);
static void segment_write_cb(struct uv__file_write *write, const int status)
{
    struct segment *s = write->data;
    struct io_uv *uv = s->uv;
    unsigned n_blocks;
    int result = 0;

    assert(uv->state != IO_UV__CLOSED);

    assert(s->buf.len % uv->block_size == 0);
    assert(s->buf.len >= uv->block_size);

    /* Check if the write was successful. */
    if (status != (int)s->buf.len) {
        assert(status != UV_ECANCELED); /* We never cancel write requests */
        if (status < 0) {
            raft_errorf(uv->logger, "write: %s", uv_strerror(status));
        } else {
            raft_errorf(uv->logger, "only %d bytes written", status);
        }
        result = RAFT_ERR_IO;
        uv->errored = true;
    }

    s->written = s->next_block * uv->block_size + s->scheduled;

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
    n_blocks = s->buf.len / uv->block_size; /* Number of blocks writen. */
    if (s->scheduled < uv->block_size) {
        /* Nothing to do */
        assert(n_blocks == 1);
    } else if (s->scheduled == uv->block_size) {
        assert(n_blocks == 1);
        s->next_block++;
        s->scheduled = 0;
        memset(s->arena.base, 0, uv->block_size);
    } else {
        assert(s->scheduled > uv->block_size);
        assert(s->buf.len > uv->block_size);

        if (s->scheduled % uv->block_size > 0) {
            s->next_block += n_blocks - 1;
            s->scheduled = s->scheduled % uv->block_size;
            memcpy(s->arena.base,
                   s->arena.base + (n_blocks - 1) * uv->block_size,
                   uv->block_size);
        } else {
            s->next_block += n_blocks;
            s->scheduled = 0;
            memset(s->arena.base, 0, uv->block_size);
        }
    }

    /* Fire the callbacks of all requests that were fulfilled with this
     * write. */
    flush_append_requests(&uv->append_writing_reqs, result);

    process_requests(uv);
}

/* Submit a file write request to append the entries encoded in the write buffer
 * of the given segment. */
static int segment_write(struct segment *s)
{
    unsigned n_blocks;
    int rv;

    assert(s->file != NULL);
    assert(s->arena.base != NULL);

    n_blocks = s->scheduled / s->uv->block_size;
    if (s->scheduled % s->uv->block_size != 0) {
        n_blocks++;
    }

    s->buf.base = s->arena.base;
    s->buf.len = n_blocks * s->uv->block_size;

    /* Set the remainder of the last block to 0 */
    if (s->scheduled % s->uv->block_size != 0) {
        memset(s->buf.base + s->scheduled, 0,
               s->uv->block_size - (s->scheduled % s->uv->block_size));
    }

    rv = uv__file_write(s->file, &s->write, &s->buf, 1,
                        s->next_block * s->uv->block_size, segment_write_cb);

    if (rv != 0) {
        return rv;
    }

    return 0;
}

/* Process pending append requests.
 *
 * Submit the relevant write request if the target open segment is available. */
static void process_requests(struct io_uv *uv)
{
    struct segment *segment;
    struct append *req;
    raft__queue *head;
    raft__queue queue;
    unsigned n_reqs;
    int rv;

    /* If we are already writing, let's wait. */
    if (!RAFT__QUEUE_IS_EMPTY(&uv->append_writing_reqs)) {
        return;
    }

    /* If we're truncating, let's wait. */
    if (uv->truncate_work.data != NULL) {
        return;
    }

prepare:
    assert(!RAFT__QUEUE_IS_EMPTY(&uv->append_segments));

    /* Get the open segment with lowest counter, iow the current one. */
    head = RAFT__QUEUE_HEAD(&uv->append_segments);
    segment = RAFT__QUEUE_DATA(head, struct segment, queue);

    /* If the preparer hasn't provided the segment yet, let's wait. */
    if (segment->file == NULL) {
        return;
    }

    /* Let's add to the segment's write buffer all pending requests targeted to
     * this segment. */
    RAFT__QUEUE_INIT(&queue);

    n_reqs = 0;
    while (!RAFT__QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
        head = RAFT__QUEUE_HEAD(&uv->append_pending_reqs);
        req = RAFT__QUEUE_DATA(head, struct append, queue);
        assert(req->segment != NULL);
        if (req->segment != segment) {
            break; /* Not targeted to this segment */
        }

        RAFT__QUEUE_REMOVE(head);
        RAFT__QUEUE_PUSH(&queue, head);

        n_reqs++;

        rv = encode_entries_to_segment_write_buf(segment, req);
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
        assert(RAFT__QUEUE_IS_EMPTY(&uv->append_writing_reqs));
        if (segment->finalize) {
            finalize_segment(segment);
            if (!RAFT__QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
                return;
            }
            if (!RAFT__QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
                goto prepare;
            }
        }
        assert(RAFT__QUEUE_IS_EMPTY(&uv->append_pending_reqs));
        return;
    }

    while (!RAFT__QUEUE_IS_EMPTY(&queue)) {
        head = RAFT__QUEUE_HEAD(&queue);
        RAFT__QUEUE_REMOVE(head);
        RAFT__QUEUE_PUSH(&uv->append_writing_reqs, head);
    }

    rv = segment_write(segment);
    if (rv != 0) {
        goto err;
    }

    return;

err:
    uv->errored = true;
}

static void prepare_segment_cb(struct io_uv__prepare *req,
                               struct uv__file *file,
                               unsigned long long counter,
                               int status)
{
    struct segment *segment = req->data;
    struct io_uv *uv = segment->uv;

    /* If we have been closed, let's discard the segment. */
    if (uv->state == IO_UV__CLOSING) {
        RAFT__QUEUE_REMOVE(&segment->queue);

        if (status == 0) {
            uv__file_close(file, (uv__file_close_cb)raft_free);

            /* Ignore errors, as there's nothing we can do about it. */
            io_uv__finalize(uv, counter, 0, 0, 0);
        }

        assert(segment->arena.base == NULL);
        raft_free(segment);
        return;
    }

    if (status != 0) {
        RAFT__QUEUE_REMOVE(&segment->queue);
        raft_free(segment);
        uv->errored = true;
        flush_append_requests(&uv->append_writing_reqs, RAFT_ERR_IO);
        return;
    }

    assert(counter > 0);
    assert(file != NULL);

    segment->file = file;
    segment->counter = counter;

    process_requests(uv);
}

/* Initialize a new open segment object. */
static void init_segment(struct segment *s, struct io_uv *uv)
{
    s->uv = uv;
    s->prepare.data = s;
    s->write.data = s;
    s->counter = 0;
    s->file = NULL;
    s->first_index = uv->append_next_index;
    s->last_index = s->first_index - 1;
    s->size = sizeof(uint64_t) /* Format version */;
    s->next_block = 0;
    s->arena.base = NULL;
    s->arena.len = 0;
    s->scheduled = 0;
    s->written = 0;
    s->finalize = false;
}

/* Submit a prepare request in order to get a new segment, since the append
 * request being submitted does not fit in the last segment we scheduled writes
 * for. */
static int prepare_segment(struct io_uv *uv)
{
    struct segment *segment;
    int rv;

    segment = raft_malloc(sizeof *segment);
    if (segment == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    init_segment(segment, uv);
    RAFT__QUEUE_PUSH(&uv->append_segments, &segment->queue);
    io_uv__prepare(uv, &segment->prepare, prepare_segment_cb);

    return 0;

err:
    assert(rv != 0);
    return rv;
}

/* Enqueue an append entries request */
static int enqueue_append_request(struct io_uv *uv, struct append *req)
{
    struct segment *segment;
    raft__queue *tail;
    bool fits;
    int rv;

    assert(uv->state == IO_UV__ACTIVE);

    assert(req->entries != NULL);
    assert(req->n > 0);
    assert(uv->append_next_index > 0);

    /* TODO: at the moment we don't allow a single batch to exceed the size of a
     * segment. */
    if (req->size > uv->block_size * uv->n_blocks + sizeof(uint64_t)) {
        return RAFT_ERR_IO_TOOBIG;
    }

    /* If we have no segments yet, it means this is the very first append, and
     * we need to add a new segment. Otherwise we check if the last segment has
     * enough room for this batch of entries. */
    if (RAFT__QUEUE_IS_EMPTY(&uv->append_segments)) {
        fits = false;
    } else {
        tail = RAFT__QUEUE_HEAD(&uv->append_segments);
        segment = RAFT__QUEUE_DATA(tail, struct segment, queue);
        fits = segment_has_enough_spare_capacity(segment, req->size);
        if (!fits) {
            segment->finalize = true; /* Finalize when all writes are done */
        }
    }

    /* If there's no segment or if this batch does not fit in this segment, we
     * need to add a new one. */
    if (!fits) {
        rv = prepare_segment(uv);
        if (rv != 0) {
            goto err;
        }
    }

    /* Get the last added segment */
    tail = RAFT__QUEUE_TAIL(&uv->append_segments);
    segment = RAFT__QUEUE_DATA(tail, struct segment, queue);

    reserve_segment_capacity(segment, req->size);

    req->segment = segment;

    RAFT__QUEUE_PUSH(&uv->append_pending_reqs, &req->queue);

    uv->append_next_index += req->n;

    process_requests(uv);

    return 0;

err:
    assert(rv != 0);

    return rv;
}

int io_uv__append(struct raft_io *io,
                  const struct raft_entry entries[],
                  unsigned n,
                  void *data,
                  void (*cb)(void *data, int status))
{
    struct io_uv *uv;
    struct append *req;
    int rv;

    uv = io->impl;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    init_append_req(req, entries, n, data, cb);

    rv = enqueue_append_request(uv, req);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    return 0;

err_after_req_alloc:
    raft_free(req);

err:
    assert(rv != 0);
    return rv;
}

static void finalize_current_segment(struct io_uv *uv)
{
    struct segment *s;
    raft__queue *head;
    bool has_pending_reqs;
    bool has_writing_reqs;

    if (RAFT__QUEUE_IS_EMPTY(&uv->append_segments)) {
        return;
    }

    /* Check if there are pending append requests targeted to the current
     * segment. */
    head = RAFT__QUEUE_HEAD(&uv->append_segments);
    s = RAFT__QUEUE_DATA(head, struct segment, queue);

    has_pending_reqs = false;
    RAFT__QUEUE_FOREACH(head, &uv->append_pending_reqs)
    {
        struct append *r = RAFT__QUEUE_DATA(head, struct append, queue);
        if (r->segment == s) {
            has_pending_reqs = true;
            break;
        }
    }
    has_writing_reqs = !RAFT__QUEUE_IS_EMPTY(&uv->append_writing_reqs);

    /* If there is no pending append request or inflight write against the
     * current segment, we can submit a request for it to be closed
     * immediately. Otherwise, we set the finalize flag.
     *
     * TODO: is it actually possible to have pending requests with no writing
     * requests? Probably no. */
    if (!has_pending_reqs && !has_writing_reqs) {
        finalize_segment(s);
    } else {
        s->finalize = true;
    }
}

int io_uv__append_flush(struct io_uv *uv)
{
    int rv;

    finalize_current_segment(uv);

    rv = prepare_segment(uv);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

void io_uv__append_unblock(struct io_uv *uv)
{
    if (!RAFT__QUEUE_IS_EMPTY(&uv->append_pending_reqs)) {
        process_requests(uv);
    }
}

void io_uv__append_stop(struct io_uv *uv)
{
    struct segment *s;
    raft__queue *tail;

    flush_append_requests(&uv->append_pending_reqs, RAFT_ERR_IO_CANCELED);
    finalize_current_segment(uv);

    /* Also finalize the segments that we didn't write at all and are just
     * sitting in the append_segments queue waiting for writes against the
     * current segment to complete. */
    while (!RAFT__QUEUE_IS_EMPTY(&uv->append_segments)) {
        tail = RAFT__QUEUE_TAIL(&uv->append_segments);
        s = RAFT__QUEUE_DATA(tail, struct segment, queue);
        if (s->finalize) {
            break; /* This is the current segment, which is the head */
        }
        assert(s->written == 0);
        finalize_segment(s);
    }
}
