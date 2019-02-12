#include <string.h>

#include "assert.h"
#include "binary.h"
#include "checksum.h"
#include "io_uv_encoding.h"
#include "io_uv_fs.h"
#include "io_uv_writer.h"

/**
 * Writer state codes.
 */
enum {
    RAFT__IO_UV_WRITER_ACTIVE = 0,
    RAFT__IO_UV_WRITER_ABORTED,
    RAFT__IO_UV_WRITER_CLOSING,
    RAFT__IO_UV_WRITER_CLOSED
};

/**
 * Advance the writer's state machine, processing any pending event.
 */
static void raft__io_uv_writer_run(struct raft__io_uv_writer *w);

static void raft__io_uv_writer_run_active(struct raft__io_uv_writer *w);

static void raft__io_uv_writer_run_aborted(struct raft__io_uv_writer *w);

static void raft__io_uv_writer_run_closing(struct raft__io_uv_writer *w);

/**
 * Submit a get request to the preparer in order to get a new segment, since the
 * append request being submitted does not fit in the last segment we scheduled
 * writes for.
 */
static int raft__io_uv_writer_add_segment(struct raft__io_uv_writer *w);

/**
 * Callback invoked after a truncate request submitted to the closer has
 * completed.
 */
static void raft__io_uv_writer_truncate_cb(
    struct raft__io_uv_closer_truncate *req,
    int status);

/**
 * Give the current segment back to the preparer for closing it.
 */
static void raft__io_uv_writer_close_segment(
    struct raft__io_uv_writer *w,
    struct raft__io_uv_writer_segment *segment);

/**
 * Called after a request to get a new segment has completed.
 */
static void raft__io_uv_writer_add_segment_cb(
    struct raft__io_uv_preparer_get *req,
    struct raft__uv_file *file,
    unsigned long long counter,
    int status);

/**
 * Fail all append requests in the given queue, invoking their callbacks with
 * the given status code.
 */
static void raft__io_uv_writer_fail_requests(raft__queue *head, int status);

/**
 * Initialize an append request object.
 */
static void raft__io_uv_writer_append_init(struct raft__io_uv_writer_append *r,
                                           const struct raft_entry *entries,
                                           unsigned n,
                                           raft__io_uv_writer_append_cb cb);

/**
 * Initialize a new open segment object.
 */
static void raft__io_uv_writer_segment_init(
    struct raft__io_uv_writer_segment *s,
    struct raft__io_uv_writer *writer);

/**
 * Return @true if the remaining capacity of the segment is equal or greater
 * than @size.
 */
static bool raft__io_uv_writer_segment_fits(
    struct raft__io_uv_writer_segment *s,
    size_t size);

/**
 * Add @size bytes to the number of bytes that the segment will hold.
 */
static void raft__io_uv_writer_segment_grow(
    struct raft__io_uv_writer_segment *s,
    size_t size);

/**
 * Prepare the next segment write by encoding the entries in the given request
 * into the segment's write buffer.
 */
static int raft__io_uv_writer_segment_prepare(
    struct raft__io_uv_writer_segment *s,
    struct raft__io_uv_writer_append *req);

/**
 * Ensure that the write buffer of the given segment is large enough to hold the
 * the given number of bytes size.
 */
static int raft__io_uv_writer_segment_ensure_buf(
    struct raft__io_uv_writer_segment *s,
    size_t size);

/**
 * Submit a file write request to append the entries in the given request.
 */
static int raft__io_uv_writer_segment_write(
    struct raft__io_uv_writer_segment *s);

/**
 * Callback invoked after an append write request has completed.
 */
static void raft__io_uv_writer_segment_write_cb(struct raft__uv_file_write *req,
                                                int status);

int raft__io_uv_writer_init(struct raft__io_uv_writer *w,
                            struct uv_loop_s *loop,
                            struct raft_logger *logger,
                            struct raft__io_uv_preparer *preparer,
                            struct raft__io_uv_closer *closer,
                            const char *dir,
                            size_t block_size,
                            unsigned n_blocks)
{
    w->loop = loop;
    w->logger = logger;
    w->preparer = preparer;
    w->closer = closer;
    w->dir = dir;
    w->next_index = 0;
    w->block_size = block_size;
    w->n_blocks = n_blocks;
    w->state = RAFT__IO_UV_WRITER_ACTIVE;

    RAFT__QUEUE_INIT(&w->segment_queue);
    RAFT__QUEUE_INIT(&w->append_queue);
    RAFT__QUEUE_INIT(&w->write_queue);
    RAFT__QUEUE_INIT(&w->truncate_queue);

    return 0;
}

void raft__io_uv_writer_close(struct raft__io_uv_writer *w,
                              raft__io_uv_writer_close_cb cb)
{
    assert(!raft__io_uv_writer_is_closing(w));

    w->state = RAFT__IO_UV_WRITER_CLOSING;
    w->close_cb = cb;

    raft__io_uv_writer_run(w);
}

void raft__io_uv_writer_set_next_index(struct raft__io_uv_writer *w,
                                       raft_index index)
{
    w->next_index = index;
}

int raft__io_uv_writer_append(struct raft__io_uv_writer *w,
                              struct raft__io_uv_writer_append *req,
                              const struct raft_entry *entries,
                              unsigned n,
                              raft__io_uv_writer_append_cb cb)
{
    struct raft__io_uv_writer_segment *segment;
    raft__queue *head;
    bool fits;
    int rv;

    assert(!raft__io_uv_writer_is_closing(w));

    assert(entries != NULL);
    assert(n > 0);
    assert(w->next_index > 0);

    raft__io_uv_writer_append_init(req, entries, n, cb);

    /* TODO: at the moment we don't allow a single batch to exceed the size of a
     * segment. */
    if (req->size > w->block_size * w->n_blocks + sizeof(uint64_t)) {
        return RAFT_ERR_IO_TOOBIG;
    }

    /* If we have no segments yet, it means this is the very first append, and
     * we need to add a new segment. Otherwise we check if the last segment has
     * enough room for this batch of entries. */
    if (RAFT__QUEUE_IS_EMPTY(&w->segment_queue)) {
        fits = false;
    } else {
        head = RAFT__QUEUE_HEAD(&w->segment_queue);
        segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment, queue);

        fits = raft__io_uv_writer_segment_fits(segment, req->size);
    }

    /* If there's no segment or if this batch does not fit in this segment, we
     * need to add a new one. */
    if (!fits) {
        rv = raft__io_uv_writer_add_segment(w);
        if (rv != 0) {
            goto err;
        }
    }

    /* Get the last added segment */
    head = RAFT__QUEUE_TAIL(&w->segment_queue);
    segment = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment, queue);

    raft__io_uv_writer_segment_grow(segment, req->size);

    req->segment = segment;

    RAFT__QUEUE_PUSH(&w->append_queue, &req->queue);

    w->next_index += n;

    raft__io_uv_writer_run(w);

    return 0;

err:
    assert(rv != 0);

    return rv;
}

int raft__io_uv_writer_truncate(struct raft__io_uv_writer *w, raft_index index)
{
    struct raft__io_uv_writer_truncate *req;
    int rv;

    /* We should truncate only entries that we were requested to append in the
     * first place .*/
    assert(index < w->next_index);

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    req->writer = w;
    req->index = index;
    req->segment = NULL;
    req->truncate = NULL;

    /* The next entry will be appended at the truncation index */
    w->next_index = index;

    /* If there's an active segment, let's attach it to the truncate request,
     * since we'll want all its write to finish before we submit the truncate
     * request. */
    if (!RAFT__QUEUE_IS_EMPTY(&w->segment_queue)) {
        raft__queue *head;

        head = RAFT__QUEUE_TAIL(&w->segment_queue);
        req->segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment, queue);

        /* Add a new segment, which will receive all write requests from now
         * on. */
        rv = raft__io_uv_writer_add_segment(w);
        if (rv != 0) {
            goto err_after_req_alloc;
        }
    }

    RAFT__QUEUE_PUSH(&w->truncate_queue, &req->queue);

    raft__io_uv_writer_run(w);

    return 0;

err_after_req_alloc:
    raft_free(req);

err:
    assert(rv != 0);

    return rv;
}

bool raft__io_uv_writer_is_closing(struct raft__io_uv_writer *w)
{
    return w->state == RAFT__IO_UV_WRITER_CLOSING ||
           w->state == RAFT__IO_UV_WRITER_CLOSED;
}

static void raft__io_uv_writer_run(struct raft__io_uv_writer *w)
{
    assert(w->state != RAFT__IO_UV_WRITER_CLOSED);

    if (w->state == RAFT__IO_UV_WRITER_CLOSING) {
        raft__io_uv_writer_run_closing(w);
        return;
    }

    if (w->state == RAFT__IO_UV_WRITER_ACTIVE) {
        raft__io_uv_writer_run_active(w);
    }

    if (w->state == RAFT__IO_UV_WRITER_ABORTED) {
        raft__io_uv_writer_run_aborted(w);
    }
}

static void raft__io_uv_writer_run_active(struct raft__io_uv_writer *w)
{
    struct raft__io_uv_writer_segment *segment;
    struct raft__io_uv_writer_append *req;
    raft__queue *head;
    raft__queue queue;
    unsigned n_reqs;
    int rv;

    /* If we are already writing, let's wait. */
    if (!RAFT__QUEUE_IS_EMPTY(&w->write_queue)) {
        return;
    }

    /* Check if there's a pending truncate request. */
    if (!RAFT__QUEUE_IS_EMPTY(&w->truncate_queue)) {
        struct raft__io_uv_writer_truncate *req;

        head = RAFT__QUEUE_HEAD(&w->truncate_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_truncate, queue);

        /* If the truncate request to the closer was already submitted, let's
         * just wait. */
        if (req->truncate != NULL) {
            return;
        }

        /* If we were writing an open segment when we received the truncate
         * request, we need to close it first */
        if (req->segment != NULL) {
            raft__io_uv_writer_close_segment(w, req->segment);
        }

        /* Submit the truncate request to the closer. */
        req->truncate = raft_malloc(sizeof *req->truncate);
        if (req->truncate == NULL) {
            rv = RAFT_ERR_NOMEM;
            goto err;
        }
        req->truncate->data = req;

        rv = raft__io_uv_closer_truncate(w->closer, req->index, req->truncate,
                                         raft__io_uv_writer_truncate_cb);
        if (rv != 0) {
            goto err;
        }

        return;
    }

    /* If there's nothing to write, this is a no-op. */
    if (RAFT__QUEUE_IS_EMPTY(&w->append_queue)) {
        return;
    }

prepare:
    assert(!RAFT__QUEUE_IS_EMPTY(&w->segment_queue));

    /* Get the open segment with lowest counter */
    head = RAFT__QUEUE_HEAD(&w->segment_queue);
    segment = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment, queue);

    /* If the preparer hasn't provided the segment yet, let's wait. */
    if (segment->file == NULL) {
        return;
    }

    /* Let's add to the segment write buffer all pending requests targeted to
     * this segment. */
    RAFT__QUEUE_INIT(&queue);

    n_reqs = 0;
    while (1) {
        head = RAFT__QUEUE_HEAD(&w->append_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_append, queue);

        assert(req->segment != NULL);

        if (req->segment != segment) {
            break;
        }

        RAFT__QUEUE_REMOVE(head);
        RAFT__QUEUE_PUSH(&queue, head);

        n_reqs++;

        rv = raft__io_uv_writer_segment_prepare(segment, req);
        if (rv != 0) {
            goto err;
        }

        if (RAFT__QUEUE_IS_EMPTY(&w->append_queue)) {
            break;
        }
    }

    if (n_reqs == 0) {
        raft__io_uv_writer_close_segment(w, segment);
        goto prepare;
    }

    rv = raft__io_uv_writer_segment_write(segment);
    if (rv != 0) {
        goto err;
    }

    while (!RAFT__QUEUE_IS_EMPTY(&queue)) {
        head = RAFT__QUEUE_HEAD(&queue);
        RAFT__QUEUE_REMOVE(head);
        RAFT__QUEUE_PUSH(&w->write_queue, head);
    }

    return;

err:
    w->state = RAFT__IO_UV_WRITER_ABORTED;
}

static void raft__io_uv_writer_run_aborted(struct raft__io_uv_writer *w)
{
    /* Abort all pending append requests. */
    raft__io_uv_writer_fail_requests(&w->append_queue, RAFT_ERR_IO_ABORTED);
}

static void raft__io_uv_writer_run_closing(struct raft__io_uv_writer *w)
{
    struct raft__io_uv_writer_segment *segment;
    raft__queue *head;

    /* Cancel all pending append requests. */
    raft__io_uv_writer_fail_requests(&w->append_queue, RAFT_ERR_IO_CANCELED);

    /* Wait for any current write or truncate request to finish. */
    if (!RAFT__QUEUE_IS_EMPTY(&w->write_queue) ||
        !RAFT__QUEUE_IS_EMPTY(&w->truncate_queue)) {
        return;
    }

    /* Wait for any pending segment get request to finish. */
    if (!RAFT__QUEUE_IS_EMPTY(&w->segment_queue)) {
        unsigned n;

        n = 0;
        RAFT__QUEUE_FOREACH(head, &w->segment_queue)
        {
            segment = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment,
                                       queue);
            if (segment->file == NULL) {
                n++;
            }
        }

        if (n > 0) {
            return;
        }
    }

    /* Close all open segments that we acquired */
    while (!RAFT__QUEUE_IS_EMPTY(&w->segment_queue)) {
        raft__queue *head;
        struct raft__io_uv_writer_segment *segment;

        head = RAFT__QUEUE_HEAD(&w->segment_queue);
        segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_segment, queue);

        raft__io_uv_writer_close_segment(w, segment);
    }

    w->state = RAFT__IO_UV_WRITER_CLOSED;

    if (w->close_cb != NULL) {
        w->close_cb(w);
    }
}

static int raft__io_uv_writer_add_segment(struct raft__io_uv_writer *w)
{
    struct raft__io_uv_writer_segment *segment;
    int rv;

    segment = raft_malloc(sizeof *segment);
    if (segment == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    raft__io_uv_writer_segment_init(segment, w);

    rv = raft__io_uv_preparer_get(w->preparer, &segment->get,
                                  raft__io_uv_writer_add_segment_cb);
    assert(rv == 0); /* Currently this can't fail */

    RAFT__QUEUE_PUSH(&w->segment_queue, &segment->queue);

    return 0;

err:
    assert(rv != 0);
    return rv;
}

static void raft__io_uv_writer_truncate_cb(
    struct raft__io_uv_closer_truncate *truncate,
    int status)
{
    struct raft__io_uv_writer_truncate *req = truncate->data;
    struct raft__io_uv_writer *w = req->writer;

    /* If we have been closed, let's just discard the request. */
    if (raft__io_uv_writer_is_closing(w)) {
        goto out;
    }

    if (status != 0) {
        w->state = RAFT__IO_UV_WRITER_ABORTED;
    }

out:
    RAFT__QUEUE_REMOVE(&req->queue);
    raft_free(truncate);
    raft_free(req);

    raft__io_uv_writer_run(w);
}

static void raft__io_uv_writer_close_segment(
    struct raft__io_uv_writer *w,
    struct raft__io_uv_writer_segment *segment)
{
    raft__io_uv_closer_put(w->closer, segment->counter, segment->written,
                           segment->first_index, segment->last_index);

    raft__uv_file_close(segment->file, (raft__uv_file_close_cb)raft_free);

    if (segment->arena.base != NULL) {
        free(segment->arena.base);
    }

    RAFT__QUEUE_REMOVE(&segment->queue);

    raft_free(segment);
}

static void raft__io_uv_writer_add_segment_cb(
    struct raft__io_uv_preparer_get *req,
    struct raft__uv_file *file,
    unsigned long long counter,
    int status)
{
    struct raft__io_uv_writer_segment *segment = req->data;
    struct raft__io_uv_writer *w = segment->writer;

    /* If we have been closed, let's discard the segment. */
    if (raft__io_uv_writer_is_closing(w)) {
        RAFT__QUEUE_REMOVE(&segment->queue);

        if (status == 0) {
            raft__uv_file_close(file, (raft__uv_file_close_cb)raft_free);

            /* Ignore errors, as there's nothing we can do about it. */
            raft__io_uv_closer_put(w->closer, counter, 0, 0, 0);
        }

        assert(segment->arena.base == NULL);

        raft_free(segment);

        goto out;
    }

    if (status != 0) {
        RAFT__QUEUE_REMOVE(&segment->queue);

        raft_free(segment);

        w->state = RAFT__IO_UV_WRITER_ABORTED;

        goto out;
    }

    assert(counter > 0);
    assert(file != NULL);

    segment->file = file;
    segment->counter = counter;

out:
    raft__io_uv_writer_run(w);
}

static void raft__io_uv_writer_fail_requests(raft__queue *queue, int status)
{
    raft__queue *head;

    while (!RAFT__QUEUE_IS_EMPTY(queue)) {
        struct raft__io_uv_writer_append *req;

        head = RAFT__QUEUE_HEAD(queue);
        RAFT__QUEUE_REMOVE(head);

        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_append, queue);

        req->cb(req, status);
    }
}

static void raft__io_uv_writer_append_init(struct raft__io_uv_writer_append *r,
                                           const struct raft_entry *entries,
                                           unsigned n,
                                           raft__io_uv_writer_append_cb cb)
{
    unsigned i;

    r->entries = entries;
    r->n = n;
    r->cb = cb;

    /* CRC checksums */
    r->size = sizeof(uint32_t) * 2;

    /* Batch header */
    r->size += raft_io_uv_sizeof__batch_header(r->n);

    /* Entries data */
    for (i = 0; i < r->n; i++) {
        size_t len = r->entries[i].buf.len;

        r->size += len;
        if (len % 8 != 0) {
            /* Add padding */
            r->size += 8 - (len % 8);
        }
    }
}

static void raft__io_uv_writer_segment_init(
    struct raft__io_uv_writer_segment *s,
    struct raft__io_uv_writer *writer)
{
    s->writer = writer;
    s->get.data = s;
    s->write.data = s;
    s->counter = 0;
    s->file = NULL;
    s->first_index = writer->next_index;
    s->last_index = s->first_index - 1;
    s->size = sizeof(uint64_t) /* Format version */;
    s->next_block = 0;
    s->arena.base = NULL;
    s->arena.len = 0;
    s->scheduled = 0;
    s->written = 0;
}

static bool raft__io_uv_writer_segment_fits(
    struct raft__io_uv_writer_segment *s,
    size_t size)
{
    size_t cap = s->writer->block_size * s->writer->n_blocks;

    return s->size + size <= cap;
}

static void raft__io_uv_writer_segment_grow(
    struct raft__io_uv_writer_segment *s,
    size_t size)
{
    assert(raft__io_uv_writer_segment_fits(s, size));

    s->size += size;
}

static int raft__io_uv_writer_segment_prepare(
    struct raft__io_uv_writer_segment *s,
    struct raft__io_uv_writer_append *req)
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
    if (s->written == 0) {
        size += sizeof(uint64_t);
    }

    assert(s->written + size <= s->writer->block_size * s->writer->n_blocks);

    rv = raft__io_uv_writer_segment_ensure_buf(s, s->scheduled + size);
    if (rv != 0) {
        return rv;
    }

    cursor = s->arena.base + s->scheduled;

    if (s->written == 0) {
        raft__put64(&cursor, RAFT__IO_UV_WRITER_FORMAT);
    }

    /* Placeholder of the checksums */
    crc1_p = cursor;
    raft__put32(&cursor, 0);
    crc2_p = cursor;
    raft__put32(&cursor, 0);

    /* Batch header */
    header = cursor;
    raft_io_uv_encode__batch_header(req->entries, req->n, cursor);
    crc1 = raft__crc32(header, raft_io_uv_sizeof__batch_header(req->n), 0);
    cursor += raft_io_uv_sizeof__batch_header(req->n);

    /* Batch data */
    crc2 = 0;
    for (i = 0; i < req->n; i++) {
        const struct raft_entry *entry = &req->entries[i];

        /* TODO: enforce the requirment of 8-byte aligment also in the
         * higher-level APIs. */
        assert(entry->buf.len % sizeof(uint64_t) == 0);

        memcpy(cursor, entry->buf.base, entry->buf.len);
        crc2 = raft__crc32(cursor, entry->buf.len, crc2);

        cursor += entry->buf.len;
    }

    raft__put32(&crc1_p, crc1);
    raft__put32(&crc2_p, crc2);

    s->scheduled += size;
    s->last_index += req->n;

    return 0;
}

static int raft__io_uv_writer_segment_ensure_buf(
    struct raft__io_uv_writer_segment *s,
    size_t size)
{
    unsigned n = (size / s->writer->block_size);
    void *base;
    size_t len;

    if (s->arena.len >= size) {
        assert(s->arena.base != NULL);
        return 0;
    }

    if (size % s->writer->block_size != 0) {
        n++;
    }

    len = s->writer->block_size * n;
    base = aligned_alloc(s->writer->block_size, len);
    if (base == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memset(base, 0, len);

    /* If the current arena is initialized, we need to copy its first block,
     * since it might have data that we want to retain in the next write. */
    if (s->arena.base != NULL) {
        assert(s->arena.len >= s->writer->block_size);
        memcpy(base, s->arena.base, s->writer->block_size);
        free(s->arena.base);
    }

    s->arena.base = base;
    s->arena.len = len;

    return 0;
}

static int raft__io_uv_writer_segment_write(
    struct raft__io_uv_writer_segment *s)
{
    unsigned n_blocks;
    int rv;

    assert(s->file != NULL);
    assert(s->arena.base != NULL);

    n_blocks = s->scheduled / s->writer->block_size;
    if (s->scheduled % s->writer->block_size != 0) {
        n_blocks++;
    }

    s->buf.base = s->arena.base;
    s->buf.len = n_blocks * s->writer->block_size;

    /* Set the remainder of the last block to 0 */
    if (s->scheduled % s->writer->block_size != 0) {
        memset(s->buf.base + s->scheduled, 0,
               s->writer->block_size - (s->scheduled % s->writer->block_size));
    }

    rv = raft__uv_file_write(s->file, &s->write, &s->buf, 1,
                             s->next_block * s->writer->block_size,
                             raft__io_uv_writer_segment_write_cb);

    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void raft__io_uv_writer_segment_write_cb(
    struct raft__uv_file_write *write,
    const int status)
{
    struct raft__io_uv_writer_segment *segment = write->data;
    struct raft__io_uv_writer *w = segment->writer;
    unsigned n_blocks;
    int result = 0;

    assert(segment->buf.len % w->block_size == 0);
    assert(segment->buf.len >= w->block_size);

    /* Number of blocks that we wrote. */
    n_blocks = segment->buf.len / w->block_size;

    /* Check if the write was successful. */
    if (status != (int)segment->buf.len) {
        if (status < 0) {
            raft_errorf(w->logger, "write: %s", uv_strerror(status));
        } else {
            raft_errorf(w->logger, "only %d bytes written", status);
        }
        result = RAFT_ERR_IO;

        if (!raft__io_uv_writer_is_closing(w)) {
            w->state = RAFT__IO_UV_WRITER_ABORTED;
        }
    }

    segment->written = segment->next_block * w->block_size + segment->scheduled;

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
    if (segment->scheduled < w->block_size) {
        /* Nothing to do */
        assert(n_blocks == 1);
    } else if (segment->scheduled == w->block_size) {
        assert(n_blocks == 1);
        segment->next_block++;
        segment->scheduled = 0;
        memset(segment->arena.base, 0, w->block_size);
    } else {
        assert(segment->scheduled > w->block_size);
        assert(segment->buf.len > w->block_size);

        if (segment->scheduled % w->block_size > 0) {
            segment->next_block += n_blocks - 1;
            segment->scheduled = segment->scheduled % w->block_size;
            memcpy(segment->arena.base,
                   segment->arena.base + (n_blocks - 1) * w->block_size,
                   w->block_size);
        } else {
            segment->next_block += n_blocks;
            segment->scheduled = 0;
            memset(segment->arena.base, 0, w->block_size);
        }
    }

    /* Fire the callbacks of all requests that were fulfilled with this
     * write. */
    while (!RAFT__QUEUE_IS_EMPTY(&w->write_queue)) {
        struct raft__io_uv_writer_append *req;
        raft__queue *head;

        head = RAFT__QUEUE_HEAD(&w->write_queue);
        RAFT__QUEUE_REMOVE(head);

        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_writer_append, queue);

        req->cb(req, result);
    }

    raft__io_uv_writer_run(w);
}
