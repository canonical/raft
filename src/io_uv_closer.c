#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "binary.h"
#include "checksum.h"
#include "io_uv_closer.h"
#include "io_uv_encoding.h"
#include "io_uv_fs.h"
#include "io_uv_writer.h"

/**
 State flags
 */
enum {
    RAFT__IO_UV_CLOSER_ACTIVE = 0,
    RAFT__IO_UV_CLOSER_ABORTED,
    RAFT__IO_UV_CLOSER_CLOSING,
    RAFT__IO_UV_CLOSER_CLOSED
};

/**
 * Advance the closer's state machine, processing any pending event.
 */
static void raft__io_uv_closer_run(struct raft__io_uv_closer *c);

static void raft__io_uv_closer_run_active(struct raft__io_uv_closer *c);

static void raft__io_uv_closer_run_aborted(struct raft__io_uv_closer *c);

static void raft__io_uv_closer_run_closing(struct raft__io_uv_closer *c);

/**
 * Drop all pending put requests.
 */
static void raft__io_uv_closer_drop_pending_put(struct raft__io_uv_closer *c);

/**
 * Schedule closing an open segment.
 */
static int raft__io_uv_closer_close_start(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_closer_segment *segment);

/**
 * Run all blocking syscalls involved in closing a used open segment.
 *
 * An open segment is closed by truncating its length to the number of bytes
 * that were actually written into it and then renaming it.
 */
static void raft__io_uv_closer_close_work_cb(uv_work_t *work);

/**
 * Invoked after the work performed in the threadpool to close a segment has
 * completed.
 */
static void raft__io_uv_closer_close_after_work_cb(uv_work_t *work, int status);

/**
 * Schedule truncating the log.
 */
static int raft__io_uv_closer_truncate_start(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_closer_truncate *req);

/**
 * Run all blocking syscalls involved in truncating the log.
 */
static void raft__io_uv_closer_truncate_work_cb(uv_work_t *work);

/**
 * Truncate a closed segment at the given index.
 */
static int raft__io_uv_closer_truncate_segment(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_loader_segment *segment,
    raft_index index);

/**
 * Invoked after the work performed in the threadpool to truncate the log has
 * completed.
 */
static void raft__io_uv_closer_truncate_after_work_cb(uv_work_t *work,
                                                      int status);

int raft__io_uv_closer_init(struct raft__io_uv_closer *c,
                            struct uv_loop_s *loop,
                            struct raft_logger *logger,
                            struct raft__io_uv_loader *loader,
                            const char *dir)
{
    c->loop = loop;
    c->logger = logger;
    c->loader = loader;
    c->dir = dir;
    c->last_index = 0;
    c->state = RAFT__IO_UV_CLOSER_ACTIVE;
    c->close_cb = NULL;

    RAFT__QUEUE_INIT(&c->put_queue);
    c->closing = NULL;

    RAFT__QUEUE_INIT(&c->truncate_queue);
    c->truncating = NULL;

    return 0;
}

void raft__io_uv_closer_close(struct raft__io_uv_closer *c,
                              raft__io_uv_closer_close_cb cb)
{
    assert(!raft__io_uv_closer_is_closing(c));

    c->state = RAFT__IO_UV_CLOSER_CLOSING;
    c->close_cb = cb;

    raft__io_uv_closer_run(c);
}

void raft__io_uv_closer_set_last_index(struct raft__io_uv_closer *c,
                                       raft_index index)
{
    c->last_index = index;
}

int raft__io_uv_closer_put(struct raft__io_uv_closer *c,
                           unsigned long long counter,
                           size_t used,
                           raft_index first_index,
                           raft_index last_index)
{
    struct raft__io_uv_closer_segment *segment;

    assert(!raft__io_uv_closer_is_closing(c));

    /* If the open segment is not empty, we expect its first index to be the
     * successor of the end index of the last segment we closed. */
    if (used > 0) {
        assert(first_index > 0);
        assert(last_index >= first_index);
        assert(first_index == c->last_index + 1);
    }

    segment = raft_malloc(sizeof *segment);
    if (segment == NULL) {
        return RAFT_ERR_NOMEM;
    }

    segment->counter = counter;
    segment->used = used;
    segment->first_index = first_index;
    segment->last_index = last_index;
    segment->closer = c;

    RAFT__QUEUE_INIT(&segment->queue);
    RAFT__QUEUE_PUSH(&c->put_queue, &segment->queue);

    c->last_index = last_index;

    raft__io_uv_closer_run(c);

    return 0;
}

int raft__io_uv_closer_truncate(struct raft__io_uv_closer *c,
                                raft_index index,
                                struct raft__io_uv_closer_truncate *req,
                                raft__io_uv_closer_truncate_cb cb)
{
    assert(index <= c->last_index);

    req->closer = c;
    req->index = index;
    req->cb = cb;

    RAFT__QUEUE_PUSH(&c->truncate_queue, &req->queue);

    c->last_index = index - 1;

    raft__io_uv_closer_run(c);

    return 0;
}

bool raft__io_uv_closer_is_closing(struct raft__io_uv_closer *c)
{
    return c->state == RAFT__IO_UV_CLOSER_CLOSING ||
           c->state == RAFT__IO_UV_CLOSER_CLOSED;
}

static void raft__io_uv_closer_run(struct raft__io_uv_closer *c)
{
    assert(c->state != RAFT__IO_UV_CLOSER_CLOSED);

    if (c->state == RAFT__IO_UV_CLOSER_CLOSING) {
        raft__io_uv_closer_run_closing(c);
        return;
    }

    if (c->state == RAFT__IO_UV_CLOSER_ACTIVE) {
        raft__io_uv_closer_run_active(c);
    }

    if (c->state == RAFT__IO_UV_CLOSER_ABORTED) {
        raft__io_uv_closer_run_aborted(c);
    }
}

static void raft__io_uv_closer_run_active(struct raft__io_uv_closer *c)
{
    struct raft__io_uv_closer_segment *segment;
    raft__queue *head;
    int rv;

    /* If we got truncate requests, we can process them only as long as we're
     * not still closing a segment. */
    if (!RAFT__QUEUE_IS_EMPTY(&c->truncate_queue)) {
        struct raft__io_uv_closer_truncate *req;

        if (c->closing != NULL) {
            return;
        }

        head = RAFT__QUEUE_HEAD(&c->truncate_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_closer_truncate, queue);

        RAFT__QUEUE_REMOVE(&req->queue);

        rv = raft__io_uv_closer_truncate_start(c, req);
        if (rv != 0) {
            goto err;
        }

        return;
    }

    /* If we're already processing a segment, let's wait. */
    if (c->closing != NULL) {
        return;
    }

    /* If there's no pending request, we're done. */
    if (RAFT__QUEUE_IS_EMPTY(&c->put_queue)) {
        return;
    }

    head = RAFT__QUEUE_HEAD(&c->put_queue);
    segment = RAFT__QUEUE_DATA(head, struct raft__io_uv_closer_segment, queue);

    RAFT__QUEUE_REMOVE(&segment->queue);

    rv = raft__io_uv_closer_close_start(c, segment);
    if (rv != 0) {
        goto err;
    }

    return;
err:
    assert(rv != 0);

    c->state = RAFT__IO_UV_CLOSER_ABORTED;
}

static void raft__io_uv_closer_run_aborted(struct raft__io_uv_closer *c)
{
    raft__io_uv_closer_drop_pending_put(c);
}

static void raft__io_uv_closer_run_closing(struct raft__io_uv_closer *c)
{
    raft__io_uv_closer_drop_pending_put(c);

    if (c->closing != NULL) {
        return;
    }

    c->state = RAFT__IO_UV_CLOSER_CLOSED;

    if (c->close_cb != NULL) {
        c->close_cb(c);
    }
}

static void raft__io_uv_closer_drop_pending_put(struct raft__io_uv_closer *c)
{
    while (!RAFT__QUEUE_IS_EMPTY(&c->put_queue)) {
        struct raft__io_uv_closer_segment *segment;
        raft__queue *head;

        head = RAFT__QUEUE_HEAD(&c->put_queue);
        RAFT__QUEUE_REMOVE(head);

        segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_closer_segment, queue);

        raft_free(segment);
    }
}

static int raft__io_uv_closer_close_start(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_closer_segment *segment)
{
    int rv;

    assert(c->closing == NULL);
    assert(segment->counter > 0);

    c->work.data = segment;

    rv = uv_queue_work(c->loop, &c->work, raft__io_uv_closer_close_work_cb,
                       raft__io_uv_closer_close_after_work_cb);
    if (rv != 0) {
        raft_errorf(c->logger, "start to truncate segment file %d: %s",
                    segment->counter, uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    c->closing = segment;

    return 0;
}

static void raft__io_uv_closer_close_work_cb(uv_work_t *work)
{
    struct raft__io_uv_closer_segment *segment = work->data;
    struct raft__io_uv_closer *c = segment->closer;
    raft__io_uv_fs_filename filename1;
    raft__io_uv_fs_filename filename2;
    int rv;

    sprintf(filename1, "open-%lld", segment->counter);

    /* If the segment hasn't actually been used (because the writer has been
     * closed or aborted before making any write), then let's just remove it. */
    if (segment->used == 0) {
        raft__io_uv_fs_unlink(c->dir, filename1);
        goto out;
    }

    /* Truncate and rename the segment */
    rv = raft__io_uv_fs_truncate(c->dir, filename1, segment->used);
    if (rv != 0) {
        raft_errorf(c->logger, "truncate segment file %d: %s", segment->counter,
                    uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto abort;
    }

    sprintf(filename2, "%020llu-%020llu", segment->first_index,
            segment->last_index);

    rv = raft__io_uv_fs_rename(c->dir, filename1, filename2);
    if (rv != 0) {
        raft_errorf(c->logger, "rename segment file %d: %s", segment->counter,
                    uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto abort;
    }

out:
    segment->status = 0;

    return;

abort:
    assert(rv != 0);

    segment->status = rv;
}

static void raft__io_uv_closer_close_after_work_cb(uv_work_t *work, int status)
{
    struct raft__io_uv_closer_segment *segment = work->data;
    struct raft__io_uv_closer *c = segment->closer;

    assert(status == 0); /* We don't cancel worker requests */

    c->closing = NULL;

    if (segment->status != 0) {
        c->state = RAFT__IO_UV_CLOSER_ABORTED;
    }

    raft_free(segment);

    raft__io_uv_closer_run(c);
}

static int raft__io_uv_closer_truncate_start(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_closer_truncate *req)
{
    int rv;

    assert(c->truncating == NULL);

    c->work.data = req;

    rv = uv_queue_work(c->loop, &c->work, raft__io_uv_closer_truncate_work_cb,
                       raft__io_uv_closer_truncate_after_work_cb);
    if (rv != 0) {
        raft_errorf(c->logger, "start to truncate log at index %lld: %s",
                    req->index, uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    c->truncating = req;

    return 0;
}

static void raft__io_uv_closer_truncate_work_cb(uv_work_t *work)
{
    struct raft__io_uv_closer_truncate *req = work->data;
    struct raft__io_uv_closer *c = req->closer;
    struct raft__io_uv_loader_segment *segments;
    struct raft__io_uv_loader_segment *segment;
    size_t n_segments;
    size_t i;
    size_t j;
    int rv;

    /* Load all segments on disk. */
    rv = raft__io_uv_loader_list(c->loader, NULL, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }

    /* TODO: this assertion should always hold except for snapshots */
    assert(n_segments > 0);

    /* Look for the segment that contains the truncate point. */
    for (i = 0; i < n_segments; i++) {
        segment = &segments[i];

        if (req->index >= segment->first_index &&
            req->index <= segment->end_index) {
            break;
        }
    }

    /* TODO: this assertion should always hold except for snapshots */
    assert(i < n_segments);

    /* If the truncate index is not the first of the segment, we need to
     * truncate it. */
    if (req->index > segment->first_index) {
        rv = raft__io_uv_closer_truncate_segment(c, segment, req->index);
        if (rv != 0) {
            goto err_after_list;
        }
    }

    for (j = i; j < n_segments; j++) {
        segment = &segments[j];

        rv = raft__io_uv_fs_unlink(c->dir, segment->filename);
        if (rv != 0) {
            raft_errorf(c->logger, "unlink segment %s: %s", segment->filename,
                        uv_strerror(rv));
            rv = RAFT_ERR_IO;
            goto err_after_list;
        }
    }

    rv = raft__io_uv_fs_sync_dir(c->dir);
    if (rv != 0) {
        raft_errorf(c->logger, "sync data directory: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_list;
    }

    raft_free(segments);

    req->status = 0;

    return;

err_after_list:
    raft_free(segments);

err:
    assert(rv != 0);

    req->status = rv;
}

static int raft__io_uv_closer_truncate_segment(
    struct raft__io_uv_closer *c,
    struct raft__io_uv_loader_segment *segment,
    raft_index index)
{
    raft__io_uv_fs_filename filename;
    raft__io_uv_fs_path path;
    struct raft_entry *entries;
    struct raft_buffer buf;
    void *cursor;
    void *header;
    unsigned crc1; /* Header checksum */
    unsigned crc2; /* Data checksum */
    void *crc1_p;  /* Pointer to header checksum slot */
    void *crc2_p;  /* Pointer to data checksum slot */
    void *batch;
    size_t n;
    size_t m;
    size_t i;
    int fd;
    int rv = 0;

    raft_infof(c->logger, "truncate %u-%u at %u", segment->first_index,
               segment->end_index, index);

    rv = raft__io_uv_loader_load_closed(c->loader, segment, &entries, &n);
    if (rv != 0) {
        goto out;
    }

    /* Discard all entries after the truncate index (included) */
    assert(index - segment->first_index < n);
    m = index - segment->first_index;

    /* Render the path.
     *
     * TODO: we should use a temporary file name so in case of crash we don't
     *      consider this segment as corrupted.
     */
    sprintf(filename, "%020llu-%020llu", segment->first_index, index - 1);
    raft__io_uv_fs_join(c->dir, filename, path);

    /* Open the file. */
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(c->logger, "open %s: %s", path, strerror(errno));
        goto out_after_load;
    }

    buf.len = sizeof(uint64_t) +     /* Format version */
              sizeof(uint32_t) * 2 + /* CRC checksum */
              raft_io_uv_sizeof__batch_header(m) /* Batch header */;
    buf.base = raft_malloc(buf.len);

    if (buf.base == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto out_after_open;
    }

    cursor = buf.base;

    raft__put64(&cursor, RAFT__IO_UV_WRITER_FORMAT); /* Format version */

    crc1_p = cursor;
    raft__put32(&cursor, 0);

    crc2_p = cursor;
    raft__put32(&cursor, 0);

    header = cursor;

    raft__put64(&cursor, m); /* Number of entries */

    crc2 = 0;

    for (i = 0; i < m; i++) {
        struct raft_entry *entry = &entries[i];

        raft__put64(&cursor, entry->term);    /* Entry term */
        raft__put8(&cursor, entry->type);     /* Entry type */
        raft__put8(&cursor, 0);               /* Unused */
        raft__put8(&cursor, 0);               /* Unused */
        raft__put8(&cursor, 0);               /* Unused */
        raft__put32(&cursor, entry->buf.len); /* Size of entry data */

        crc2 = raft__crc32(entry->buf.base, entry->buf.len, crc2);
    }

    crc1 = raft__crc32(header, raft_io_uv_sizeof__batch_header(m), 0);

    raft__put32(&crc1_p, crc1);
    raft__put32(&crc2_p, crc2);

    rv = write(fd, buf.base, buf.len);

    raft_free(buf.base);

    if (rv == -1) {
        raft_errorf(c->logger, "write %s: %s", path, strerror(errno));
        rv = RAFT_ERR_IO;
        goto out_after_open;
    }
    if (rv != (int)buf.len) {
        raft_errorf(c->logger, "write %s: only %d bytes written", path, rv);
        rv = RAFT_ERR_IO;
        goto out_after_open;
    }

    for (i = 0; i < m; i++) {
        struct raft_entry *entry = &entries[i];

        rv = write(fd, entry->buf.base, entry->buf.len);
        if (rv == -1) {
            raft_errorf(c->logger, "write %s: %s", path, strerror(errno));
            rv = RAFT_ERR_IO;
            goto out_after_open;
        }
        if (rv != (int)entry->buf.len) {
            raft_errorf(c->logger, "write %s: only %d bytes written", path, rv);
            rv = RAFT_ERR_IO;
            goto out_after_open;
        }
    }

    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(c->logger, "fsync %s: %s", path, strerror(errno));
        rv = RAFT_ERR_IO;
        goto out_after_open;
    }

out_after_open:
    close(fd);

out_after_load:
    batch = NULL;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        if (entry->batch != batch) {
            raft_free(entry->batch);
            batch = entry->batch;
        }
    }

    raft_free(entries);

out:
    return rv;
}

static void raft__io_uv_closer_truncate_after_work_cb(uv_work_t *work,
                                                      int status)
{
    struct raft__io_uv_closer_truncate *req = work->data;
    struct raft__io_uv_closer *c = req->closer;

    assert(status == 0);

    if (req->status != 0) {
        c->state = RAFT__IO_UV_CLOSER_ABORTED;
    }

    req->cb(req, req->status);

    c->truncating = NULL;

    raft__io_uv_closer_run(c);
}
