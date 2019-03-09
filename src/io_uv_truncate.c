#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "byte.h"
#include "io_uv.h"
#include "io_uv_encoding.h"
#include "io_uv_load.h"

struct truncate
{
    struct io_uv *uv;
    raft_index index;
    int status;
    raft__queue queue;
};

/* Truncate a segment that was already closed. */
static int truncate_closed_segment(struct io_uv *uv,
                                   struct io_uv__segment_meta *segment,
                                   raft_index index)
{
    io_uv__filename filename;
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

    raft_infof(uv->logger, "truncate %u-%u at %u", segment->first_index,
               segment->end_index, index);

    rv = io_uv__load_closed(uv, segment, &entries, &n);
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
    sprintf(filename, "%llu-%llu", segment->first_index, index - 1);

    /* Open the file. */
    fd = raft__io_uv_fs_open(uv->dir, filename, O_WRONLY | O_CREAT | O_EXCL);
    if (fd == -1) {
        raft_errorf(uv->logger, "open %s: %s", filename, strerror(errno));
        goto out_after_load;
    }

    buf.len = sizeof(uint64_t) +     /* Format version */
              sizeof(uint32_t) * 2 + /* CRC checksum */
              io_uv__sizeof_batch_header(m) /* Batch header */;
    buf.base = raft_malloc(buf.len);

    if (buf.base == NULL) {
        rv = RAFT_ENOMEM;
        goto out_after_open;
    }

    cursor = buf.base;

    byte__put64(&cursor, IO_UV__DISK_FORMAT); /* Format version */

    crc1_p = cursor;
    byte__put32(&cursor, 0);

    crc2_p = cursor;
    byte__put32(&cursor, 0);

    header = cursor;

    byte__put64(&cursor, m); /* Number of entries */

    crc2 = 0;

    for (i = 0; i < m; i++) {
        struct raft_entry *entry = &entries[i];

        byte__put64(&cursor, entry->term);    /* Entry term */
        byte__put8(&cursor, entry->type);     /* Entry type */
        byte__put8(&cursor, 0);               /* Unused */
        byte__put8(&cursor, 0);               /* Unused */
        byte__put8(&cursor, 0);               /* Unused */
        byte__put32(&cursor, entry->buf.len); /* Size of entry data */

        crc2 = byte__crc32(entry->buf.base, entry->buf.len, crc2);
    }

    crc1 = byte__crc32(header, io_uv__sizeof_batch_header(m), 0);

    byte__put32(&crc1_p, crc1);
    byte__put32(&crc2_p, crc2);

    rv = write(fd, buf.base, buf.len);

    raft_free(buf.base);

    if (rv == -1) {
        raft_errorf(uv->logger, "write %s: %s", filename, strerror(errno));
        rv = RAFT_ERR_IO;
        goto out_after_open;
    }
    if (rv != (int)buf.len) {
        raft_errorf(uv->logger, "write %s: only %d bytes written", filename,
                    rv);
        rv = RAFT_ERR_IO;
        goto out_after_open;
    }

    for (i = 0; i < m; i++) {
        struct raft_entry *entry = &entries[i];

        rv = write(fd, entry->buf.base, entry->buf.len);
        if (rv == -1) {
            raft_errorf(uv->logger, "write %s: %s", filename, strerror(errno));
            rv = RAFT_ERR_IO;
            goto out_after_open;
        }
        if (rv != (int)entry->buf.len) {
            raft_errorf(uv->logger, "write %s: only %d bytes written", filename,
                        rv);
            rv = RAFT_ERR_IO;
            goto out_after_open;
        }
    }

    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(uv->logger, "fsync %s: %s", filename, strerror(errno));
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

/* Execute a truncate request in a thread. */
static void work_cb(uv_work_t *work)
{
    struct truncate *r = work->data;
    struct io_uv *uv = r->uv;
    struct io_uv__snapshot_meta *snapshots;
    struct io_uv__segment_meta *segments;
    struct io_uv__segment_meta *segment;
    size_t n_snapshots;
    size_t n_segments;
    size_t i;
    size_t j;
    int rv;

    /* Load all segments on disk. */
    rv = io_uv__load_list(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }
    if (snapshots != NULL) {
        raft_free(snapshots);
    }

    /* Look for the segment that contains the truncate point. */
    for (i = 0; i < n_segments; i++) {
        segment = &segments[i];

        if (segment->is_open) {
            continue;
        }

        if (r->index >= segment->first_index &&
            r->index <= segment->end_index) {
            break;
        }
    }

    /* If there's no segment at all to truncate, we're done. */
    if (i == n_segments) {
        goto out;
    }

    /* If the truncate index is not the first of the segment, we need to
     * truncate it. */
    if (r->index > segment->first_index) {
        rv = truncate_closed_segment(uv, segment, r->index);
        if (rv != 0) {
            goto err_after_list;
        }
    }

    for (j = i; j < n_segments; j++) {
        segment = &segments[j];

        if (segment->is_open) {
            continue;
        }

        rv = raft__io_uv_fs_unlink(uv->dir, segment->filename);
        if (rv != 0) {
            raft_errorf(uv->logger, "unlink segment %s: %s", segment->filename,
                        uv_strerror(rv));
            rv = RAFT_ERR_IO;
            goto err_after_list;
        }
    }

    rv = raft__io_uv_fs_sync_dir(uv->dir);
    if (rv != 0) {
        raft_errorf(uv->logger, "sync data directory: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_list;
    }

out:
    if (segments != NULL) {
        raft_free(segments);
    }

    r->status = 0;

    return;

err_after_list:
    raft_free(segments);

err:
    assert(rv != 0);

    r->status = rv;
}

static void after_work_cb(uv_work_t *work, int status)
{
    struct truncate *r = work->data;
    struct io_uv *uv = r->uv;

    assert(status == 0);

    if (r->status != 0) {
        uv->errored = true;
    }

    /* Update the finalizer last index setting it to the truncation index minus
     * one (i.e. the index of the last entry we truncated), since the next
     * segment to be finalized should start at the truncation index. */
    uv->finalize_last_index = r->index - 1;

    uv->truncate_work.data = NULL;
    raft_free(r);

    io_uv__append_unblock(uv);
    io_uv__snapshot_put_unblock(uv);
    io_uv__maybe_close(uv);
}

/* Start executing a truncate request. */
static int truncate_start(struct truncate *r)
{
    struct io_uv *uv = r->uv;
    int rv;

    assert(uv->truncate_work.data == NULL);
    uv->truncate_work.data = r;

    rv = uv_queue_work(uv->loop, &uv->truncate_work, work_cb, after_work_cb);
    if (rv != 0) {
        raft_errorf(uv->logger, "start to truncate log at index %lld: %s",
                    r->index, uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/* Process pending truncate requests. */
static void process_requests(struct io_uv *uv)
{
    struct truncate *r;
    raft__queue *head;
    int rv;

    /* If there are no truncate requests, there's nothing to do. */
    if (RAFT__QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        return;
    }

    /* If there are pending writes in progress, let's wait. */
    if (!RAFT__QUEUE_IS_EMPTY(&uv->append_writing_reqs)) {
        return;
    }

    /* If there are segments being closed, let's wait. */
    if (!RAFT__QUEUE_IS_EMPTY(&uv->finalize_reqs) ||
        uv->finalize_work.data != NULL) {
        return;
    }

    /* Pop the head of the queue */
    head = RAFT__QUEUE_HEAD(&uv->truncate_reqs);
    r = RAFT__QUEUE_DATA(head, struct truncate, queue);
    RAFT__QUEUE_REMOVE(&r->queue);

    rv = truncate_start(r);
    if (rv != 0) {
        uv->errored = true;
    }
}

int io_uv__truncate(struct raft_io *io, raft_index index)
{
    struct io_uv *uv;

    uv = io->impl;

    struct truncate *req;
    int rv;

    /* We should truncate only entries that we were requested to append in the
     * first place. */
    assert(index < uv->append_next_index);

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }
    req->uv = uv;
    req->index = index;

    /* The next entry will be appended at the truncation index. */
    uv->append_next_index = index;

    /* Make sure that we wait for any inflight writes to finish and then close
     * the current segment. */
    rv = io_uv__append_flush(uv);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    RAFT__QUEUE_PUSH(&uv->truncate_reqs, &req->queue);
    process_requests(uv);

    return 0;

err_after_req_alloc:
    raft_free(req);

err:
    assert(rv != 0);

    return rv;
}

void io_uv__truncate_unblock(struct io_uv *uv)
{
    process_requests(uv);
}

void io_uv__truncate_stop(struct io_uv *uv)
{
    while (!RAFT__QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        struct truncate *r;
        raft__queue *head;
        head = RAFT__QUEUE_HEAD(&uv->truncate_reqs);
        RAFT__QUEUE_REMOVE(head);
        r = RAFT__QUEUE_DATA(head, struct truncate, queue);
        raft_free(r);
    }
}
