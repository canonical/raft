#include <string.h>
#include <sys/uio.h>

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "io_uv.h"
#include "io_uv_fs.h"
#include "io_uv_load.h"
#include "logging.h"

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define SNAPSHOT_TEMPLATE "snapshot-%llu-%llu-%llu"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define SNAPSHOT_META_TEMPLATE SNAPSHOT_TEMPLATE ".meta"

struct put
{
    struct io_uv *uv;
    struct raft_io_snapshot_put *req;
    const struct raft_snapshot *snapshot;
    struct
    {
        unsigned long long timestamp;
        uint64_t header[4];         /* Format, CRC, configuration index/len */
        struct raft_buffer bufs[2]; /* Premable and configuration */
    } meta;
    int status;
    raft__queue queue;
};

struct get
{
    struct io_uv *uv;
    struct raft_io_snapshot_get *req;
    struct raft_snapshot *snapshot;
    struct uv_work_s work;
    int status;
    raft__queue queue;
};

static int write_file(struct raft_io *io,
                      const char *dir,
                      const char *filename,
                      struct raft_buffer *bufs,
                      unsigned n_bufs)
{
    int flags = O_WRONLY | O_CREAT | O_EXCL;
    int fd;
    int rv;
    size_t size;
    unsigned i;

    size = 0;
    for (i = 0; i < n_bufs; i++) {
        size += bufs[i].len;
    }

    fd = raft__io_uv_fs_open(dir, filename, flags);
    if (fd == -1) {
        errorf(io, "open %s: %s", filename, uv_strerror(-errno));
        return RAFT_ERR_IO;
    }

    rv = writev(fd, (const struct iovec *)bufs, n_bufs);
    if (rv != (int)(size)) {
        errorf(io, "write %s: %s", filename, uv_strerror(-errno));
        goto err_after_file_open;
    }

    rv = fsync(fd);
    if (rv == -1) {
        errorf(io, "fsync %s: %s", filename, uv_strerror(-errno));
        goto err_after_file_open;
    }

    rv = close(fd);
    if (rv == -1) {
        errorf(io, "close %s: %s", filename, uv_strerror(-errno));
        goto err;
    }

    return 0;

err_after_file_open:
    close(fd);
err:
    return RAFT_ERR_IO;
}

/* TODO: remove code duplication with io_uv_load.c */
static void snapshot_data_filename(struct io_uv__snapshot_meta *meta,
                                   io_uv__filename filename)
{
    size_t len = strlen(meta->filename) - strlen(".meta");
    strncpy(filename, meta->filename, len);
    filename[len] = 0;
}

/* Remove all segmens and snapshots that are not needed anymore, but ignore
 * errors.
 *
 * TODO: remove code duplication with io_uv_load.c */
static int remove_old_segments_and_snapshots(struct io_uv *uv,
                                             raft_index last_index)
{
    struct io_uv__snapshot_meta *snapshots;
    struct io_uv__segment_meta *segments;
    size_t n_snapshots;
    size_t n_segments;
    size_t i;
    int rv = 0;

    rv = io_uv__load_list(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto out;
    }

    /* Leave at least two snapshots, for safety. */
    if (n_snapshots > 2) {
        for (i = 0; i < n_snapshots - 2; i++) {
            struct io_uv__snapshot_meta *s = &snapshots[i];
            io_uv__filename filename;
            rv = raft__io_uv_fs_unlink(uv->dir, s->filename);
            if (rv != 0) {
                goto out;
            }
            snapshot_data_filename(s, filename);
            rv = raft__io_uv_fs_unlink(uv->dir, filename);
            if (rv != 0) {
                goto out;
            }
        }
    }

    /* Remove all unused closed segments */
    for (i = 0; i < n_segments; i++) {
        struct io_uv__segment_meta *segment = &segments[i];

        if (segment->is_open) {
            continue;
        }

        if (segment->end_index < last_index) {
            rv = raft__io_uv_fs_unlink(uv->dir, segment->filename);
            if (rv != 0) {
                goto out;
            }
        }
    }

out:
    if (snapshots != NULL) {
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }

    return rv;
}

static void put_work_cb(uv_work_t *work)
{
    struct put *r = work->data;
    struct io_uv *uv = r->uv;
    io_uv__filename filename;
    int rv;

    sprintf(filename, SNAPSHOT_META_TEMPLATE, r->snapshot->term,
            r->snapshot->index, r->meta.timestamp);

    rv = write_file(uv->io, uv->dir, filename, r->meta.bufs, 2);
    if (rv != 0) {
        r->status = rv;
        return;
    }

    sprintf(filename, SNAPSHOT_TEMPLATE, r->snapshot->term, r->snapshot->index,
            r->meta.timestamp);

    rv = write_file(uv->io, uv->dir, filename, r->snapshot->bufs,
                    r->snapshot->n_bufs);
    if (rv != 0) {
        r->status = rv;
        return;
    }

    rv = raft__io_uv_fs_sync_dir(uv->dir);
    if (rv != 0) {
        r->status = rv;
        return;
    }

    rv = remove_old_segments_and_snapshots(uv, r->snapshot->index);
    if (rv != 0) {
        r->status = rv;
    }

    r->status = 0;

    return;
}

static void put_after_work_cb(uv_work_t *work, int status)
{
    struct put *r = work->data;
    struct io_uv *uv = r->uv;

    assert(status == 0);
    RAFT__QUEUE_REMOVE(&r->queue);
    uv->snapshot_put_work.data = NULL;

    r->req->cb(r->req, r->status);

    raft_free(r->meta.bufs[1].base);
    raft_free(r);

    io_uv__maybe_close(uv);
}

/* Process pending put requests. */
static void process_put_requests(struct io_uv *uv)
{
    struct put *r;
    raft__queue *head;
    int rv;

    /* If there aren't pending snapshot put requests, there's nothing to do. */
    if (RAFT__QUEUE_IS_EMPTY(&uv->snapshot_put_reqs)) {
        return;
    }

    /* If we're already writing a snapshot, let's wait. */
    if (uv->snapshot_put_work.data != NULL) {
        return;
    }

    /* If there's a pending truncate request, let's wait. Typically the truncate
     * request is initiated by the InstallSnapshot RPC handler. */
    if (uv->truncate_work.data != NULL ||
        !RAFT__QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        return;
    }

    /* Get the head of the queue */
    head = RAFT__QUEUE_HEAD(&uv->snapshot_put_reqs);
    r = RAFT__QUEUE_DATA(head, struct put, queue);

    uv->snapshot_put_work.data = r;
    rv = uv_queue_work(uv->loop, &uv->snapshot_put_work, put_work_cb,
                       put_after_work_cb);
    if (rv != 0) {
        errorf(uv->io, "store snapshot %lld: %s", r->snapshot->index,
               uv_strerror(rv));
        uv->errored = true;
    }
}

int io_uv__snapshot_put(struct raft_io *io,
                        struct raft_io_snapshot_put *req,
                        const struct raft_snapshot *snapshot,
                        raft_io_snapshot_put_cb cb)
{
    struct io_uv *uv;
    struct put *r;
    void *cursor;
    unsigned crc;
    int rv;

    uv = io->impl;

    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }
    r->uv = uv;
    r->req = req;
    r->snapshot = snapshot;
    r->meta.timestamp = uv_now(uv->loop);

    req->cb = cb;

    /* Prepare the buffers for the metadata file. */
    r->meta.bufs[0].base = r->meta.header;
    r->meta.bufs[0].len = sizeof r->meta.header;

    rv = configuration__encode(&snapshot->configuration, &r->meta.bufs[1]);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    cursor = r->meta.header;
    byte__put64(&cursor, IO_UV__DISK_FORMAT);
    byte__put64(&cursor, 0);
    byte__put64(&cursor, snapshot->configuration_index);
    byte__put64(&cursor, r->meta.bufs[1].len);

    crc = byte__crc32(&r->meta.header[2], sizeof(uint64_t) * 2, 0);
    crc = byte__crc32(r->meta.bufs[1].base, r->meta.bufs[1].len, crc);

    cursor = &r->meta.header[1];
    byte__put64(&cursor, crc);

    RAFT__QUEUE_PUSH(&uv->snapshot_put_reqs, &r->queue);
    process_put_requests(uv);

    return 0;

err_after_req_alloc:
    raft_free(r);
err:
    assert(rv != 0);
    return rv;
}

void io_uv__snapshot_put_unblock(struct io_uv *uv)
{
    process_put_requests(uv);
}

static void get_work_cb(uv_work_t *work)
{
    struct get *r = work->data;
    struct io_uv *uv = r->uv;
    struct io_uv__snapshot_meta *snapshots;
    size_t n_snapshots;
    struct io_uv__segment_meta *segments;
    size_t n_segments;
    int rv;

    rv = io_uv__load_list(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        r->status = rv;
        goto out;
    }

    if (snapshots != NULL) {
        rv = io_uv__load_snapshot(uv, &snapshots[n_snapshots - 1], r->snapshot);
        if (rv != 0) {
            r->status = rv;
        }
        raft_free(snapshots);
    }

    if (segments != NULL) {
        raft_free(segments);
    }

    r->status = 0;

out:
    return;
}

static void get_after_work_cb(uv_work_t *work, int status)
{
    struct get *r = work->data;
    struct io_uv *uv = r->uv;
    assert(status == 0);

    RAFT__QUEUE_REMOVE(&r->queue);

    r->req->cb(r->req, r->snapshot, r->status);
    raft_free(r);

    io_uv__maybe_close(uv);
}

int io_uv__snapshot_get(struct raft_io *io,
                        struct raft_io_snapshot_get *req,
                        raft_io_snapshot_get_cb cb)
{
    struct io_uv *uv;
    struct get *r;
    int rv;

    uv = io->impl;

    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }
    r->uv = uv;
    r->req = req;
    req->cb = cb;

    r->snapshot = raft_malloc(sizeof *r->snapshot);
    if (r->snapshot == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_req_alloc;
    }
    r->work.data = r;

    RAFT__QUEUE_PUSH(&uv->snapshot_get_reqs, &r->queue);
    rv = uv_queue_work(uv->loop, &r->work, get_work_cb, get_after_work_cb);
    if (rv != 0) {
        errorf(uv->io, "get last snapshot: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_snapshot_alloc;
    }

    return 0;

err_after_snapshot_alloc:
    raft_free(r->snapshot);
err_after_req_alloc:
    raft_free(r);
err:
    assert(rv != 0);
    return rv;
}
