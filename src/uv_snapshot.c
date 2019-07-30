#include <string.h>
#include <sys/uio.h>

#include "array.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "logging.h"
#include "uv.h"
#include "uv_os.h"

/* Template string for snapshot filenames: snapshot term, snapshot index,
 * creation timestamp (milliseconds since epoch). */
#define TEMPLATE "snapshot-%llu-%llu-%llu"

/* Template string for snapshot metadata filenames: snapshot term,  snapshot
 * index, creation timestamp (milliseconds since epoch). */
#define META_TEMPLATE TEMPLATE ".meta"

/* Arbitrary maximum configuration size. Should be practically be enough */
#define META_MAX_CONFIGURATION_SIZE 1024 * 1024

/* Check if the given filename matches the one of a snapshot metadata filename
 * (snapshot-xxx-yyy-zzz.meta), and fill the given info structure if so.
 *
 * Return true if the filename matched, false otherwise. */
static bool infoMatch(const char *filename, struct uvSnapshotInfo *info)
{
    unsigned consumed = 0;
    int matched;
    size_t filename_len = strnlen(filename, UV__FILENAME_MAX_LEN + 1);

    if (filename_len > UV__FILENAME_MAX_LEN) {
        return false;
    }

    matched = sscanf(filename, META_TEMPLATE "%n", &info->term, &info->index,
                     &info->timestamp, &consumed);
    if (matched != 3 || consumed != filename_len) {
        return false;
    }

    strcpy(info->filename, filename);
    return true;
}

/* Render the filename of the data file of a snapshot */
static void filenameOf(struct uvSnapshotInfo *info, uvFilename filename)
{
    size_t len = strlen(info->filename) - strlen(".meta");
    assert(len < UV__FILENAME_MAX_LEN);
    strncpy(filename, info->filename, len);
    filename[len] = 0;
}

int uvSnapshotInfoAppendIfMatch(struct uv *uv,
                                const char *filename,
                                struct uvSnapshotInfo *infos[],
                                size_t *n_infos,
                                bool *appended)
{
    struct uvSnapshotInfo info;
    bool matched;
    struct stat sb;
    uvFilename snapshot_filename;
    int rv;

    /* Check if it's a snapshot metadata filename */
    matched = infoMatch(filename, &info);
    if (!matched) {
        *appended = false;
        return 0;
    }

    /* Check if there's actually a snapshot file for this snapshot metadata. If
     * there's none, it means that we aborted before finishing the snapshot, so
     * let's remove the metadata file. */
    filenameOf(&info, snapshot_filename);
    rv = uvStat(uv->dir, snapshot_filename, &sb);
    if (rv != 0) {
        if (rv == ENOENT) {
            uvUnlink(uv->dir, filename); /* Ignore errors */
            *appended = false;
            return 0;
        }
        uvErrorf(uv, "stat %s: %s", snapshot_filename, osStrError(rv));
        return RAFT_IOERR;
    }

    ARRAY__APPEND(struct uvSnapshotInfo, info, infos, n_infos, rv);
    if (rv == -1) {
        return RAFT_NOMEM;
    }

    *appended = true;

    return 0;
}

/* Compare two snapshots to decide which one is more recent. */
static int compare(const void *p1, const void *p2)
{
    struct uvSnapshotInfo *s1 = (struct uvSnapshotInfo *)p1;
    struct uvSnapshotInfo *s2 = (struct uvSnapshotInfo *)p2;

    /* If terms are different, the snaphot with the highest term is the most
     * recent. */
    if (s1->term != s2->term) {
        return s1->term < s2->term ? -1 : 1;
    }

    /* If the term are identical and the index differ, the snapshot with the
     * highest index is the most recent */
    if (s1->index != s2->index) {
        return s1->index < s2->index ? -1 : 1;
    }

    /* If term and index are identical, compare the timestamp. */
    return s1->timestamp < s2->timestamp ? -1 : 1;
}

void uvSnapshotSort(struct uvSnapshotInfo *infos, size_t n_infos)
{
    qsort(infos, n_infos, sizeof *infos, compare);
}

/* Parse the metadata file of a snapshot and populate the given snapshot object
 * accordingly. */
static int loadMeta(struct uv *uv,
                    struct uvSnapshotInfo *info,
                    struct raft_snapshot *snapshot)
{
    uint64_t header[1 + /* Format version */
                    1 + /* CRC checksum */
                    1 + /* Configuration index */
                    1 /* Configuration length */];
    struct raft_buffer buf;
    unsigned format;
    unsigned crc1;
    unsigned crc2;
    int fd;
    int rv;

    snapshot->term = info->term;
    snapshot->index = info->index;

    rv = uvOpen(uv->dir, info->filename, O_RDONLY, &fd);
    if (rv != 0) {
        uvErrorf(uv, "open %s: %s", info->filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }
    rv = uvReadN(fd, header, sizeof header);
    if (rv != 0) {
        uvErrorf(uv, "read %s: %s", info->filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err_after_open;
    }

    format = byteFlip64(header[0]);
    if (format != UV__DISK_FORMAT) {
        uvErrorf(uv, "load %s: unsupported format %lu", info->filename, format);
        rv = RAFT_MALFORMED;
        goto err_after_open;
    }

    crc1 = byteFlip64(header[1]);

    snapshot->configuration_index = byteFlip64(header[2]);
    buf.len = byteFlip64(header[3]);
    if (buf.len > META_MAX_CONFIGURATION_SIZE) {
        uvErrorf(uv, "load %s: configuration data too big (%ld)",
                 info->filename, buf.len);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }
    if (buf.len == 0) {
        uvErrorf(uv, "load %s: no configuration data", info->filename, buf.len);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_open;
    }

    rv = uvReadN(fd, buf.base, buf.len);
    if (rv != 0) {
        uvErrorf(uv, "read %s: %s", info->filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err_after_buf_malloc;
    }

    crc2 = byteCrc32(header + 2, sizeof header - sizeof(uint64_t) * 2, 0);
    crc2 = byteCrc32(buf.base, buf.len, crc2);

    if (crc1 != crc2) {
        uvErrorf(uv, "read %s: checksum mismatch", info->filename);
        rv = RAFT_CORRUPT;
        goto err_after_open;
    }

    raft_configuration_init(&snapshot->configuration);
    rv = configurationDecode(&buf, &snapshot->configuration);
    if (rv != 0) {
        goto err_after_buf_malloc;
    }

    raft_free(buf.base);
    close(fd);

    return 0;

err_after_buf_malloc:
    raft_free(buf.base);

err_after_open:
    close(fd);

err:
    assert(rv != 0);
    return rv;
}

/* Load the snapshot data file. */
static int loadData(struct uv *uv,
                    struct uvSnapshotInfo *info,
                    struct raft_snapshot *snapshot)
{
    struct stat sb;
    uvFilename filename;
    struct raft_buffer buf;
    int fd;
    int rv;

    filenameOf(info, filename);

    rv = uvStat(uv->dir, filename, &sb);
    if (rv != 0) {
        uvErrorf(uv, "stat %s: %s", filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }

    rv = uvOpen(uv->dir, filename, O_RDONLY, &fd);
    if (rv != 0) {
        uvErrorf(uv, "open %s: %s", filename, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }

    buf.len = sb.st_size;
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_open;
    }

    rv = uvReadN(fd, buf.base, buf.len);
    if (rv != 0) {
        uvErrorf(uv, "read %s: %s", filename, osStrError(rv));
        goto err_after_buf_alloc;
    }

    snapshot->bufs = raft_malloc(sizeof *snapshot->bufs);
    snapshot->n_bufs = 1;
    if (snapshot->bufs == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_buf_alloc;
    }

    snapshot->bufs[0] = buf;

    close(fd);

    return 0;

err_after_buf_alloc:
    raft_free(buf.base);

err_after_open:
    close(fd);

err:
    assert(rv != 0);
    return rv;
}

int uvSnapshotLoad(struct uv *uv,
                   struct uvSnapshotInfo *meta,
                   struct raft_snapshot *snapshot)
{
    int rv;
    rv = loadMeta(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }
    rv = loadData(uv, meta, snapshot);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

struct put
{
    struct uv *uv;
    struct raft_io_snapshot_put *req;
    const struct raft_snapshot *snapshot;
    struct
    {
        unsigned long long timestamp;
        uint64_t header[4];         /* Format, CRC, configuration index/len */
        struct raft_buffer bufs[2]; /* Premable and configuration */
    } meta;
    int status;
    queue queue;
};

struct get
{
    struct uv *uv;
    struct raft_io_snapshot_get *req;
    struct raft_snapshot *snapshot;
    struct uv_work_s work;
    int status;
    queue queue;
};

/* Remove all segmens and snapshots that are not needed anymore, but ignore
 * errors.
 *
 * TODO: remove code duplication with io_uv_load.c */
static int removeOldSegmentsAndSnapshots(struct uv *uv, raft_index last_index)
{
    struct uvSnapshotInfo *snapshots;
    struct uvSegmentInfo *segments;
    size_t n_snapshots;
    size_t n_segments;
    size_t i;
    int rv = 0;

    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto out;
    }

    /* Leave at least two snapshots, for safety. */
    if (n_snapshots > 2) {
        for (i = 0; i < n_snapshots - 2; i++) {
            struct uvSnapshotInfo *s = &snapshots[i];
            uvFilename filename;
            rv = uvUnlink(uv->dir, s->filename);
            if (rv != 0) {
                uvErrorf(uv, "unlink %s: %s", s->filename, osStrError(rv));
                rv = RAFT_IOERR;
                goto out;
            }
            filenameOf(s, filename);
            rv = uvUnlink(uv->dir, filename);
            if (rv != 0) {
                uvErrorf(uv, "unlink %s: %s", filename, osStrError(rv));
                rv = RAFT_IOERR;
                goto out;
            }
        }
    }

    /* Remove all unused closed segments */
    for (i = 0; i < n_segments; i++) {
        struct uvSegmentInfo *segment = &segments[i];
        if (segment->is_open) {
            continue;
        }
        if (segment->end_index < last_index) {
            rv = uvUnlink(uv->dir, segment->filename);
            if (rv != 0) {
                uvErrorf(uv, "unlink %s: %s", segment->filename,
                         osStrError(rv));
                rv = RAFT_IOERR;
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

static void putWorkCb(uv_work_t *work)
{
    struct put *r = work->data;
    struct uv *uv = r->uv;
    uvFilename filename;
    int rv;

    sprintf(filename, META_TEMPLATE, r->snapshot->term, r->snapshot->index,
            r->meta.timestamp);

    rv = osCreateFile(uv->dir, filename, r->meta.bufs, 2);
    if (rv != 0) {
        uvErrorf(uv, "write %s: %s", filename, osStrError(rv));
        r->status = RAFT_IOERR;
        return;
    }

    sprintf(filename, TEMPLATE, r->snapshot->term, r->snapshot->index,
            r->meta.timestamp);

    rv =
        osCreateFile(uv->dir, filename, r->snapshot->bufs, r->snapshot->n_bufs);
    if (rv != 0) {
        uvErrorf(uv, "write %s: %s", filename, osStrError(rv));
        r->status = RAFT_IOERR;
        return;
    }

    rv = removeOldSegmentsAndSnapshots(uv, r->snapshot->index);
    if (rv != 0) {
        r->status = rv;
        return;
    }

    rv = uvSyncDir(uv->dir);
    if (rv != 0) {
        uvErrorf(uv, "sync %s: %s", uv->dir, osStrError(rv));
        r->status = RAFT_IOERR;
        return;
    }

    r->status = 0;

    return;
}

static void putAfterWorkCb(uv_work_t *work, int status)
{
    struct put *r = work->data;
    struct uv *uv = r->uv;

    assert(status == 0);
    QUEUE_REMOVE(&r->queue);
    uv->snapshot_put_work.data = NULL;

    r->req->cb(r->req, r->status);

    raft_free(r->meta.bufs[1].base);
    raft_free(r);

    uvMaybeClose(uv);
}

/* Process pending put requests. */
static void processPutRequests(struct uv *uv)
{
    struct put *r;
    queue *head;
    int rv;

    /* If we're already writing a snapshot, let's wait. */
    if (uv->snapshot_put_work.data != NULL) {
        return;
    }

    /* If there's a pending truncate request, let's wait. Typically the truncate
     * request is initiated by the InstallSnapshot RPC handler. */
    if (uv->truncate_work.data != NULL || !QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        return;
    }

    /* Get the head of the queue */
    head = QUEUE_HEAD(&uv->snapshot_put_reqs);
    r = QUEUE_DATA(head, struct put, queue);

    /* Detect if we're being run just after a truncate request in order to
     * restore a snaphost, in that case we want to adjust the finalize last
     * index accordingly.
     *
     * TODO: this doesn't work in all cases. Reason about exact sequence of
     * events, make logic more elegant and robust.  */
    if (uv->finalize_last_index == 0) {
        uv->finalize_last_index = r->snapshot->index;
    }

    uv->snapshot_put_work.data = r;
    rv = uv_queue_work(uv->loop, &uv->snapshot_put_work, putWorkCb,
                       putAfterWorkCb);
    if (rv != 0) {
        uvErrorf(uv, "store snapshot %lld: %s", r->snapshot->index,
                 uv_strerror(rv));
        uv->errored = true;
    }
}

int uvSnapshotPut(struct raft_io *io,
                  struct raft_io_snapshot_put *req,
                  const struct raft_snapshot *snapshot,
                  raft_io_snapshot_put_cb cb)
{
    struct uv *uv;
    struct put *r;
    void *cursor;
    unsigned crc;
    int rv;

    uv = io->impl;

    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_NOMEM;
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

    rv = configurationEncode(&snapshot->configuration, &r->meta.bufs[1]);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    /* If the next append index is set to 1, it means that we're restoring a
     * snapshot after having trucated the log. Set the next append index to the
     * snapshot's last index + 1. */
    if (uv->append_next_index == 1) {
        uv->append_next_index = snapshot->index + 1;
        /* We expect that a new prepared segment has just been requested, we
         * need to update its first index too.
         *
         * TODO: this should be cleaned up. */
        uvAppendFixPreparedSegmentFirstIndex(uv);
    }

    cursor = r->meta.header;
    bytePut64(&cursor, UV__DISK_FORMAT);
    bytePut64(&cursor, 0);
    bytePut64(&cursor, snapshot->configuration_index);
    bytePut64(&cursor, r->meta.bufs[1].len);

    crc = byteCrc32(&r->meta.header[2], sizeof(uint64_t) * 2, 0);
    crc = byteCrc32(r->meta.bufs[1].base, r->meta.bufs[1].len, crc);

    cursor = &r->meta.header[1];
    bytePut64(&cursor, crc);

    QUEUE_PUSH(&uv->snapshot_put_reqs, &r->queue);
    processPutRequests(uv);

    return 0;

err_after_req_alloc:
    raft_free(r);
err:
    assert(rv != 0);
    return rv;
}

void uvSnapshotMaybeProcessRequests(struct uv *uv)
{
    /* If there aren't pending snapshot put requests, there's nothing to do. */
    if (QUEUE_IS_EMPTY(&uv->snapshot_put_reqs)) {
        return;
    }
    processPutRequests(uv);
}

static void getWorkCb(uv_work_t *work)
{
    struct get *r = work->data;
    struct uv *uv = r->uv;
    struct uvSnapshotInfo *snapshots;
    size_t n_snapshots;
    struct uvSegmentInfo *segments;
    size_t n_segments;
    int rv;

    r->status = 0;

    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        r->status = rv;
        goto out;
    }
    if (snapshots != NULL) {
        rv = uvSnapshotLoad(uv, &snapshots[n_snapshots - 1], r->snapshot);
        if (rv != 0) {
            r->status = rv;
        }
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }
out:
    return;
}

static void getAfterWorkCb(uv_work_t *work, int status)
{
    struct get *r = work->data;
    struct uv *uv = r->uv;
    assert(status == 0);
    QUEUE_REMOVE(&r->queue);
    r->req->cb(r->req, r->snapshot, r->status);
    raft_free(r);
    uvMaybeClose(uv);
}

int uvSnapshotGet(struct raft_io *io,
                  struct raft_io_snapshot_get *req,
                  raft_io_snapshot_get_cb cb)
{
    struct uv *uv;
    struct get *r;
    int rv;

    uv = io->impl;

    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    r->uv = uv;
    r->req = req;
    req->cb = cb;

    r->snapshot = raft_malloc(sizeof *r->snapshot);
    if (r->snapshot == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_req_alloc;
    }
    r->work.data = r;

    QUEUE_PUSH(&uv->snapshot_get_reqs, &r->queue);
    rv = uv_queue_work(uv->loop, &r->work, getWorkCb, getAfterWorkCb);
    if (rv != 0) {
        uvErrorf(uv, "get last snapshot: %s", uv_strerror(rv));
        rv = RAFT_IOERR;
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
