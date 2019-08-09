#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "byte.h"
#include "logging.h"
#include "uv.h"
#include "uv_encoding.h"

struct truncate
{
    struct uv *uv;
    raft_index index;
    int status;
    queue queue;
};

/* Execute a truncate request in a thread. */
static void workCb(uv_work_t *work)
{
    struct truncate *r = work->data;
    struct uv *uv = r->uv;
    struct uvSnapshotInfo *snapshots;
    struct uvSegmentInfo *segments;
    struct uvSegmentInfo *segment;
    size_t n_snapshots;
    size_t n_segments;
    size_t i;
    size_t j;
    char errmsg[2048];
    int rv;

    /* Load all segments on disk. */
    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
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
        rv = uvSegmentTruncate(uv, segment, r->index);
        if (rv != 0) {
            goto err_after_list;
        }
    }

    for (j = i; j < n_segments; j++) {
        segment = &segments[j];

        if (segment->is_open) {
            continue;
        }

        rv = uvUnlinkFile(uv->dir, segment->filename, errmsg);
        if (rv != 0) {
            uvErrorf(uv, "unlink segment %s: %s", segment->filename,
                     uv_strerror(rv));
            rv = RAFT_IOERR;
            goto err_after_list;
        }
    }

    rv = uvSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "sync data directory: %s", uv_strerror(rv));
        rv = RAFT_IOERR;
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

static void afterWorkCb(uv_work_t *work, int status)
{
    struct truncate *r = work->data;
    struct uv *uv = r->uv;

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

    uvAppendMaybeProcessRequests(uv);
    uvSnapshotMaybeProcessRequests(uv);
    uvMaybeClose(uv);
}

/* Process pending truncate requests. */
static void processRequests(struct uv *uv)
{
    struct truncate *r;
    queue *head;
    int rv;

    /* If there are pending writes in progress, let's wait. */
    if (!QUEUE_IS_EMPTY(&uv->append_writing_reqs)) {
        return;
    }

    /* If there are segments being closed, let's wait. */
    if (!QUEUE_IS_EMPTY(&uv->finalize_reqs) || uv->finalize_work.data != NULL) {
        return;
    }

    /* Pop the head of the queue */
    head = QUEUE_HEAD(&uv->truncate_reqs);
    r = QUEUE_DATA(head, struct truncate, queue);
    QUEUE_REMOVE(&r->queue);

    uv->truncate_work.data = r;
    rv = uv_queue_work(uv->loop, &uv->truncate_work, workCb, afterWorkCb);
    if (rv != 0) {
        uvErrorf(uv, "truncate index %lld: %s", r->index, uv_strerror(rv));
        uv->truncate_work.data = NULL;
        uv->errored = true;
    }
}

int uvTruncate(struct raft_io *io, raft_index index)
{
    struct uv *uv;
    struct truncate *req;
    int rv;

    uv = io->impl;

    assert(!uv->closing);

    /* We should truncate only entries that we were requested to append in the
     * first place. */
    assert(index <= uv->append_next_index);

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    req->uv = uv;
    req->index = index;

    /* The next entry will be appended at the truncation index. */
    uv->append_next_index = index;

    /* Make sure that we wait for any inflight writes to finish and then close
     * the current segment. */
    rv = uvAppendForceFinalizingCurrentSegment(uv);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    QUEUE_PUSH(&uv->truncate_reqs, &req->queue);
    processRequests(uv);

    return 0;

err_after_req_alloc:
    raft_free(req);
err:
    assert(rv != 0);
    return rv;
}

void uvTruncateMaybeProcessRequests(struct uv *uv)
{
    /* If there are no truncate requests, there's nothing to do. */
    if (QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        return;
    }
    processRequests(uv);
}

void uvTruncateClose(struct uv *uv)
{
    while (!QUEUE_IS_EMPTY(&uv->truncate_reqs)) {
        struct truncate *r;
        queue *head;
        head = QUEUE_HEAD(&uv->truncate_reqs);
        QUEUE_REMOVE(head);
        r = QUEUE_DATA(head, struct truncate, queue);
        raft_free(r);
    }
}
