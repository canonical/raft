#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "byte.h"
#include "heap.h"
#include "logging.h"
#include "uv.h"
#include "uv_encoding.h"

/* Track a truncate request. */
struct uvTruncate
{
    struct uv *uv;
    struct UvBarrier barrier;
    raft_index index;
    int status;
};

/* Execute a truncate request in a thread. */
static void workCb(uv_work_t *work)
{
    struct uvTruncate *r = work->data;
    struct uv *uv = r->uv;
    struct uvSnapshotInfo *snapshots;
    struct uvSegmentInfo *segments;
    struct uvSegmentInfo *segment;
    size_t n_snapshots;
    size_t n_segments;
    size_t i;
    size_t j;
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    /* Load all segments on disk. */
    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }
    if (snapshots != NULL) {
        HeapFree(snapshots);
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

        rv = UvFsRemoveFile(uv->dir, segment->filename, errmsg);
        if (rv != 0) {
            Tracef(uv->tracer, "unlink segment %s: %s", segment->filename,
                   errmsg);
            rv = RAFT_IOERR;
            goto err_after_list;
        }
    }

    rv = UvFsSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        Tracef(uv->tracer, "sync data directory: %s", errmsg);
        rv = RAFT_IOERR;
        goto err_after_list;
    }

out:
    if (segments != NULL) {
        HeapFree(segments);
    }

    r->status = 0;

    return;

err_after_list:
    HeapFree(segments);

err:
    assert(rv != 0);

    r->status = rv;
}

static void afterWorkCb(uv_work_t *work, int status)
{
    struct uvTruncate *r = work->data;
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
    HeapFree(r);

    UvUnblock(uv);
    uvSnapshotMaybeProcessRequests(uv);
    uvMaybeFireCloseCb(uv);
}

static void uvTruncateBarrierCb(struct UvBarrier *barrier)
{
    struct uvTruncate *truncate = barrier->data;
    struct uv *uv = truncate->uv;
    int rv;

    /* If we're closing, don't perform truncation at all and abort here. */
    if (uv->closing) {
        HeapFree(truncate);
        return;
    }

    assert(QUEUE_IS_EMPTY(&uv->append_writing_reqs));
    assert(QUEUE_IS_EMPTY(&uv->finalize_reqs));
    assert(uv->finalize_work.data == NULL);

    uv->truncate_work.data = truncate;
    rv = uv_queue_work(uv->loop, &uv->truncate_work, workCb, afterWorkCb);
    if (rv != 0) {
        Tracef(uv->tracer, "truncate index %lld: %s", truncate->index,
               uv_strerror(rv));
        uv->truncate_work.data = NULL;
        uv->errored = true;
    }
}

int uvTruncate(struct raft_io *io, raft_index index)
{
    struct uv *uv;
    struct uvTruncate *req;
    int rv;

    uv = io->impl;

    assert(!uv->closing);

    /* We should truncate only entries that we were requested to append in the
     * first place. */
    assert(index <= uv->append_next_index);

    req = HeapMalloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    req->uv = uv;
    req->index = index;
    req->barrier.data = req;

    /* Make sure that we wait for any inflight writes to finish and then close
     * the current segment. */
    rv = UvBarrier(uv, index, &req->barrier, uvTruncateBarrierCb);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    return 0;

err_after_req_alloc:
    HeapFree(req);
err:
    assert(rv != 0);
    return rv;
}

void uvTruncateClose(struct uv *uv)
{
    (void)uv;
}
