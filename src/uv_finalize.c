#include "assert.h"
#include "heap.h"
#include "queue.h"
#include "uv.h"
#include "uv_os.h"

struct segment
{
    struct uv *uv;
    uvCounter counter;      /* Segment counter */
    size_t used;            /* Number of used bytes */
    raft_index first_index; /* Index of first entry */
    raft_index last_index;  /* Index of last entry */
    int status;             /* Status code of blocking syscalls */
    queue queue;            /* Link to finalize queue */
};

/* Run all blocking syscalls involved in closing a used open segment.
 *
 * An open segment is closed by truncating its length to the number of bytes
 * that were actually written into it and then renaming it. */
static void workCb(uv_work_t *work)
{
    struct segment *s = work->data;
    struct uv *uv = s->uv;
    char filename1[UV__FILENAME_LEN];
    char filename2[UV__FILENAME_LEN];
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    int rv;

    sprintf(filename1, UV__OPEN_TEMPLATE, s->counter);
    sprintf(filename2, UV__CLOSED_TEMPLATE, s->first_index, s->last_index);

    Tracef(uv->tracer, "finalize %s into %s", filename1, filename2);

    /* If the segment hasn't actually been used (because the writer has been
     * closed or aborted before making any write), just remove it. */
    if (s->used == 0) {
        rv = UvFsRemoveFile(uv->dir, filename1, errmsg);
        if (rv != 0) {
            goto err;
        }
        goto sync;
    }

    /* Truncate and rename the segment.*/
    rv = UvFsTruncateAndRenameFile(uv->dir, s->used, filename1, filename2,
                                   errmsg);
    if (rv != 0) {
        goto err;
    }

sync:
    rv = UvFsSyncDir(uv->dir, errmsg);
    if (rv != 0) {
        goto err;
    }

    s->status = 0;
    return;

err:
    Tracef(uv->tracer, "truncate segment %s: %s", filename1, errmsg);
    assert(rv != 0);
    s->status = rv;
}

static void processRequests(struct uv *uv);
static void afterWorkCb(uv_work_t *work, int status)
{
    struct segment *s = work->data;
    struct uv *uv = s->uv;
    assert(status == 0); /* We don't cancel worker requests */
    uv->finalize_work.data = NULL;
    if (s->status != 0) {
        uv->errored = true;
    }
    HeapFree(s);

    if (QUEUE_IS_EMPTY(&uv->finalize_reqs)) {
        if (uv->closing) {
            uvMaybeFireCloseCb(uv);
            return;
        }
        if (uv->barrier != NULL) {
            uv->barrier->cb(uv->barrier);
            return;
        }
    }

    processRequests(uv);
}

/* Schedule finalizing an open segment. */
static int finalizeSegment(struct segment *s)
{
    struct uv *uv = s->uv;
    int rv;

    assert(uv->finalize_work.data == NULL);
    assert(s->counter > 0);

    uv->finalize_work.data = s;

    rv = uv_queue_work(uv->loop, &uv->finalize_work, workCb, afterWorkCb);
    if (rv != 0) {
        Tracef(uv->tracer, "start to truncate segment file %d: %s", s->counter,
               uv_strerror(rv));
        return RAFT_IOERR;
    }

    return 0;
}

/* Process pending requests to finalize open segments */
static void processRequests(struct uv *uv)
{
    struct segment *segment;
    queue *head;
    int rv;

    /* If we're already processing a segment, let's wait. */
    if (uv->finalize_work.data != NULL) {
        return;
    }

    /* If there's no pending request, we're done. */
    if (QUEUE_IS_EMPTY(&uv->finalize_reqs)) {
        return;
    }

    head = QUEUE_HEAD(&uv->finalize_reqs);
    segment = QUEUE_DATA(head, struct segment, queue);
    QUEUE_REMOVE(&segment->queue);

    rv = finalizeSegment(segment);
    if (rv != 0) {
        goto err;
    }

    return;
err:
    assert(rv != 0);

    uv->errored = true;
}

int uvFinalize(struct uv *uv,
               unsigned long long counter,
               size_t used,
               raft_index first_index,
               raft_index last_index)
{
    struct segment *segment;

    /* If the open segment is not empty, we expect its first index to be the
     * successor of the end index of the last segment we closed. */
    if (used > 0) {
        assert(first_index > 0);
        assert(last_index >= first_index);
    }

    segment = HeapMalloc(sizeof *segment);
    if (segment == NULL) {
        return RAFT_NOMEM;
    }

    segment->uv = uv;
    segment->counter = counter;
    segment->used = used;
    segment->first_index = first_index;
    segment->last_index = last_index;

    QUEUE_INIT(&segment->queue);
    QUEUE_PUSH(&uv->finalize_reqs, &segment->queue);

    processRequests(uv);

    return 0;
}
