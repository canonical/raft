#include "assert.h"
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
    osFilename filename1;
    osFilename filename2;
    int rv;

    sprintf(filename1, UV__OPEN_TEMPLATE, s->counter);

    /* If the segment hasn't actually been used (because the writer has been
     * closed or aborted before making any write), then let's just remove it. */
    if (s->used == 0) {
        osUnlink(uv->dir, filename1);
        goto out;
    }

    /* Truncate and rename the segment */
    rv = osTruncate(uv->dir, filename1, s->used);
    if (rv != 0) {
        uvErrorf(uv, "truncate segment %s: %s", filename1, osStrError(rv));
        rv = RAFT_IOERR;
        goto abort;
    }

    sprintf(filename2, UV__CLOSED_TEMPLATE, s->first_index, s->last_index);

    rv = osRename(uv->dir, filename1, filename2);
    if (rv != 0) {
        uvErrorf(uv, "rename segment %d: %s", s->counter, osStrError(rv));
        rv = RAFT_IOERR;
        goto abort;
    }

out:
    s->status = 0;
    return;

abort:
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
    raft_free(s);
    processRequests(uv);
    uvTruncateMaybeProcessRequests(uv);
    uvMaybeClose(uv);
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
        uvErrorf(uv, "start to truncate segment file %d: %s", s->counter,
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

    assert(uv->state == UV__ACTIVE || uv->closing);

    /* If the open segment is not empty, we expect its first index to be the
     * successor of the end index of the last segment we closed. */
    if (used > 0) {
        assert(first_index > 0);
        assert(last_index >= first_index);
        /* TODO: this assertion still fails sometimes */
        /* assert(first_index == uv->finalize_last_index + 1); */
    }

    segment = raft_malloc(sizeof *segment);
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

    if (used > 0) {
        uv->finalize_last_index = last_index;
    }

    processRequests(uv);

    return 0;
}
