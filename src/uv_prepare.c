#include <unistd.h>

#include "assert.h"
#include "uv.h"
#include "uv_os.h"

/* The happy path for a uvPrepare request is:
 *
 * - If there is a prepared open segment available, fire the request's callback
 *   immediately.
 *
 * - Otherwise, wait for the creation of a new open segment to complete,
 *   possibly kicking off the creation logic if no segment is being created
 *   currently.
 *
 * Possible failure modes are:
 *
 * - The create file request fails, in that case we fail all pending prepare
 *   requests and we mark the uv instance as errored.
 *
 * On close:
 *
 * - Cancel all pending prepare requests.
 * - Remove unused prepared open segments.
 * - Cancel any pending internal create segment request.
 */

/* At the moment the uv implementation of raft_io->append does not use
 * concurrent writes. */
#define MAX_CONCURRENT_WRITES 1

/* Number of open segments that we try to keep ready for writing. */
#define TARGET_POOL_SIZE 2

/* An open segment being prepared or sitting in the pool */
struct segment
{
    struct uv *uv;              /* Open segment file */
    struct uvFile *file;        /* Open segment file */
    struct uvFileCreate create; /* Create file request */
    unsigned long long counter; /* Segment counter */
    uvPath path;                /* Path of the segment */
    queue queue;                /* Pool */
};

/* Flush all pending requests, invoking their callbacks with the given
 * status. */
static void flushRequests(struct uv *uv, int status)
{
    while (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        queue *head;
        struct uvPrepare *r;
        head = QUEUE_HEAD(&uv->prepare_reqs);
        r = QUEUE_DATA(head, struct uvPrepare, queue);
        QUEUE_REMOVE(&r->queue);
        r->cb(r, NULL, 0, status);
    }
}

/* Destroy the segment object and remove the segment file. */
static void removeSegmentFileCloseCb(struct uvFile *file)
{
    struct segment *s = file->data;
    raft_free(file);
    unlink(s->path);
    raft_free(s);
}

/* Start removing a prepared open segment */
static void removeSegment(struct segment *s)
{
    assert(s->counter > 0);
    assert(s->file != NULL);
    uvFileClose(s->file, removeSegmentFileCloseCb);
}

void uvPrepareClose(struct uv *uv)
{
    assert(uv->closing);

    /* Cancel all pending prepare requests. */
    flushRequests(uv, RAFT_CANCELED);

    /* Remove any unused prepared segment. */
    while (!QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        queue *head;
        struct segment *s;
        head = QUEUE_HEAD(&uv->prepare_pool);
        s = QUEUE_DATA(head, struct segment, queue);
        QUEUE_REMOVE(&s->queue);
        removeSegment(s);
    }

    /* Cancel any in-progress segment creation request. */
    if (uv->prepare_file != NULL) {
        struct segment *s = uv->prepare_file->data;
        removeSegment(s);
    }
}

/* Process pending prepare requests.
 *
 * If we have some segments in the pool, use them to complete some pending
 * requests. */
static void processRequests(struct uv *uv)
{
    queue *head;
    assert(!uv->closing);

    /* We can finish the requests for which we have ready segments. */
    while (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        struct segment *segment;
        struct uvPrepare *req;

        /* If there's no prepared open segments available, let's bail out. */
        if (QUEUE_IS_EMPTY(&uv->prepare_pool)) {
            break;
        }

        /* Pop a segment from the pool. */
        head = QUEUE_HEAD(&uv->prepare_pool);
        segment = QUEUE_DATA(head, struct segment, queue);
        QUEUE_REMOVE(&segment->queue);

        /* Pop the head of the prepare requests queue. */
        head = QUEUE_HEAD(&uv->prepare_reqs);
        req = QUEUE_DATA(head, struct uvPrepare, queue);
        QUEUE_REMOVE(&req->queue);

        /* Finish the request */
        req->cb(req, segment->file, segment->counter, 0);
        raft_free(segment);
    }
}

static void maybePrepareSegment(struct uv *uv);
static void prepareSegmentFileCreateCb(struct uvFileCreate *req,
                                       int status,
                                       const char *errmsg)
{
    struct segment *s;
    struct uv *uv;

    s = req->data;
    uv = s->uv;

    /* If we have been canceled, it means we are closing. */
    if (status == UV_ECANCELED) {
        assert(uv->closing);
        return;
    }

    /* If the request has failed, simply close the file and mark this instance
     * as errored. */
    if (status != 0) {
        flushRequests(uv, RAFT_IOERR);
        uv->prepare_file = NULL;
        uv->errored = true;
        uvErrorf(uv, "create segment %s: %s", s->path, errmsg);
        uvFileClose(req->file, (uvFileCloseCb)raft_free);
        raft_free(s);
        return;
    }

    uv->prepare_file = NULL;
    QUEUE_PUSH(&uv->prepare_pool, &s->queue);

    /* Let's process any pending request. */
    processRequests(uv);

    /* Start creating a new segment if needed. */
    maybePrepareSegment(uv);
}

/* Start creating a new segment file. */
static int prepareSegment(struct uv *uv)
{
    struct segment *s;
    uvFilename filename;
    char errmsg[2048];
    int rv;

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    s->uv = uv;
    s->file = raft_malloc(sizeof *s->file);
    if (s->file == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_segment_alloc;
    }

    rv = uvFileInit(s->file, uv->loop, uv->direct_io, uv->async_io, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "init segment file %d: %s", s->counter, uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err_after_file_alloc;
    }

    s->file->data = s;
    s->create.data = s;
    s->counter = uv->prepare_next_counter;

    sprintf(filename, UV__OPEN_TEMPLATE, s->counter);
    uvJoin(uv->dir, filename, s->path);

    rv = uvFileCreate(s->file, &s->create, uv->dir, filename,
                      uv->block_size * uv->n_blocks, MAX_CONCURRENT_WRITES,
                      prepareSegmentFileCreateCb, errmsg);
    if (rv != 0) {
        uvErrorf(uv, "create segment file %d: %s", s->counter, uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err_after_file_init;
    }

    uv->prepare_file = s->file;
    uv->prepare_next_counter++;

    return 0;

err_after_file_init:
    uvFileClose(s->file, (uvFileCloseCb)raft_free);
    goto err_after_segment_alloc;
err_after_file_alloc:
    raft_free(s->file);
err_after_segment_alloc:
    raft_free(s);
err:
    assert(rv != 0);
    return rv;
}

/* If the pool has less than TARGET_POOL_SIZE segments, and we're not already
 * creating a segment, start creating a new segment. */
static void maybePrepareSegment(struct uv *uv)
{
    queue *head;
    unsigned n;
    int rv;

    assert(!uv->closing);

    /* If we are already creating a segment, we're done. */
    if (uv->prepare_file != NULL) {
        return;
    }

    /* Check how many prepared open segments we have. */
    n = 0;
    QUEUE_FOREACH(head, &uv->prepare_pool) { n++; }

    if (n < TARGET_POOL_SIZE) {
        rv = prepareSegment(uv);
        if (rv != 0) {
            flushRequests(uv, rv);
            uv->errored = true;
        }
    }
}

void uvPrepare(struct uv *uv, struct uvPrepare *req, uvPrepareCb cb)
{
    assert(uv->state == UV__ACTIVE);
    req->cb = cb;
    QUEUE_PUSH(&uv->prepare_reqs, &req->queue);
    processRequests(uv);
    maybePrepareSegment(uv);
}
