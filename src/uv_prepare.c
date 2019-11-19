#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "heap.h"
#include "uv.h"
#include "uv_error.h"
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
struct preparedSegment
{
    struct uv *uv;                     /* Open segment file */
    size_t size;                       /* Segment size */
    struct uv_work_s work;             /* To execute logic in the threadpool */
    int status;                        /* Result of threadpool callback */
    char errmsg[RAFT_ERRMSG_BUF_SIZE]; /* Error of threadpool callback */
    bool canceled;                     /* Cancellation flag */
    unsigned long long counter;        /* Segment counter */
    char filename[UV__FILENAME_LEN];   /* Filename of the segment */
    uv_file fd;                        /* File descriptor of prepared file */
    queue queue;                       /* Pool */
};

/* Flush all pending requests, invoking their callbacks with the given
 * status. */
static void uvPrepareFlushRequests(struct uv *uv, int status)
{
    while (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        queue *head;
        struct uvPrepare *req;
        head = QUEUE_HEAD(&uv->prepare_reqs);
        req = QUEUE_DATA(head, struct uvPrepare, queue);
        QUEUE_REMOVE(&req->queue);
        req->cb(req, status);
    }
}

/* Remove a prepared open segment */
static void uvPrepareRemove(struct preparedSegment *s)
{
    assert(s->counter > 0);
    assert(s->fd >= 0);
    UvOsClose(s->fd);
    UvFsRemoveFile(s->uv->dir, s->filename, s->errmsg);
    raft_free(s);
}

/* Cancel a prepared segment creation. */
static void uvPrepareCancel(struct preparedSegment *s)
{
    assert(s->counter > 0);
    s->canceled = true; /* Memory released in uvPrepareCreateFileAfterWorkCb */
}

void uvPrepareClose(struct uv *uv)
{
    assert(uv->closing);

    /* Cancel all pending prepare requests. */
    uvPrepareFlushRequests(uv, RAFT_CANCELED);

    /* Remove any unused prepared segment. */
    while (!QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        queue *head;
        struct preparedSegment *s;
        head = QUEUE_HEAD(&uv->prepare_pool);
        s = QUEUE_DATA(head, struct preparedSegment, queue);
        QUEUE_REMOVE(&s->queue);
        uvPrepareRemove(s);
    }

    /* Cancel any in-progress segment creation request. */
    if (uv->prepare_inflight != NULL) {
        struct preparedSegment *s = uv->prepare_inflight;
        uvPrepareCancel(s);
    }
}

/* Finish the oldest pending prepare request using the next available prepared
 * segment. */
static void uvPrepareRequestFinish(struct uv *uv)
{
    queue *head;
    struct preparedSegment *segment;
    struct uvPrepare *req;

    assert(!uv->closing);
    assert(!QUEUE_IS_EMPTY(&uv->prepare_reqs));
    assert(!QUEUE_IS_EMPTY(&uv->prepare_pool));

    /* Pop a segment from the pool. */
    head = QUEUE_HEAD(&uv->prepare_pool);
    segment = QUEUE_DATA(head, struct preparedSegment, queue);
    QUEUE_REMOVE(&segment->queue);

    /* Pop the head of the prepare requests queue. */
    head = QUEUE_HEAD(&uv->prepare_reqs);
    req = QUEUE_DATA(head, struct uvPrepare, queue);
    QUEUE_REMOVE(&req->queue);

    /* Finish the request */
    assert(segment->fd >= 0);
    req->fd = segment->fd;
    req->counter = segment->counter;
    HeapFree(segment);
    req->cb(req, 0);
}

static void uvPrepareCreateFileWorkCb(uv_work_t *work)
{
    struct preparedSegment *s = work->data;
    struct uv *uv = s->uv;
    int rv;

    rv = UvFsAllocateFile(uv->dir, s->filename, s->size, &s->fd, s->errmsg);
    if (rv != 0) {
        goto err;
    }

    rv = UvFsSyncDir(uv->dir, s->errmsg);
    if (rv != 0) {
        goto err_after_allocate;
    }

    s->status = 0;
    return;

err_after_allocate:
    UvOsClose(s->fd);
err:
    assert(rv != 0);
    s->status = rv;
    return;
}

static void uvMaybePrepareSegment(struct uv *uv);

static void uvPrepareCreateFileAfterWorkCb(uv_work_t *work, int status)
{
    struct preparedSegment *s = work->data;
    struct uv *uv = s->uv;
    assert(status == 0);

    uv->prepare_inflight = NULL; /* Reset the creation in-progress marker. */

    /* If we were canceled, let's mark the prepare request as canceled,
     * regardless of the actual outcome. */
    if (s->canceled) {
        if (s->status == 0) {
            char path[UV__PATH_SZ];
            UvOsJoin(uv->dir, s->filename, path);
            UvOsClose(s->fd);
            UvOsUnlink(path);
        }
        Tracef(uv->tracer, "canceled creation of %s", s->filename);
        raft_free(s);
        uvMaybeFireCloseCb(uv);
        return;
    }

    /* If the request has failed, mark this instance as errored. */
    if (s->status != 0) {
        uvPrepareFlushRequests(uv, RAFT_IOERR);
        uv->errored = true;
        Tracef(uv->tracer, "create segment %s: %s", s->filename, s->errmsg);
        raft_free(s);
        return;
    }

    assert(s->fd >= 0);

    Tracef(uv->tracer, "completed creation of %s", s->filename);
    QUEUE_PUSH(&uv->prepare_pool, &s->queue);

    /* Let's process any pending request. */
    if (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        uvPrepareRequestFinish(uv);
    }

    /* Start creating a new segment if needed. */
    uvMaybePrepareSegment(uv);
}

/* Start creating a new segment file. */
static int uvPrepareSegment(struct uv *uv)
{
    struct preparedSegment *s;
    int rv;

    assert(uv->prepare_inflight == NULL);

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    memset(s, 0, sizeof *s);
    s->uv = uv;
    s->counter = uv->prepare_next_counter;
    s->work.data = s;
    s->fd = -1;
    s->size = uv->block_size * uvSegmentBlocks(uv);
    s->canceled = false;

    sprintf(s->filename, UV__OPEN_TEMPLATE, s->counter);

    Tracef(uv->tracer, "create open segment %s", s->filename);
    rv = uv_queue_work(uv->loop, &s->work, uvPrepareCreateFileWorkCb,
                       uvPrepareCreateFileAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        Tracef(uv->tracer, "can't create segment %s: %s", s->filename,
               uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err_after_segment_alloc;
    }

    uv->prepare_inflight = s;
    uv->prepare_next_counter++;

    return 0;

err_after_segment_alloc:
    raft_free(s);
err:
    assert(rv != 0);
    return rv;
}

/* If the pool has less than TARGET_POOL_SIZE segments, and we're not already
 * creating a segment, start creating a new segment. */
static void uvMaybePrepareSegment(struct uv *uv)
{
    queue *head;
    unsigned n;
    int rv;

    assert(!uv->closing);

    /* If we are already creating a segment, we're done. */
    if (uv->prepare_inflight != NULL) {
        return;
    }

    /* Check how many prepared open segments we have. */
    n = 0;
    QUEUE_FOREACH(head, &uv->prepare_pool) { n++; }

    if (n < TARGET_POOL_SIZE) {
        rv = uvPrepareSegment(uv);
        if (rv != 0) {
            uvPrepareFlushRequests(uv, rv);
            uv->errored = true;
        }
    }
}

void uvPrepare(struct uv *uv, struct uvPrepare *req, uvPrepareCb cb)
{
    assert(!uv->closing);
    req->cb = cb;
    QUEUE_PUSH(&uv->prepare_reqs, &req->queue);
    if (!QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        uvPrepareRequestFinish(uv);
    }
    uvMaybePrepareSegment(uv);
}
