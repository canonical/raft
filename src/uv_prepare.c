#include <string.h>
#include <unistd.h>

#include "assert.h"
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
struct segment
{
    struct uv *uv;                   /* Open segment file */
    struct UvFsCreateFile req;       /* Create file request */
    unsigned long long counter;      /* Segment counter */
    char filename[UV__FILENAME_LEN]; /* Filename of the segment */
    char path[UV__PATH_SZ];          /* Full path of the segment */
    uv_file fd;                      /* File descriptor of prepared file */
    queue queue;                     /* Pool */
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
        r->cb(r, status);
    }
}

/* Start removing a prepared open segment */
static void uvPrepareRemove(struct segment *s)
{
    assert(s->counter > 0);
    assert(s->fd >= 0);
    UvOsClose(s->fd);
    UvOsUnlink(s->path);
    raft_free(s);
}

/* Cancel segment file creation. */
static void uvPrepareCancel(struct segment *s)
{
    assert(s->counter > 0);
    UvFsCreateFileCancel(&s->req);
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
        uvPrepareRemove(s);
    }

    /* Cancel any in-progress segment creation request. */
    if (uv->prepare_file != NULL) {
        struct segment *s = uv->prepare_file->data;
        uvPrepareCancel(s);
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
        assert(segment->fd >= 0);
        req->fd = segment->fd;
        req->counter = segment->counter;
        req->cb(req, 0);
        raft_free(segment);
    }
}

static void maybePrepareSegment(struct uv *uv);
static void prepareSegmentCreateFileCb(struct UvFsCreateFile *req, int status)
{
    struct segment *s;
    struct uv *uv;

    s = req->data;
    uv = s->uv;

    /* If we have been canceled, it means we are closing. */
    if (status == UV__CANCELED) {
        uvDebugf(uv, "canceled creation of %s", s->filename);
        assert(uv->closing);
        raft_free(s);
        return;
    }

    /* If the request has failed, mark this instance as errored. */
    if (status != 0) {
        flushRequests(uv, RAFT_IOERR);
        uv->prepare_file = NULL;
        uv->errored = true;
        uvErrorf(uv, "create segment %s: %s", s->path, UvFsErrMsg(&uv->fs));
        raft_free(s);
        return;
    }

    assert(req->fd >= 0);

    uvDebugf(uv, "completed creation of %s", s->filename);
    uv->prepare_file = NULL;
    s->fd = req->fd;
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
    int rv;

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    s->uv = uv;
    s->req.data = s;
    s->counter = uv->prepare_next_counter;
    s->fd = -1;

    sprintf(s->filename, UV__OPEN_TEMPLATE, s->counter);
    strcpy(s->path, uv->dir);
    strcat(s->path, "/");
    strcat(s->path, s->filename);

    uvDebugf(uv, "create open segment %s", s->filename);
    rv = UvFsCreateFile(&uv->fs, &s->req, uv->dir, s->filename,
                        uv->block_size * uv->n_blocks,
                        prepareSegmentCreateFileCb);
    if (rv != 0) {
        uvErrorf(uv, "can't create segment %s: %s", s->filename,
                 UvFsErrMsg(&uv->fs));
        rv = RAFT_IOERR;
        goto err_after_segment_alloc;
    }

    uv->prepare_file = &s->req;
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
