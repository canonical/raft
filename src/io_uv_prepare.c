#include <unistd.h>

#include "assert.h"
#include "io_uv.h"
#include "io_uv_fs.h"
#include "logging.h"

/* The happy path for a io_uv__prepare request is:
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
 *   requests and we mark the io_uv instance as errored.
 *
 * On close:
 *
 * - Cancel all pending prepare requests.
 * - Remove unused prepared open segments.
 * - Cancel any pending internal create segment request.
 */

/* At the moment the io-uv implementation of raft_io->append does not use
 * concurrent writes. */
#define MAX_CONCURRENT_WRITES 1

/* Number of open segments that we try to keep ready for writing. */
#define TARGET_POOL_SIZE 2

/* An open segment being prepared or sitting in the pool */
struct segment
{
    struct io_uv *uv;              /* Open segment file */
    struct uv__file *file;         /* Open segment file */
    struct uv__file_create create; /* Create file request */
    unsigned long long counter;    /* Segment counter */
    io_uv__path path;              /* Path of the segment */
    raft__queue queue;             /* Pool */
};

/* Process pending prepare requests.
 *
 * If we have some segments in the pool, use them to complete some pending
 * requests. */
static void process_requests(struct io_uv *uv);

/* Maintain the pool of prepared open segments.
 *
 * If the pool has less than TARGET_POOL_SIZE segments, and we're not already
 * creating a segment, start creating a new segment. */
static void maintain_pool(struct io_uv *uv);

/* Start creating a new segment file. */
static int create_segment(struct io_uv *uv);
static void create_segment_cb(struct uv__file_create *req, int status);

/* Start removing a prepared open segment */
static void remove_segment(struct segment *s);
static void remove_segment_cb(struct uv__file *file);

void io_uv__prepare(struct io_uv *uv,
                    struct io_uv__prepare *req,
                    io_uv__prepare_cb cb)
{
    assert(uv->state == IO_UV__ACTIVE);
    req->cb = cb;
    RAFT__QUEUE_PUSH(&uv->prepare_reqs, &req->queue);
    process_requests(uv);
    maintain_pool(uv);
}

/* Flush all pending requests with the given status. */
static void flush_requests(struct io_uv *uv, int status)
{
    while (!RAFT__QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        raft__queue *head;
        struct io_uv__prepare *r;
        head = RAFT__QUEUE_HEAD(&uv->prepare_reqs);
        r = RAFT__QUEUE_DATA(head, struct io_uv__prepare, queue);
        RAFT__QUEUE_REMOVE(&r->queue);
        r->cb(r, NULL, 0, status);
    }
}

void io_uv__prepare_stop(struct io_uv *uv)
{
    assert(uv->state == IO_UV__CLOSING);

    /* Cancel all pending prepare requests. */
    flush_requests(uv, RAFT_ERR_IO_CANCELED);

    /* Remove any unused prepared segment. */
    while (!RAFT__QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        raft__queue *head;
        struct segment *s;
        head = RAFT__QUEUE_HEAD(&uv->prepare_pool);
        s = RAFT__QUEUE_DATA(head, struct segment, queue);
        RAFT__QUEUE_REMOVE(&s->queue);
        remove_segment(s);
    }

    /* Cancel any in-progress segment creation request. */
    if (uv->preparing != NULL) {
        struct segment *s = uv->preparing->data;
        remove_segment(s);
    }
}

static void process_requests(struct io_uv *uv)
{
    raft__queue *head;

    /* We can finish the requests for which we have ready segments. */
    while (!RAFT__QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        struct segment *segment;
        struct io_uv__prepare *req;

        /* If there's no prepared open segments available, let's bail out. */
        if (RAFT__QUEUE_IS_EMPTY(&uv->prepare_pool)) {
            break;
        }

        /* Pop a segment from the pool. */
        head = RAFT__QUEUE_HEAD(&uv->prepare_pool);
        segment = RAFT__QUEUE_DATA(head, struct segment, queue);
        RAFT__QUEUE_REMOVE(&segment->queue);

        /* Pop the head of the prepare requests queue. */
        head = RAFT__QUEUE_HEAD(&uv->prepare_reqs);
        req = RAFT__QUEUE_DATA(head, struct io_uv__prepare, queue);
        RAFT__QUEUE_REMOVE(&req->queue);

        /* Finish the request */
        req->cb(req, segment->file, segment->counter, 0);
        raft_free(segment);
    }
}

static void maintain_pool(struct io_uv *uv)
{
    raft__queue *head;
    unsigned n;
    int rv;

    /* If we are already creating a segment, we're done. */
    if (uv->preparing != NULL) {
        return;
    }

    /* Check how many prepared open segments we have. */
    n = 0;
    RAFT__QUEUE_FOREACH(head, &uv->prepare_pool) { n++; }

    if (n < TARGET_POOL_SIZE) {
        rv = create_segment(uv);
        if (rv != 0) {
            flush_requests(uv, rv);
            uv->errored = true;
        }
    }
}

static int create_segment(struct io_uv *uv)
{
    struct segment *s;
    io_uv__filename filename;
    int rv;

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    s->uv = uv;

    s->file = raft_malloc(sizeof *s->file);
    if (s->file == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_segment_alloc;
    }

    rv = uv__file_init(s->file, uv->loop);
    if (rv != 0) {
        errorf(uv->io, "init open segment file %d: %s", s->counter,
               uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_file_alloc;
    }

    s->file->data = s;
    s->create.data = s;
    s->counter = uv->prepare_next_counter;

    sprintf(filename, "open-%lld", s->counter);
    io_uv__join(uv->dir, filename, s->path);

    rv = uv__file_create(s->file, &s->create, s->path,
                         uv->block_size * uv->n_blocks, MAX_CONCURRENT_WRITES,
                         create_segment_cb);
    if (rv != 0) {
        errorf(uv->io, "request creation of open segment %d: %s", s->counter,
               uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_file_init;
    }

    uv->preparing = s->file;
    uv->prepare_next_counter++;

    return 0;

err_after_file_init:
    uv__file_close(s->file, (uv__file_close_cb)raft_free);
    goto err_after_segment_alloc;

err_after_file_alloc:
    raft_free(s->file);

err_after_segment_alloc:
    raft_free(s);

err:
    assert(rv != 0);

    return rv;
}

static void create_segment_cb(struct uv__file_create *req, int status)
{
    struct segment *s;
    struct io_uv *uv;

    /* If we have been canceled, it means we are shutting down and the
     * request has been canceled (and its memory possibly freed). */
    if (status == UV_ECANCELED) {
        return;
    }

    s = req->data;
    uv = s->uv;

    /* If the request has failed, simply close the file and mark this instance
     * as errored. */
    if (status != 0) {
        flush_requests(uv, RAFT_ERR_IO);
        uv->preparing = NULL;
        uv->errored = true;
        errorf(uv->io, "create open segment %s: %s", s->path,
               uv_strerror(status));
        uv__file_close(req->file, (uv__file_close_cb)raft_free);
        raft_free(s);
        return;
    }

    uv->preparing = NULL;
    RAFT__QUEUE_PUSH(&uv->prepare_pool, &s->queue);

    /* Let's process any pending request. */
    process_requests(uv);

    /* Start creating a new segment if needed. */
    maintain_pool(uv);
}

static void remove_segment(struct segment *s)
{
    assert(s->counter > 0);
    assert(s->file != NULL);
    uv__file_close(s->file, remove_segment_cb);
}

static void remove_segment_cb(struct uv__file *file)
{
    struct segment *s = file->data;
    raft_free(file);
    unlink(s->path);
    raft_free(s);
}
