#include <unistd.h>

#include "assert.h"
#include "io_uv_preparer.h"

/**
 State flags
 */
enum {
    RAFT__IO_UV_PREPARER_ACTIVE = 0,
    RAFT__IO_UV_PREPARER_ABORTED,
    RAFT__IO_UV_PREPARER_CLOSING,
    RAFT__IO_UV_PREPARER_CLOSED
};

/* At the moment the io-uv-write does not use concurrent writes. */
#define RAFT__IO_UV_PREPARER_CREATE_MAX_CONCURRENT_WRITES 1

/**
 * Advance the preparer's state machine, processing any pending event.
 */
static void raft__io_uv_preparer_run(struct raft__io_uv_preparer *p);

static void raft__io_uv_preparer_run_active(struct raft__io_uv_preparer *p);

static void raft__io_uv_preparer_run_aborted(struct raft__io_uv_preparer *p);

static void raft__io_uv_preparer_run_closing(struct raft__io_uv_preparer *p);

/**
 * Start creating a new open segment.
 */
static int raft__io_uv_preparer_create_segment(struct raft__io_uv_preparer *p);

/**
 * Callback invoked after a create file request to create a new open segment has
 * been completed. It will trigger a loop iteration.
 */
static void raft__io_uv_preparer_create_segment_cb(
    struct raft__uv_file_create *req,
    int status);

/**
 * Start the removal of an open segment.
 */
static void raft__io_uv_preparer_remove_segment(
    struct raft__io_uv_preparer *p,
    struct raft__io_uv_preparer_segment *segment);

/**
 * Callback invoked after the open file of an unused segment has been closed.
 */
static void raft__io_uv_preparer_remove_segment_cb(struct raft__uv_file *file);

int raft__io_uv_preparer_init(struct raft__io_uv_preparer *p,
                              struct uv_loop_s *loop,
                              struct raft_logger *logger,
                              const char *dir,
                              size_t block_size,
                              unsigned n_blocks)
{
    p->loop = loop;
    p->logger = logger;
    p->dir = dir;
    p->state = RAFT__IO_UV_PREPARER_ACTIVE;
    p->block_size = block_size;
    p->n_blocks = n_blocks;
    p->next_counter = 1;
    p->close_cb = NULL;

    RAFT__QUEUE_INIT(&p->create_queue);
    RAFT__QUEUE_INIT(&p->ready_queue);
    RAFT__QUEUE_INIT(&p->close_queue);
    RAFT__QUEUE_INIT(&p->get_queue);

    return 0;
}

void raft__io_uv_preparer_close(struct raft__io_uv_preparer *p,
                                raft__io_uv_preparer_close_cb cb)
{
    assert(!raft__io_uv_preparer_is_closing(p));

    p->state = RAFT__IO_UV_PREPARER_CLOSING;
    p->close_cb = cb;

    raft__io_uv_preparer_run(p);
}

int raft__io_uv_preparer_get(struct raft__io_uv_preparer *p,
                             struct raft__io_uv_preparer_get *req,
                             raft__io_uv_preparer_get_cb cb)
{
    assert(!raft__io_uv_preparer_is_closing(p));

    req->cb = cb;

    RAFT__QUEUE_INIT(&req->queue);
    RAFT__QUEUE_PUSH(&p->get_queue, &req->queue);

    raft__io_uv_preparer_run(p);

    return 0;
}

bool raft__io_uv_preparer_is_closing(struct raft__io_uv_preparer *p)
{
    return p->state == RAFT__IO_UV_PREPARER_CLOSING ||
           p->state == RAFT__IO_UV_PREPARER_CLOSED;
}

static void raft__io_uv_preparer_run(struct raft__io_uv_preparer *p)
{
    assert(p->state != RAFT__IO_UV_PREPARER_CLOSED);

    if (p->state == RAFT__IO_UV_PREPARER_CLOSING) {
        raft__io_uv_preparer_run_closing(p);
        return;
    }

    if (p->state == RAFT__IO_UV_PREPARER_ACTIVE) {
        raft__io_uv_preparer_run_active(p);
    }

    if (p->state == RAFT__IO_UV_PREPARER_ABORTED) {
        raft__io_uv_preparer_run_aborted(p);
    }
}

static void raft__io_uv_preparer_run_active(struct raft__io_uv_preparer *p)
{
    raft__queue *head;
    unsigned n;
    int rv;

    /* We can finish the requests for which we have ready segments. */
    while (!RAFT__QUEUE_IS_EMPTY(&p->get_queue)) {
        struct raft__io_uv_preparer_segment *segment;
        struct raft__io_uv_preparer_get *req;

        /* If there's no ready segment available, let's bail out. */
        if (RAFT__QUEUE_IS_EMPTY(&p->ready_queue)) {
            break;
        }

        /* Pop the head of the ready segments queue. */
        head = RAFT__QUEUE_HEAD(&p->ready_queue);
        segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_preparer_segment, queue);

        RAFT__QUEUE_REMOVE(&segment->queue);

        /* Pop the head of the get requests queue. */
        head = RAFT__QUEUE_HEAD(&p->get_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_preparer_get, queue);

        RAFT__QUEUE_REMOVE(&req->queue);

        /* Finish the request */
        req->cb(req, segment->file, segment->counter, 0);

        raft_free(segment);
    }

    /* If we are already creating a segment, let's bail out. */
    if (!RAFT__QUEUE_IS_EMPTY(&p->create_queue)) {
        return;
    }

    /* Count how many ready segments we have. */
    n = 0;
    RAFT__QUEUE_FOREACH(head, &p->ready_queue) { n++; }

    /* If we have reached our target number, there's nothing to do. */
    if (n == RAFT__IO_UV_PREPARER_N) {
        return;
    }

    rv = raft__io_uv_preparer_create_segment(p);
    if (rv != 0) {
        goto err;
    }

    return;

err:
    assert(rv != 0);

    p->state = RAFT__IO_UV_PREPARER_ABORTED;
}

static void raft__io_uv_preparer_run_aborted(struct raft__io_uv_preparer *p)
{
    raft__queue *head;

    /* Fail all pending get requests. */
    while (!RAFT__QUEUE_IS_EMPTY(&p->get_queue)) {
        struct raft__io_uv_preparer_get *req;

        head = RAFT__QUEUE_HEAD(&p->get_queue);
        RAFT__QUEUE_REMOVE(head);

        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_preparer_get, queue);

        req->cb(req, NULL, 0, RAFT_ERR_IO_ABORTED);
    }
}

static void raft__io_uv_preparer_run_closing(struct raft__io_uv_preparer *p)
{
    raft__queue *head;

    /* Cancel all pending get requests. */
    while (!RAFT__QUEUE_IS_EMPTY(&p->get_queue)) {
        struct raft__io_uv_preparer_get *req;

        head = RAFT__QUEUE_HEAD(&p->get_queue);
        RAFT__QUEUE_REMOVE(head);

        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_preparer_get, queue);

        req->cb(req, NULL, 0, RAFT_ERR_IO_CANCELED);
    }

    /* Schedule the removal of all ready open segments. */
    while (!RAFT__QUEUE_IS_EMPTY(&p->ready_queue)) {
        struct raft__io_uv_preparer_segment *segment;

        head = RAFT__QUEUE_HEAD(&p->ready_queue);
        segment =
            RAFT__QUEUE_DATA(head, struct raft__io_uv_preparer_segment, queue);

        RAFT__QUEUE_REMOVE(&segment->queue);

        raft__io_uv_preparer_remove_segment(p, segment);
    }

    if (!RAFT__QUEUE_IS_EMPTY(&p->close_queue) ||
        !RAFT__QUEUE_IS_EMPTY(&p->create_queue)) {
        return;
    }

    p->state = RAFT__IO_UV_PREPARER_CLOSED;

    if (p->close_cb != NULL) {
        p->close_cb(p);
    }
}

static int raft__io_uv_preparer_create_segment(struct raft__io_uv_preparer *p)
{
    struct raft__io_uv_preparer_segment *segment;
    raft__io_uv_fs_filename filename;
    int rv;

    segment = raft_malloc(sizeof *segment);
    if (segment == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    segment->preparer = p;

    segment->file = raft_malloc(sizeof *segment->file);
    if (segment->file == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_segment_alloc;
    }
    rv = raft__uv_file_init(segment->file, p->loop);
    if (rv != 0) {
        raft_errorf(p->logger, "init open segment file %d: %s",
                    segment->counter, uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_file_alloc;
    }

    segment->file->data = segment;
    segment->create.data = segment;
    segment->counter = p->next_counter;

    sprintf(filename, "open-%lld", segment->counter);
    raft__io_uv_fs_join(p->dir, filename, segment->path);

    rv = raft__uv_file_create(segment->file, &segment->create, segment->path,
                              p->block_size * p->n_blocks,
                              RAFT__IO_UV_PREPARER_CREATE_MAX_CONCURRENT_WRITES,
                              raft__io_uv_preparer_create_segment_cb);
    if (rv != 0) {
        raft_errorf(p->logger, "request creation of open segment %d: %s",
                    segment->counter, uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_file_init;
    }

    RAFT__QUEUE_INIT(&segment->queue);
    RAFT__QUEUE_PUSH(&p->create_queue, &segment->queue);

    p->next_counter++;

    return 0;

err_after_file_init:
    raft__uv_file_close(segment->file, (raft__uv_file_close_cb)raft_free);
    goto err_after_segment_alloc;

err_after_file_alloc:
    raft_free(segment->file);

err_after_segment_alloc:
    raft_free(segment);

err:
    assert(rv != 0);

    return rv;
}

static void raft__io_uv_preparer_create_segment_cb(
    struct raft__uv_file_create *req,
    int status)
{
    struct raft__io_uv_preparer_segment *segment;
    struct raft__io_uv_preparer *p;

    segment = req->data;
    p = segment->preparer;

    RAFT__QUEUE_REMOVE(&segment->queue);

    if (status != 0) {
        raft_errorf(p->logger, "create open segment %s: %s", segment->path,
                    uv_strerror(status));
        raft__uv_file_close(req->file, (raft__uv_file_close_cb)raft_free);
        raft_free(segment);
        p->state = RAFT__IO_UV_PREPARER_ABORTED;
        goto out;
    }

    /* If we have been closed, let's close the segment immediately. */
    if (raft__io_uv_preparer_is_closing(p)) {
        raft__io_uv_preparer_remove_segment(p, segment);
        return;
    }

    RAFT__QUEUE_INIT(&segment->queue);
    RAFT__QUEUE_PUSH(&p->ready_queue, &segment->queue);

out:
    raft__io_uv_preparer_run(p);
}

static void raft__io_uv_preparer_remove_segment(
    struct raft__io_uv_preparer *p,
    struct raft__io_uv_preparer_segment *segment)
{
    assert(segment->counter > 0);

    RAFT__QUEUE_PUSH(&p->close_queue, &segment->queue);

    /* If the segment has not been used, we'll just close its open file handle
     * and unlink it. Otherwise we need to rename it and truncate it. */
    assert(segment->file != NULL);
    raft__uv_file_close(segment->file, raft__io_uv_preparer_remove_segment_cb);
}

static void raft__io_uv_preparer_remove_segment_cb(struct raft__uv_file *file)
{
    struct raft__io_uv_preparer_segment *segment = file->data;
    struct raft__io_uv_preparer *p = segment->preparer;

    RAFT__QUEUE_REMOVE(&segment->queue);

    raft_free(file);
    unlink(segment->path);

    raft_free(segment);

    raft__io_uv_preparer_run(p);
}
