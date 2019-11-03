#include "uv_writer.h"

#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "heap.h"
#include "uv_error.h"

static void uvWriterSetErrMsg(struct UvWriter *w, char *errmsg)
{
    HeapFree(w->errmsg); /* Delete any previous error. */
    w->errmsg = errmsg;
}

/* Set the request status according the given result code. */
static void uvWriterReqSetStatus(struct UvWriterReq *req, int result)
{
    char *errmsg = NULL;
    if (result < 0) {
        errmsg = uvSysErrMsg("aio", result);
        req->status = UV__ERROR;
    } else if ((size_t)result < req->len) {
        errmsg = errMsgPrintf("short write: %d bytes instead of %ld", result,
                              req->len);
        req->status = UV__ERROR;
    } else {
        req->status = 0;
    }
    uvWriterSetErrMsg(req->writer, errmsg);
}

/* Remove the request from the queue of inflight writes and invoke the request
 * callback if set. */
static void uvWriterReqFinish(struct UvWriterReq *req)
{
    QUEUE_REMOVE(&req->queue);
    uvWriterSetErrMsg(req->writer, req->errmsg);
    req->cb(req, req->status);
}

/* Run blocking syscalls involved in a file write request.
 *
 * Perform a KAIO write request and synchronously wait for it to complete. */
static void uvWriterWorkCb(uv_work_t *work)
{
    struct UvWriterReq *req; /* Writer request object */
    struct UvWriter *w;      /* Writer object */
    aio_context_t ctx;       /* KAIO handle */
    struct iocb *iocbs;      /* Pointer to KAIO request object */
    struct io_event event;   /* KAIO response object */
    int n_events;
    char *errmsg;
    int rv;

    req = work->data;
    w = req->writer;

    iocbs = &req->iocb;

    /* If more than one write in parallel is allowed, submit the AIO request
     * using a dedicated context, to avoid synchronization issues between
     * threads when multiple writes are submitted in parallel. This is
     * suboptimal but in real-world users should use file systems and kernels
     * with proper async write support. */
    if (w->n_events > 1) {
        ctx = 0;
        rv = UvOsIoSetup(1 /* Maximum concurrent requests */, &ctx);
        if (rv != 0) {
            errmsg = uvSysErrMsg("io_setup", rv);
            goto out;
        }
    } else {
        ctx = w->ctx;
    }

    /* Submit the request */
    rv = UvOsIoSubmit(ctx, 1, &iocbs);
    if (rv != 0) {
        /* UNTESTED: since we're not using NOWAIT and the parameters are valid,
         * this shouldn't fail. */
        errmsg = uvSysErrMsg("io_submit", rv);
        goto out_after_io_setup;
    }

    /* Wait for the request to complete */
    n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
    assert(n_events == 1);
    assert(rv == 0);

out_after_io_setup:
    if (w->n_events > 1) {
        UvOsIoDestroy(ctx);
    }

out:
    if (rv != 0) {
        req->errmsg = errmsg;
        req->status = UV__ERROR;
    } else {
        uvWriterReqSetStatus(req, event.res);
    }

    return;
}

/* Callback run after writeWorkCb has returned. It normally invokes the write
 * request callback. */
static void uvWriterAfterWorkCb(uv_work_t *work, int status)
{
    struct UvWriterReq *req; /* Write file request object */

    assert(status == 0); /* We don't cancel worker requests */

    req = work->data;

    /* If we were canceled, let's mark the request as canceled, regardless of
     * the actual outcome. */
    if (req->canceled) {
        HeapFree(req->errmsg);
        req->errmsg = errMsgPrintf("canceled");
        req->status = UV__CANCELED;
    }

    uvWriterReqFinish(req);
}

/* Callback fired when the event fd associated with AIO write requests should be
 * ready for reading (i.e. when a write has completed). */
static void uvWriterPollCb(uv_poll_t *poller, int status, int events)
{
    struct UvWriter *w = poller->data;
    uint64_t completed; /* True if the write is complete */
    unsigned i;
    int n_events;
    int rv;

    assert(w->event_fd >= 0);

    /* TODO: it's not clear when polling could fail. In this case we should
     * probably mark all pending requests as failed. */
    assert(status == 0);

    assert(events & UV_READABLE);

    /* Read the event file descriptor */
    rv = read(w->event_fd, &completed, sizeof completed);
    if (rv != sizeof completed) {
        /* UNTESTED: According to eventfd(2) this is the only possible failure
         * mode, meaning that epoll has indicated that the event FD is not yet
         * ready. */
        assert(errno == EAGAIN);
        return;
    }

    /* TODO: this assertion fails in unit tests */
    /* assert(completed == 1); */

    /* Try to fetch the write responses.
     *
     * If we got here at least one write should have completed and io_events
     * should return immediately without blocking. */
    n_events = UvOsIoGetevents(w->ctx, 1, w->n_events, w->events, NULL);
    assert(n_events >= 1);

    for (i = 0; i < (unsigned)n_events; i++) {
        struct io_event *event = &w->events[i];
        struct UvWriterReq *req = *((void **)&event->data);

        /* If we are closing, we mark the write as canceled, although
         * technically it might have worked. */
        if (req->canceled) {
            HeapFree(req->errmsg);
            req->errmsg = errMsgPrintf("canceled");
            req->status = UV__CANCELED;
            goto finish;
        }

#if defined(RWF_NOWAIT)
        /* If we got EAGAIN, it means it was not possible to perform the write
         * asynchronously, so let's fall back to the threadpool. */
        if (event->res == -EAGAIN) {
            req->iocb.aio_flags &= ~IOCB_FLAG_RESFD;
            req->iocb.aio_resfd = 0;
            req->iocb.aio_rw_flags &= ~RWF_NOWAIT;
            req->work.data = req;
            rv = uv_queue_work(w->loop, &req->work, uvWriterWorkCb,
                               uvWriterAfterWorkCb);
            if (rv != 0) {
                /* UNTESTED: with the current libuv implementation this should
                 * never fail. */
                req->errmsg = uvSysErrMsg("uv_queue_work", rv);
                req->status = UV__ERROR;
                goto finish;
            }
            return;
        }
#endif /* RWF_NOWAIT */

        uvWriterReqSetStatus(req, event->res);

    finish:
        uvWriterReqFinish(req);
    }
}

int UvWriterInit(struct UvFs *fs,
                 struct UvWriter *w,
                 struct uv_loop_s *loop,
                 uv_file fd,
                 bool direct /* Whether to use direct I/O */,
                 bool async /* Whether async I/O is available */,
                 unsigned max_concurrent_writes)
{
    char *errmsg = NULL;
    int rv = 0;

    w->loop = loop;
    w->fd = fd;
    w->async = async;
    w->ctx = 0;
    w->n_events = max_concurrent_writes;
    w->errmsg = NULL;

    /* Set direct I/O if available. */
    if (direct) {
        rv = UvOsSetDirectIo(w->fd);
        if (rv != 0) {
            errmsg = uvSysErrMsg("fcntl", rv);
            goto err;
        }
    }

    /* Setup the AIO context. */
    rv = UvOsIoSetup(w->n_events, &w->ctx);
    if (rv != 0) {
        errmsg = uvSysErrMsg("io_setup", rv);
        rv = UV__ERROR;
        goto err;
    }

    /* Initialize the array of re-usable event objects. */
    w->events = HeapCalloc(w->n_events, sizeof *w->events);
    if (w->events == NULL) {
        /* UNTESTED: todo */
        errmsg = errMsgPrintf("failed to alloc events array");
        rv = UV__ERROR;
        goto err_after_io_setup;
    }

    /* Create an event file descriptor to get notified when a write has
     * completed. */
    rv = UvOsEventfd(0, UV__EFD_NONBLOCK);
    if (rv < 0) {
        /* UNTESTED: should fail only with ENOMEM */
        errmsg = uvSysErrMsg("eventfd", rv);
        rv = UV__ERROR;
        goto err_after_events_alloc;
    }
    w->event_fd = rv;

    rv = uv_poll_init(loop, &w->event_poller, w->event_fd);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this should never
         * fail. */
        errmsg = uvSysErrMsg("uv_poll_init", rv);
        rv = UV__ERROR;
        goto err_after_event_fd;
    }
    w->event_poller.data = w;

    rv = uv_poll_start(&w->event_poller, UV_READABLE, uvWriterPollCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this should never
         * fail. */
        errmsg = uvSysErrMsg("uv_poll_start", rv);
        rv = UV__ERROR;
        goto err_after_event_fd;
    }

    QUEUE_INIT(&w->write_queue);

    return 0;

err_after_event_fd:
    UvOsClose(w->event_fd);
err_after_events_alloc:
    HeapFree(w->events);
err_after_io_setup:
    UvOsIoDestroy(w->ctx);
err:
    UvFsSetErrMsg(fs, errmsg);
    assert(rv != 0);
    return rv;
}

const char *UvWriterErrMsg(struct UvWriter *w)
{
    return w->errmsg;
}

static void uvWriterPollerCloseCb(struct uv_handle_s *handle)
{
    struct UvWriter *w = handle->data;

    UvOsClose(w->fd);
    UvOsClose(w->event_fd);
    HeapFree(w->events);
    HeapFree(w->errmsg);
    UvOsIoDestroy(w->ctx);

    if (w->close_cb != NULL) {
        w->close_cb(w);
    }
}

void UvWriterClose(struct UvWriter *w, UvWriterCloseCb cb)
{
    assert(QUEUE_IS_EMPTY(&w->write_queue));
    w->close_cb = cb;
    uv_close((struct uv_handle_s *)&w->event_poller, uvWriterPollerCloseCb);
}

/* Return the total lengths of the given buffers. */
static size_t lenOfBufs(const uv_buf_t bufs[], unsigned n)
{
    size_t len = 0;
    unsigned i;
    for (i = 0; i < n; i++) {
        len += bufs[i].len;
    }
    return len;
}

int UvWriterSubmit(struct UvWriter *w,
                   struct UvWriterReq *req,
                   const uv_buf_t bufs[],
                   unsigned n,
                   size_t offset,
                   UvWriterReqCb cb)
{
    char *errmsg = NULL;
    int rv = 0;
#if defined(RWF_NOWAIT)
    struct iocb *iocbs = &req->iocb;
#endif /* RWF_NOWAIT */

    /* TODO: at the moment we are not leveraging the support for concurrent
     *       writes, so ensure that we're getting write requests
     *       sequentially. */
    if (w->n_events == 1) {
        assert(QUEUE_IS_EMPTY(&w->write_queue));
    }

    assert(w->fd >= 0);
    assert(w->event_fd >= 0);
    assert(w->ctx != 0);
    assert(req != NULL);
    assert(bufs != NULL);
    assert(n > 0);

    req->writer = w;
    req->cb = cb;
    req->len = lenOfBufs(bufs, n);
    memset(&req->iocb, 0, sizeof req->iocb);
    req->iocb.aio_fildes = w->fd;
    req->iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
    req->iocb.aio_reqprio = 0;
    *((void **)(&req->iocb.aio_buf)) = (void *)bufs;
    req->iocb.aio_nbytes = n;
    req->iocb.aio_offset = offset;
    *((void **)(&req->iocb.aio_data)) = (void *)req;

    req->errmsg = NULL;
    req->canceled = false;

    QUEUE_PUSH(&w->write_queue, &req->queue);

#if defined(RWF_HIPRI)
    /* High priority request, if possible */
    req->iocb.aio_rw_flags |= RWF_HIPRI;
#endif

#if defined(RWF_DSYNC)
    /* Use per-request synchronous I/O if available. Otherwise, we have opened
     * the file with O_DSYNC. */
    req->iocb.aio_rw_flags |= RWF_DSYNC;
#endif

#if defined(RWF_NOWAIT)
    /* If io_submit can be run in a 100% non-blocking way, we'll try to write
     * without using the threadpool. */
    if (w->async) {
        req->iocb.aio_flags |= IOCB_FLAG_RESFD;
        req->iocb.aio_resfd = w->event_fd;
        req->iocb.aio_rw_flags |= RWF_NOWAIT;
    }
#else
    /* Since there's no support for NOWAIT, io_submit might occasionally block
     * and we need to run it in the threadpool. */
    assert(w->async == false);
#endif /* RWF_NOWAIT */

#if defined(RWF_NOWAIT)
    /* Try to submit the write request asynchronously */
    if (w->async) {
        rv = UvOsIoSubmit(w->ctx, 1, &iocbs);

        /* If no error occurred, we're done, the write request was
         * submitted. */
        if (rv == 0) {
            goto done;
        }

        /* Check the reason of the error. */
        switch (rv) {
            case UV__EOPNOTSUPP:
                /* NOWAIT is not supported, this should not occur because we
                 * checked it in uvProbeIoCapabilities. */
                assert(0);
                break;
            case UV_EAGAIN:
                break;
            default:
                /* Unexpected error */
                errmsg = uvSysErrMsg("io_submit", rv);
                rv = UV__ERROR;
                goto err;
        }

        /* Submitting the write would block, or NOWAIT is not
         * supported. Let's run this request in the threadpool. */
        req->iocb.aio_flags &= ~IOCB_FLAG_RESFD;
        req->iocb.aio_resfd = 0;
        req->iocb.aio_rw_flags &= ~RWF_NOWAIT;
    }
#endif /* RWF_NOWAIT */

    /* If we got here it means we need to run io_submit in the threadpool. */
    req->work.data = req;

    rv =
        uv_queue_work(w->loop, &req->work, uvWriterWorkCb, uvWriterAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        errmsg = uvSysErrMsg("uv_queue_work", rv);
        rv = UV__ERROR;
        goto err;
    }

#if defined(RWF_NOWAIT)
done:
#endif /* RWF_NOWAIT */
    return 0;

err:
    assert(rv != 0);
    uvWriterSetErrMsg(w, errmsg);
    QUEUE_REMOVE(&req->queue);
    return rv;
}

void UvWriterCancel(struct UvWriterReq *req)
{
    req->canceled = true;
}
