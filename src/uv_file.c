#include "uv_file.h"

#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <uv.h>

#include "assert.h"

/* State codes */
enum { CREATING = 1, READY, ERRORED, CLOSED };

/* Run blocking syscalls involved in file creation (e.g. posix_fallocate()). */
static void createWorkCb(uv_work_t *work)
{
    struct uvFileCreate *req; /* Create file request object */
    struct uvFile *f;         /* File handle */
    int rv;

    req = work->data;
    f = req->file;

    assert(f->state == CREATING);

    /* Allocate the desired size. */
    rv = posix_fallocate(f->fd, 0, req->size);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        uvErrMsgSys(req->errmsg, posix_fallocate, rv);
        goto err;
    }

    /* Sync the file and its directory */
    rv = fsync(f->fd);
    if (rv == -1) {
        /* UNTESTED: should fail only in case of disk errors */
        uvErrMsgSys(req->errmsg, fsync, errno);
        goto err;
    }
    rv = uvSyncDir(req->dir, req->errmsg);
    if (rv != 0) {
        /* UNTESTED: should fail only in case of disk errors */
        goto err;
    }

    /* Set direct I/O if available. */
    if (f->direct) {
        rv = uvSetDirectIo(f->fd, req->errmsg);
        if (rv != 0) {
            goto err;
        }
    }

    req->status = 0;
    return;

err:
    req->status = UV__ERROR;
}

/* Set the write status according the given result code. */
static void setWriteStatus(struct uvFileWrite *req, int result)
{
    if (result < 0) {
        uvErrMsgSys(req->errmsg, aio, -result);
        req->status = UV__ERROR;
    } else if ((size_t)result < req->len) {
        uvErrMsgPrintf(req->errmsg, "short write: %d bytes instead of %ld",
                       result, req->len);
        req->status = UV__ERROR;
    } else {
        req->status = 0;
    }
}

/* Run blocking syscalls involved in a file write request.
 *
 * Perform a KAIO write request and synchronously wait for it to complete. */
static void writeWorkCb(uv_work_t *work)
{
    struct uvFileWrite *req; /* Write file request object */
    struct uvFile *f;        /* File object */
    aio_context_t ctx;       /* KAIO handle */
    struct iocb *iocbs;      /* Pointer to KAIO request object */
    struct io_event event;   /* KAIO response object */
    int n_events;
    int rv;

    req = work->data;
    f = req->file;
    assert(f->state == READY);

    iocbs = &req->iocb;

    /* If more than one write in parallel is allowed, submit the AIO request
     * using a dedicated context, to avoid synchronization issues between
     * threads when multiple writes are submitted in parallel. This is
     * suboptimal but in real-world users should use file systems and kernels
     * with proper async write support. */
    if (f->n_events > 1) {
        ctx = 0;
        rv = uvIoSetup(1 /* Maximum concurrent requests */, &ctx, req->errmsg);
        if (rv != 0) {
            /* UNTESTED: should fail only with ENOMEM */
            goto out;
        }
    } else {
        ctx = f->ctx;
    }

    /* Submit the request */
    rv = uvIoSubmit(ctx, 1, &iocbs, req->errmsg);
    if (rv != 0) {
        /* UNTESTED: since we're not using NOWAIT and the parameters are valid,
         * this shouldn't fail. */
        goto out_after_io_setup;
    }

    /* Wait for the request to complete */
    rv = uvIoGetevents(ctx, 1, 1, &event, NULL, &n_events, req->errmsg);
    assert(n_events == 1);
    assert(rv == 0);

out_after_io_setup:
    if (f->n_events > 1) {
        uvTryIoDestroy(ctx);
    }

out:
    if (rv != 0) {
        req->status = UV__ERROR;
    } else {
        setWriteStatus(req, event.res);
    }

    return;
}

/* Remove the request from the queue of inflight writes and invoke the request
 * callback if set. */
static void writeFinish(struct uvFileWrite *req)
{
    QUEUE_REMOVE(&req->queue);
    req->cb(req, req->status, req->errmsg);
}

/* Invoked at the end of the closing sequence. It invokes the close callback. */
static void pollCloseCb(struct uv_handle_s *handle)
{
    struct uvFile *f = handle->data;
    uvErrMsg errmsg;
    int rv;

    assert(f->closing);
    assert(f->state != CLOSED);
    assert(QUEUE_IS_EMPTY(&f->write_queue));

    rv = close(f->event_fd);
    assert(rv == 0);
    if (f->ctx != 0) {
        rv = uvIoDestroy(f->ctx, errmsg);
        assert(rv == 0);
    }
    free(f->events);

    f->state = CLOSED;

    if (f->close_cb != NULL) {
        f->close_cb(f);
    }
}

/* Close the poller if the closing flag is on and there's no infiglht create or
   write request. */
static void maybeClosed(struct uvFile *f)
{
    assert(f->state != CLOSED);

    if (!f->closing) {
        return;
    }

    /* If are creating the file we need to wait for the create to finish. */
    if (f->state == CREATING) {
        return;
    }

    /* If are writing we need to wait for the writes to finish. */
    if (!QUEUE_IS_EMPTY(&f->write_queue)) {
        return;
    }

    if (!uv_is_closing((struct uv_handle_s *)&f->event_poller)) {
        uv_close((struct uv_handle_s *)&f->event_poller, pollCloseCb);
    }
}

/* Callback run after writeWorkCb has returned. It normally invokes the write
 * request callback. */
static void writeAfterWorkCb(uv_work_t *work, int status)
{
    struct uvFileWrite *req; /* Write file request object */
    struct uvFile *f;

    assert(status == 0); /* We don't cancel worker requests */

    req = work->data;
    f = req->file;

    assert(f->state == READY);

    /* If we were closed, let's mark the request as canceled, regardless of the
     * actual outcome. */
    if (f->closing) {
        uvErrMsgPrintf(req->errmsg, "canceled");
        req->status = UV__CANCELED;
    }

    writeFinish(req);
    maybeClosed(f);
}

/* Callback fired when the event fd associated with AIO write requests should be
 * ready for reading (i.e. when a write has completed). */
static void writePollCb(uv_poll_t *poller, int status, int events)
{
    struct uvFile *f = poller->data; /* File handle */
    uint64_t completed;              /* True if the write is complete */
    uvErrMsg errmsg;
    unsigned i;
    int n_events;
    int rv;

    assert(f != NULL);
    assert(f->event_fd >= 0);
    assert(f->state == READY);

    /* TODO: it's not clear when polling could fail. In this case we should
     * probably mark all pending requests as failed. */
    assert(status == 0);

    assert(events & UV_READABLE);

    /* Read the event file descriptor */
    rv = read(f->event_fd, &completed, sizeof completed);
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
    rv = uvIoGetevents(f->ctx, 1, f->n_events, f->events, NULL, &n_events,
                       errmsg);
    assert(rv == 0);

    for (i = 0; i < (unsigned)n_events; i++) {
        struct io_event *event = &f->events[i];
        struct uvFileWrite *req = *((void **)&event->data);

        /* If we are closing, we mark the write as canceled, although
         * technically it might have worked. */
        if (f->closing) {
            uvErrMsgPrintf(req->errmsg, "canceled");
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
            rv = uv_queue_work(f->loop, &req->work, writeWorkCb,
                               writeAfterWorkCb);
            if (rv != 0) {
                /* UNTESTED: with the current libuv implementation this should
                 * never fail. */
                uvErrMsgPrintf(req->errmsg, "uv_queue_work: %s",
                               uv_strerror(rv));
                req->status = UV__ERROR;
                goto finish;
            }
            return;
        }
#endif /* RWF_NOWAIT */

        setWriteStatus(req, event->res);

    finish:
        writeFinish(req);
    }

    /* If we've been closed, let's see if we can stop the poller and fire the
     * close callback. */
    maybeClosed(f);
}

/* Main loop callback run after @createWorkCb has returned. It normally starts
 * the eventfd poller to receive notifications about completed writes and invoke
 * the create request callback. */
static void createAfterWorkCb(uv_work_t *work, int status)
{
    struct uvFileCreate *req;
    struct uvFile *f;
    uvErrMsg errmsg;
    int rv;

    assert(status == 0); /* We don't cancel worker requests */
    req = work->data;
    assert(req != NULL);
    f = req->file;

    /* If we were closed, abort here. */
    if (f->closing) {
        uvTryUnlinkFile(req->dir, req->filename);
        uvErrMsgPrintf(req->errmsg, "canceled");
        req->status = UV__CANCELED;
        goto out;
    }

    /* If no error occurred, start polling the event file descriptor. */
    if (req->status == 0) {
        rv = uv_poll_start(&f->event_poller, UV_READABLE, writePollCb);
        if (rv != 0) {
            /* UNTESTED: the underlying libuv calls should never fail. */
            uvErrMsgPrintf(req->errmsg, "uv_poll_start: %s", uv_strerror(rv));
            uvIoDestroy(f->ctx, errmsg);
            close(f->event_fd);
            close(f->fd);
            uvTryUnlinkFile(req->dir, req->filename);
            req->status = UV__ERROR;
        }
    }

out:
    if (req->status == 0) {
        f->state = READY;
    } else {
        f->state = ERRORED;
    }

    if (req->cb != NULL) {
        req->cb(req, req->status, req->errmsg);
    }

    maybeClosed(f);
}

int uvFileInit(struct uvFile *f,
               struct uv_loop_s *loop,
               bool direct,
               bool async,
               char *errmsg)
{
    int rv;

    f->loop = loop;
    f->fd = -1;
    f->direct = direct;
    f->async = async;
    f->event_fd = -1;

    /* Create an event file descriptor to get notified when a write has
     * completed. */
    f->event_fd = eventfd(0, EFD_NONBLOCK);
    if (f->event_fd < 0) {
        /* UNTESTED: should fail only with ENOMEM */
        uvErrMsgPrintf(errmsg, "eventfd: %s", strerror(errno));
        rv = UV__ERROR;
        goto err;
    }

    rv = uv_poll_init(f->loop, &f->event_poller, f->event_fd);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this should never
         * fail. */
        uvErrMsgPrintf(errmsg, "uv_poll_init: %s", uv_strerror(rv));
        rv = UV__ERROR;
        goto err_after_event_fd;
    }
    f->event_poller.data = f;

    f->ctx = 0;
    f->events = NULL;
    f->n_events = 0;
    QUEUE_INIT(&f->write_queue);
    f->closing = false;
    f->close_cb = NULL;

    return 0;

err_after_event_fd:
    close(f->event_fd);
err:
    assert(rv != 0);
    return rv;
}

int uvFileCreate(struct uvFile *f,
                 struct uvFileCreate *req,
                 const char *dir,
                 const char *filename,
                 size_t size,
                 unsigned max_n_writes,
                 uvFileCreateCb cb,
                 char *errmsg)
{
    int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
    uvErrMsg errmsg2;
    int rv;

    assert(UV__DIR_HAS_VALID_LEN(dir));
    assert(UV__FILENAME_HAS_VALID_LEN(filename));
    assert(size > 0);
    assert(!f->closing);

    f->state = CREATING;

    req->file = f;
    req->cb = cb;
    req->dir = dir;
    req->filename = filename;
    req->size = size;
    req->status = 0;
    req->work.data = req;

#if !defined(RWF_DSYNC)
    /* If per-request synchronous I/O is not supported, open the file with the
     * sync flag. */
    flags |= O_DSYNC;
#endif

    f->events = NULL; /* We'll allocate this in the create callback */
    f->n_events = max_n_writes;

    /* Try to create a brand new file. */
    rv = uvOpenFile(dir, filename, flags, &f->fd, errmsg);
    if (rv != 0) {
        goto err;
    }

    /* Setup the AIO context. */
    rv = uvIoSetup(f->n_events /* Maximum concurrent requests */, &f->ctx,
                   errmsg);
    if (rv != 0) {
        goto err_after_open;
    }

    /* Initialize the array of re-usable event objects. */
    f->events = calloc(f->n_events, sizeof *f->events);
    if (f->events == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        uvErrMsgPrintf(errmsg, "failed to alloc events array");
        rv = UV__ERROR;
        goto err_after_io_setup;
    }

    rv = uv_queue_work(f->loop, &req->work, createWorkCb, createAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        uvErrMsgPrintf(errmsg, "uv_queue_work: %s", uv_strerror(rv));
        rv = UV__ERROR;
        goto err_after_open;
    }

    return 0;

err_after_io_setup:
    uvIoDestroy(f->ctx, errmsg2);
    f->ctx = 0;
err_after_open:
    close(f->fd);
    uvTryUnlinkFile(dir, filename);
    f->fd = -1;
err:
    assert(rv != 0);
    f->state = 0;
    return rv;
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

int uvFileWrite(struct uvFile *f,
                struct uvFileWrite *req,
                const uv_buf_t bufs[],
                unsigned n,
                size_t offset,
                uvFileWriteCb cb,
                char *errmsg)
{
    int rv;
#if defined(RWF_NOWAIT)
    struct iocb *iocbs = &req->iocb;
#endif /* RWF_NOWAIT */

    assert(!f->closing);
    assert(f->state == READY);

    /* TODO: at the moment we are not leveraging the support for concurrent
     *       writes, so ensure that we're getting write requests
     *       sequentially. */
    if (f->n_events == 1) {
        assert(QUEUE_IS_EMPTY(&f->write_queue));
    }

    assert(f->fd >= 0);
    assert(f->event_fd >= 0);
    assert(f->ctx != 0);
    assert(req != NULL);
    assert(bufs != NULL);
    assert(n > 0);

    req->file = f;
    req->cb = cb;
    req->len = lenOfBufs(bufs, n);
    memset(&req->iocb, 0, sizeof req->iocb);
    req->iocb.aio_fildes = f->fd;
    req->iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
    req->iocb.aio_reqprio = 0;
    *((void **)(&req->iocb.aio_buf)) = (void *)bufs;
    req->iocb.aio_nbytes = n;
    req->iocb.aio_offset = offset;
    *((void **)(&req->iocb.aio_data)) = (void *)req;

    QUEUE_PUSH(&f->write_queue, &req->queue);

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
    if (f->async) {
        req->iocb.aio_flags |= IOCB_FLAG_RESFD;
        req->iocb.aio_resfd = f->event_fd;
        req->iocb.aio_rw_flags |= RWF_NOWAIT;
    }
#else
    /* Since there's no support for NOWAIT, io_submit might occasionally block
     * and we need to run it in the threadpool. */
    assert(f->async == false);
#endif /* RWF_NOWAIT */

#if defined(RWF_NOWAIT)
    /* Try to submit the write request asynchronously */
    if (f->async) {
        rv = uvIoSubmit(f->ctx, 1, &iocbs, errmsg);

        /* If no error occurred, we're done, the write request was
         * submitted. */
        if (rv == 0) {
            goto done;
        }

        /* Check the reason of the error. */
        switch (rv) {
            case UV__NOTSUPP:
                /* NOWAIT is not supported, this should not occur because we
                 * checked it in uvProbeIoCapabilities. */
                assert(0);
                break;
            case UV__AGAIN:
                break;
            default:
                /* Unexpected error */
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

    rv = uv_queue_work(f->loop, &req->work, writeWorkCb, writeAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        uvErrMsgPrintf(errmsg, "uv_queue_work: %s", uv_strerror(rv));
        rv = UV__ERROR;
        goto err;
    }

#if defined(RWF_NOWAIT)
done:
#endif /* RWF_NOWAIT */
    return 0;

err:
    assert(rv != 0);
    QUEUE_REMOVE(&req->queue);
    return rv;
}

bool uvFileIsOpen(struct uvFile *f)
{
    return !f->closing;
}

void uvFileClose(struct uvFile *f, uvFileCloseCb cb)
{
    int rv;
    assert(!f->closing);

    f->closing = true;
    f->close_cb = cb;

    if (f->fd != -1) {
        rv = close(f->fd);
        assert(rv == 0);
    }

    maybeClosed(f);
}
