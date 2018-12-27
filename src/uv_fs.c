#include <assert.h>
#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <xfs/xfs.h>

#include <uv.h>

#include "uv_fs.h"

/**
 * Declaration of the AIO APIs that we use. This avoids having to depend on
 * libaio.
 */

static int io_setup(unsigned nr, aio_context_t *ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

static int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

static int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

static int io_getevents(aio_context_t ctx,
                        long min_nr,
                        long max_nr,
                        struct io_event *events,
                        struct timespec *timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}

/**
 * Check if @size is the correct block size for @fd.
 */
#if defined(RWF_NOWAIT)
static int raft_uv_fs__block_size_probe(int fd, size_t size, bool *ok)
{
    void *data;
    uv_buf_t buf;
    int rv;
    struct iocb iocb;
    struct iocb *iocbs = &iocb;
    aio_context_t ctx = 0;
    struct io_event event;

    data = aligned_alloc(size, size);
    if (data == NULL) {
        return UV_ENOMEM;
    }

    rv = io_setup(1, &ctx);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    memset(data, 0, size);

    buf.base = (void *)data;
    buf.len = size;

    memset(&iocb, 0, sizeof iocb);

    iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
    iocb.aio_buf = (uint64_t)&buf;
    iocb.aio_nbytes = 1;
    iocb.aio_offset = 0;
    iocb.aio_fildes = fd;
    iocb.aio_reqprio = 0;
    iocb.aio_rw_flags |= RWF_NOWAIT | RWF_DSYNC;

    rv = io_submit(ctx, 1, &iocbs);
    if (rv == -1) {
        free(data);
        io_destroy(ctx);
        return uv_translate_sys_error(errno);
    }

    do {
        rv = io_getevents(ctx, 1, 1, &event, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv == 1);

    io_destroy(ctx);

    /* We expect the write to either succeed or fail with EAGAIN (which means
     * this is not the correct block size). */
    if (event.res > 0) {
        *ok = true;
    } else {
        if (event.res != -EAGAIN) {
            free(data);
            return uv_translate_sys_error(-event.res);
        }
        *ok = false;
    }

    free(data);

    return 0;
}
#endif /* RWF_NOWAIT */

int raft_uv_fs__block_size(const char *dir, size_t *size)
{
    struct statfs fs_info;
    struct stat info;
    char path[RAFT_UV_FS_MAX_PATH_LEN];
    int fd;
    int flags;
    int rv;

    assert(dir != NULL);
    assert(size != NULL);

#if !defined(RWF_NOWAIT)
    /* If NOWAIT is not supported, just return 4096 since in practice it should
     * always work fine. */
    *size = 4096;
    return 0;
#else
    assert(strlen(dir) < RAFT_UV_FS_MAX_DIR_LEN);

    /* Create a temporary probe file. */
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, ".probe-XXXXXX");

    fd = mkstemp(path);
    if (fd == -1) {
        return uv_translate_sys_error(errno);
    }
    unlink(path);

    /* For XFS, we can use the dedicated API to figure out the optimal block
     * size */
    if (platform_test_xfs_fd(fd)) {
        struct dioattr attr;

        rv = xfsctl(path, fd, XFS_IOC_DIOINFO, &attr);

        close(fd);

        if (rv != 0) {
            return rv;
        }

        *size = attr.d_miniosz;

        return 0;
    }

    /* Get the file system type */
    rv = fstatfs(fd, &fs_info);
    if (rv == -1) {
        close(fd);
        return uv_translate_sys_error(errno);
    }

    switch (fs_info.f_type) {
        case 0x01021994: /* tmpfs */
            /* 4096 is ok. */
            *size = 4096;
            close(fd);
            return 0;

        case 0x2fc12fc1: /* ZFS */
            /* Let's use whatever stat() returns. */
            rv = fstat(fd, &info);
            close(fd);
            if (rv != 0) {
                return uv_translate_sys_error(errno);
            }
            *size = info.st_blksize;
            return 0;
    }

    /* For all other file systems, we try to probe the correct size by trial and
     * error. */
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        close(fd);
        return uv_translate_sys_error(rv);
    }

    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);
    if (rv == -1) {
        close(fd);
        return uv_translate_sys_error(errno);
    }

    *size = 4096;
    while (*size >= 512) {
        bool ok;

        rv = raft_uv_fs__block_size_probe(fd, *size, &ok);
        if (rv != 0) {
            close(fd);
            return rv;
        }

        if (ok) {
            close(fd);
            return 0;
        }

        *size = *size / 2;
    }

    return UV_EINVAL;
#endif /* RWF_NOWAIT */
}

/**
 * If set, invoke the request's callback.
 */
static void raft_uv_fs__invoke_cb(struct raft_uv_fs *req)
{
    if (req->cb != NULL) {
        req->cb(req);
    }
}

/**
 * Attempt to use direct I/O. If we can't, check if the file system does not
 * support direct I/O and ignore the error in that case.
 *
 * This is called in UV's threadpool.
 */
static int raft_uv_fs__create_set_direct_io(struct raft_uv_file *f)
{
    int flags;
    struct statfs fs_info;
    int rv;

    flags = fcntl(f->fd, F_GETFL);
    rv = fcntl(f->fd, F_SETFL, flags | O_DIRECT);

    if (rv == -1) {
        if (errno != EINVAL) {
            return -1;
        }

        rv = fstatfs(f->fd, &fs_info);
        if (rv == -1) {
            return -1;
        }

        switch (fs_info.f_type) {
            case 0x01021994: /* TMPFS_MAGIC */
            case 0x2fc12fc1: /* ZFS magic */
                /* If director I/O is not supported, then io_submit will be
                 * blocking. */
                f->async = false;
                break;
            default:
                return -1;
        }
    }

    return 0;
}

/**
 * Sync the directory where @path lives in. This is necessary in order to ensure
 * that the new entry is saved in the directory inode.
 *
 * This is called in UV's threadpool.
 */
static int raft_uv_fs__create_sync_dir(const char *path)
{
    char dir[RAFT_UV_FS_MAX_PATH_LEN];
    int fd;
    int rv;

    /* Any path passed to us should honor the defined limits. */
    assert(strlen(path) < RAFT_UV_FS_MAX_PATH_LEN);

    strncpy(dir, path, RAFT_UV_FS_MAX_PATH_LEN);
    dirname(dir);

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return -1;
    }

    rv = fsync(fd);

    close(fd);

    return rv;
}

/**
 * Run blocking syscalls involved in file creation.
 *
 * This is called in UV's threadpool.
 */
static void raft_uv_fs__create_work_cb(uv_work_t *work)
{
    struct raft_uv_fs *req;
    struct raft_uv_file *f;
    int flags = O_WRONLY | O_CREAT | O_EXCL;
    int rv;

    assert(work != NULL);

    req = work->data;
    f = req->file;

#if !defined(RWF_DSYNC)
    /* If per-request synchronous I/O is not supported, open the file with the
     * sync flag. */
    flags |= O_DSYNC;
#endif

    /* First of all, try to create a brand new file. */
    f->fd = open(req->path, flags, S_IRUSR | S_IWUSR);
    if (f->fd == -1) {
        rv = errno;
        goto err;
    }

    /* Allocate the desired size. */
    rv = posix_fallocate(f->fd, 0, req->size);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        goto err_after_open;
    }

    /* Sync the file and its directory */
    rv = fsync(f->fd);
    if (rv == -1) {
        rv = errno;
        goto err_after_open;
    }
    rv = raft_uv_fs__create_sync_dir(req->path);
    if (rv == -1) {
        rv = errno;
        goto err_after_open;
    }

    /* Set direct I/O if possible. */
    rv = raft_uv_fs__create_set_direct_io(f);
    if (rv == -1) {
        rv = errno;
        goto err_after_open;
    }

    /* Create an event file descriptor to get notified when a write has
     * completed. */
    f->event_fd = eventfd(0, EFD_NONBLOCK);
    if (f->event_fd < 0) {
        rv = errno;
        goto err_after_open;
    }

    /* Setup the AIO context. */
    rv = io_setup(1 /* Maximum concurrent requests */, &f->ctx);
    if (rv == -1) {
        rv = errno;
        goto err_after_eventfd;
    }

    return;

err_after_eventfd:
    close(f->event_fd);
    f->event_fd = -1;

err_after_open:
    close(f->fd);
    unlink(req->path);
    f->fd = -1;

err:
    req->status = uv_translate_sys_error(rv);
}

/**
 * Callback fired when the event fd that we're polling should be ready for
 * reading.
 */
static void raft_uv_fs__poll_cb(uv_poll_t *poller, int status, int events)
{
    struct raft_uv_fs *req = poller->data;
    struct raft_uv_file *f = req->file;
    uint64_t completed;
    struct io_event event;
    int rv;

    assert(req != NULL);
    assert(req->data != NULL);
    assert(f->event_fd >= 0);

    if (status != 0) {
        req->status = status;
        goto invoke_cb;
    }

    assert(events & UV_READABLE);

    rv = read(f->event_fd, &completed, sizeof completed);
    if (rv != sizeof completed) {
        /* According to eventfd(2) this is the only possible failure mode,
         * meaning that epoll has indicated that the event FD is not yet
         * ready. */
        assert(errno == EAGAIN);
        return;
    }

    assert(completed == 1);

    /* TODO: set a timeout? in theory if we got here the write was completed and
     * io_events should return immediately without blocking */
    do {
        rv = io_getevents(f->ctx, 1, 1, &event, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv == 1);

    req->status = event.res;

invoke_cb:
    poller->data = NULL; /* The write request has been completed */

    raft_uv_fs__invoke_cb(req);
}

/**
 * Start polling the event file descriptor to get notified when a write
 * completes.
 */
static int raft_uv_fs__create_start_polling(struct raft_uv_file *f)
{
    int rv;

    rv = uv_poll_init(f->loop, &f->event_poller, f->event_fd);
    if (rv != 0) {
        goto err;
    }

    rv = uv_poll_start(&f->event_poller, UV_READABLE, raft_uv_fs__poll_cb);
    if (rv != 0) {
        goto err_after_init;
    }

    return 0;

err_after_init:
    uv_close((struct uv_handle_s *)&f->event_poller, NULL);

err:
    assert(rv != 0);

    return rv;
}

/**
 * Callback run after @raft_uv_fs__create_work_cb has returned. It's run in
 * the main loop thread.
 */
static void raft_uv_fs__create_after_work_cb(uv_work_t *work, int status)
{
    struct raft_uv_fs *req;
    struct raft_uv_file *f;
    int rv;

    assert(work != NULL);

    assert(status == 0); /* We don't cancel worker requests */

    req = work->data;
    f = req->file;

    /* If no error occurred, start polling the event file descriptor. */
    if (req->status == 0) {
        rv = raft_uv_fs__create_start_polling(f);
        if (rv != 0) {
            req->status = rv;

            io_destroy(f->ctx);
            close(f->event_fd);
            close(f->fd);
            unlink(req->path);
        }
    }

    raft_uv_fs__invoke_cb(req);
}

int raft_uv_fs__create(struct raft_uv_file *f,
                       struct raft_uv_fs *req,
                       struct uv_loop_s *loop,
                       const char *path,
                       size_t size,
                       raft_uv_fs_cb cb)
{
    int rv;

    assert(f != NULL);
    assert(req != NULL);
    assert(loop != NULL);
    assert(path != NULL);
    assert(size > 0);

    f->loop = loop;
    f->fd = -1;
    f->async = true;
    f->event_fd = -1;
    f->event_poller.data = NULL;
    f->ctx = 0;

    req->file = f;
    req->cb = cb;
    req->path = path;
    req->size = size;
    req->status = 0;
    req->work.data = req;

    rv = uv_queue_work(f->loop, &req->work, raft_uv_fs__create_work_cb,
                       raft_uv_fs__create_after_work_cb);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_uv_fs__close(struct raft_uv_file *f)
{
    int rv;

    assert(f != NULL);

    rv = uv_poll_stop(&f->event_poller);
    if (rv != 0) {
        return rv;
    }

    uv_close((struct uv_handle_s *)&f->event_poller, NULL);

    rv = close(f->event_fd);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    rv = io_destroy(f->ctx);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    rv = close(f->fd);
    if (rv == -1) {
        return uv_translate_sys_error(errno);
    }

    return 0;
}

/**
 * Run blocking syscalls involved in a file write.
 *
 * This is called in UV's threadpool.
 */
static void raft_uv_fs__write_work_cb(uv_work_t *work)
{
    int rv;
    struct raft_uv_fs *req;
    struct raft_uv_file *f;
    struct iocb *iocbs;
    struct io_event event;

    assert(work != NULL);
    assert(work->data != NULL);

    req = work->data;
    f = req->file;

    iocbs = &req->iocb;

    /* Submit the request */
    rv = io_submit(f->ctx, 1, &iocbs);
    if (rv == -1) {
        req->status = uv_translate_sys_error(errno);
    }

    /* Wait for the request to complete */
    do {
        rv = io_getevents(f->ctx, 1, 1, &event, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv == 1);

    req->status = event.res;
}

/**
 * Callback run after @raft_uv_fs__write_work_cb has returned. It's run in
 * the main loop thread.
 */
static void raft_uv_fs__write_after_work_cb(uv_work_t *work, int status)
{
    struct raft_uv_fs *req;
    struct raft_uv_file *f;

    assert(work != NULL);

    assert(status == 0); /* We don't cancel worker requests */

    req = work->data;
    f = req->file;

    /* Invoke the callback. */
    f->event_poller.data = NULL;
    raft_uv_fs__invoke_cb(req);
}

int raft_uv_fs__write(struct raft_uv_file *f,
                      struct raft_uv_fs *req,
                      const uv_buf_t bufs[],
                      unsigned n,
                      size_t offset,
                      raft_uv_fs_cb cb)
{
    int rv;
    struct iocb *iocbs = &req->iocb;

    assert(f != NULL);
    assert(f->fd >= 0);
    assert(f->event_fd >= 0);
    assert(f->event_poller.data == NULL); /* No other request in progress */
    assert(f->ctx != 0);

    assert(req != NULL);

    assert(bufs != NULL);
    assert(n > 0);

    req->file = f;
    req->cb = cb;
    f->event_poller.data = req;

    memset(&req->iocb, 0, sizeof req->iocb);

    req->iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
    req->iocb.aio_buf = (uint64_t)bufs;
    req->iocb.aio_nbytes = n;
    req->iocb.aio_offset = offset;
    req->iocb.aio_fildes = f->fd;
    req->iocb.aio_reqprio = 0;

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
     * without using the threadpool, unless we had previously detected that it
     * is not possible. */
    if (f->async) {
        req->iocb.aio_flags |= IOCB_FLAG_RESFD;
        req->iocb.aio_resfd = f->event_fd;
        req->iocb.aio_rw_flags |= RWF_NOWAIT;
    }
#else
    /* Since there's no support for NOWAIT, io_submit might occasionally block
     * and we need to run it in the threadpool. */
    f->async = false;
#endif /* RWF_NOWAIT */

#if defined(RWF_NOWAIT)
    /* Try to submit the write request asynchronously */
    if (f->async) {
        rv = io_submit(f->ctx, 1, &iocbs);

        if (rv != -1) {
            /* If no error occurred, we're done, the write request was
             * submitted. */
            assert(rv == 1); /* TODO: can 0 be returned? */
            return 0;
        }

        /* Check the reason of the error. */
        switch (errno) {
            case EOPNOTSUPP:
                /* NOWAIT is not supported, fallback to sync mode */
                f->async = false;
            case EAGAIN:
                /* Submitting the write would block, or NOWAIT is not
                 * supported at all, let's run this request in the
                 * threadpool. */
                req->iocb.aio_flags &= ~IOCB_FLAG_RESFD;
                req->iocb.aio_resfd = 0;
                req->iocb.aio_rw_flags &= ~RWF_NOWAIT;
                break;
            default:
                /* Unexpected error */
                rv = uv_translate_sys_error(errno);
                goto err;
        }
    }
#endif /* RWF_NOWAIT */

    /* If we got here it means we need to run io_submit in the threadpool. */
    req->work.data = req;

    rv = uv_queue_work(f->loop, &req->work, raft_uv_fs__write_work_cb,
                       raft_uv_fs__write_after_work_cb);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    assert(rv != 0);

    f->event_poller.data = NULL;

    return rv;
}
