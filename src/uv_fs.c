#include <libgen.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <xfs/xfs.h>

#include <uv.h>

#include "assert.h"
#include "uv_fs.h"
#include "aio.h"

void raft_uv_fs__join(const char *dir, const char *filename, char *path)
{
    assert(strlen(dir) < RAFT_UV_FS_MAX_DIR_LEN);
    assert(strlen(filename) < RAFT_UV_FS_MAX_FILENAME_LEN);

    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

#if defined(RWF_NOWAIT)
/**
 * Check if @size is the correct block size for @fd. If it is, @ok will be set
 * to #true.
 */
static int raft_uv_fs__block_size_probe(int fd, size_t size, bool *ok)
{
    void *buf;                  /* Buffer to use for the probe write */
    aio_context_t ctx = 0;      /* KAIO context handle */
    struct iocb iocb;           /* KAIO request object */
    struct iocb *iocbs = &iocb; /* Because the io_submit() API sucks */
    struct io_event event;      /* KAIO response object */
    int rv;

    /* Setup the KAIO context handle */
    rv = io_setup(1, &ctx);
    if (rv == -1) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        return uv_translate_sys_error(errno);
    }

    /* Allocate the write buffer */
    buf = aligned_alloc(size, size);
    if (buf == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        return UV_ENOMEM;
    }
    memset(buf, 0, size);

    /* Prepare the KAIO request object */
    memset(&iocb, 0, sizeof iocb);
    iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
    iocb.aio_buf = (uint64_t)buf;
    iocb.aio_nbytes = size;
    iocb.aio_offset = 0;
    iocb.aio_fildes = fd;
    iocb.aio_reqprio = 0;
    iocb.aio_rw_flags |= RWF_NOWAIT | RWF_DSYNC;

    /* Submit the KAIO request */
    rv = io_submit(ctx, 1, &iocbs);
    if (rv == -1) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        free(buf);
        io_destroy(ctx);
        return uv_translate_sys_error(errno);
    }

    /* Fetch the response: will block until done. */
    do {
        rv = io_getevents(ctx, 1, 1, &event, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv == 1);

    /* Release the write buffer. */
    free(buf);

    /* Release the KAIO context handle. */
    io_destroy(ctx);

    /* We expect the write to either succeed or fail with EAGAIN (which means
     * this is not the correct block size). */
    if (event.res > 0) {
        *ok = true;
    } else {
        if (event.res != -EAGAIN) {
            /* UNTESTED: this should basically fail only because of disk errors,
             * since we allocated the file with posix_fallocate. */
            return uv_translate_sys_error(-event.res);
        }
        *ok = false;
    }

    return 0;
}
#endif /* RWF_NOWAIT */

int raft_uv_fs__block_size(const char *dir, size_t *size)
{
    struct statfs fs_info; /* To get the type code of the underlying fs */
    struct stat info;      /* To get the block size reported by the fs */
    raft_uv_path path;     /* To hold the path of probe file */
    int fd;                /* File descriptor of the probe file */
    int flags;             /* To hold the current fcntl flags */
    int rv;

    assert(dir != NULL);
    assert(size != NULL);

#if !defined(RWF_NOWAIT)
    /* If NOWAIT is not supported, just return 4096. In practice it should
     * always work fine. */
    *size = 4096;
    return 0;
#else
    assert(strlen(dir) < RAFT_UV_FS_MAX_DIR_LEN);

    /* Create a temporary probe file. */
    raft_uv_fs__join(dir, ".probe-XXXXXX", path);

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
            /* UNTEST: since the path and fd are valid, can this ever fail? */
            return rv;
        }

        /* TODO: this was taken from seastar's code which has this comment:
	 *
	 *   xfs wants at least the block size for writes
	 *   FIXME: really read the block size
	 *
	 * We should check with the xfs folks what's going on here and possibly
	 * how to figure the device block size.
	 */
        *size = attr.d_miniosz > 4096 ? attr.d_miniosz : 4096;

        return 0;
    }

    /* Get the file system type */
    rv = fstatfs(fd, &fs_info);
    if (rv == -1) {
        /* UNTESTED: in practice ENOMEM should be the only failure mode */
        close(fd);
        return uv_translate_sys_error(errno);
    }

    /* Special-case the file systems that do not support O_DIRECT/NOWAIT */
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
                /* UNTESTED: ENOMEM should be the only failure mode */
                return uv_translate_sys_error(errno);
            }
            /* TODO: The block size indicated in the ZFS case seems way to
             * high. Reducing it to 4096 seems safe since at the moment ZFS does
             * not support async writes. We might want to change this once ZFS
             * async support is released. */
            *size = info.st_blksize > 4096 ? 4096 : info.st_blksize;
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
        /* UNTESTED: this should actually never fail, for the file systems we
         * currently support. */
        close(fd);
        return uv_translate_sys_error(errno);
    }

    *size = 4096;
    while (*size >= 512) {
        bool ok;

        rv = raft_uv_fs__block_size_probe(fd, *size, &ok);
        if (rv != 0) {
            /* UNTESTED: all syscalls performed by underlying code should fail
             * at most with ENOMEM. */
            goto err;
        }

        if (ok) {
            close(fd);
            return 0;
        }

        *size = *size / 2;
    }

    /* UNTEST: at least one of the probed block sizes should work for the file
     * systems we currently support. */
    rv = UV_EINVAL;

err:
    assert(rv != 0);

    close(fd);

    return rv;
#endif /* RWF_NOWAIT */
}

/**
 * If set, invoke the request's callback.
 */
static void raft_uv_fs__done(struct raft_uv_fs *req)
{
    if (req->cb != NULL) {
        req->cb(req);
    }
}

/**
 * Run blocking syscalls involved in a file write.
 *
 * Perform a KAIO write request and synchronously wait for it to complete.
 *
 * This is called in UV's threadpool.
 */
static void raft_uv_fs__write_work_cb(uv_work_t *work)
{
    struct raft_uv_fs *req; /* Create file request object */
    aio_context_t ctx = 0;  /* KAIO handle */
    struct iocb *iocbs;     /* Pointer to KAIO request object */
    struct io_event event;  /* KAIO response object */
    int rv;

    assert(work != NULL);
    assert(work->data != NULL);

    req = work->data;

    iocbs = &req->iocb;

    /* Perform the request using a dedicated context, to avoid synchronization
     * issues between threads when multiple write requests are submitted in
     * parallel. This is suboptimal but in real-world users should use file
     * systems and kernels with proper async write support. */

    rv = io_setup(1 /* Maximum concurrent requests */, &ctx);
    if (rv == -1) {
        /* UNTESTED: should fail only with ENOMEM */
        goto out;
    }

    /* Submit the request */
    rv = io_submit(ctx, 1, &iocbs);
    if (rv == -1) {
        /* UNTESTED: since we're not using NOWAIT and the parameters are valid,
         * this shouldn't fail. */
        goto out_after_io_setup;
    }

    /* Wait for the request to complete */
    do {
        rv = io_getevents(ctx, 1, 1, &event, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv == 1);
    rv = 0;

out_after_io_setup:
    io_destroy(ctx);

out:
    if (rv != 0) {
        req->status = uv_translate_sys_error(errno);
    } else {
        req->status = event.res;
    }

    return;
}

/**
 * Callback run after @raft_uv_fs__write_work_cb has returned. It's run in
 * the main loop thread.
 */
static void raft_uv_fs__write_after_work_cb(uv_work_t *work, int status)
{
    struct raft_uv_fs *req; /* Create file request object */

    assert(work != NULL);

    assert(status == 0); /* We don't cancel worker requests */

    req = work->data;

    /* Possibly invoke the request's callback. */
    raft_uv_fs__done(req);
}

/**
 * Callback fired when the event fd that we're polling should be ready for
 * reading.
 */
static void raft_uv_fs__write_poll_cb(uv_poll_t *poller, int status, int events)
{
    struct raft_uv_file *f = poller->data; /* File handle */
    uint64_t completed;                    /* True if the write is complete */
    int rv;
    unsigned i;

    assert(f != NULL);
    assert(f->event_fd >= 0);

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
     * If we got here a least one write should have completed and io_events
     * should return immediately without blocking. */
    do {
        rv = io_getevents(f->ctx, 1, f->n_events, f->events, NULL);
    } while (rv == -1 && errno == EINTR);

    assert(rv >= 1);

    for (i = 0; i < (unsigned)rv; i++) {
        struct io_event *event = &f->events[i];
        struct raft_uv_fs *req = (void *)event->data;

#if defined(RWF_NOWAIT)
        /* If we got EAGAIN, it means it was not possible to perform the write
         * asynchronously, so let's fall back to the threadpool. */
        if (event->res == -EAGAIN) {
            req->iocb.aio_flags &= ~IOCB_FLAG_RESFD;
            req->iocb.aio_resfd = 0;
            req->iocb.aio_rw_flags &= ~RWF_NOWAIT;

            req->work.data = req;
            rv = uv_queue_work(f->loop, &req->work, raft_uv_fs__write_work_cb,
                               raft_uv_fs__write_after_work_cb);
            if (rv != 0) {
                /* UNTESTED: with the current libuv implementation this should
                 * never fail. */
                req->status = rv;
                goto invoke_cb;
            }

            return;
        }
#endif /* RWF_NOWAIT */

        req->status = event->res;

    invoke_cb:
        raft_uv_fs__done(req);
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
    int flags;             /* Current fcntl flags */
    struct statfs fs_info; /* To check the file system type */
    int rv;

    flags = fcntl(f->fd, F_GETFL);
    rv = fcntl(f->fd, F_SETFL, flags | O_DIRECT);

    if (rv == -1) {
        if (errno != EINVAL) {
            /* UNTESTED: the parameters are ok, so this should never happen. */
            return -1;
        }

        rv = fstatfs(f->fd, &fs_info);
        if (rv == -1) {
            /* UNTESTED: in practice ENOMEM should be the only failure mode */
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
                /* UNTESTED: this is an unsupported file system. */
                errno = EINVAL;
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
    raft_uv_path dir; /* The directory portion of path */
    int fd;           /* Directory file descriptor */
    int rv;

    /* Any path passed to us should honor the defined limits. */
    assert(strlen(path) < RAFT_UV_FS_MAX_PATH_LEN);

    strncpy(dir, path, RAFT_UV_FS_MAX_PATH_LEN);
    dirname(dir);

    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        /* UNTESTED: since the directory has been already accessed, this
         * shouldn't fail */
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
    struct raft_uv_fs *req;                  /* Create file request object */
    struct raft_uv_file *f;                  /* File handle */
    int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
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
        /* UNTESTED: should fail only in case of disk errors */
        rv = errno;
        goto err_after_open;
    }
    rv = raft_uv_fs__create_sync_dir(req->path);
    if (rv == -1) {
        /* UNTESTED: should fail only in case of disk errors */
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
        /* UNTESTED: should fail only with ENOMEM */
        rv = errno;
        goto err_after_open;
    }

    /* Setup the AIO context. */
    rv = io_setup(f->n_events /* Maximum concurrent requests */, &f->ctx);
    if (rv == -1) {
        /* UNTESTED: should fail only with ENOMEM */
        rv = errno;
        goto err_after_eventfd;
    }

    /* Initialize the array of re-usable event objects. */
    f->events = calloc(f->n_events, sizeof *f->events);
    if (f->events == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        goto err_after_io_setup;
    }

    req->status = 0;

    return;

err_after_io_setup:
    io_destroy(f->ctx);

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
 * Start polling the event file descriptor to get notified when a write
 * completes.
 */
static int raft_uv_fs__create_start_polling(struct raft_uv_file *f)
{
    int rv;

    rv = uv_poll_init(f->loop, &f->event_poller, f->event_fd);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this should never
         * fail. */
        goto err;
    }

    f->event_poller.data = f;

    rv =
        uv_poll_start(&f->event_poller, UV_READABLE, raft_uv_fs__write_poll_cb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this should never
         * fail. */
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
            /* UNTESTED: the underlying libuv calls should never fail. */
            req->status = rv;

            io_destroy(f->ctx);
            close(f->event_fd);
            close(f->fd);
            unlink(req->path);
        }
    }

    raft_uv_fs__done(req);
}

int raft_uv_fs__create(struct raft_uv_file *f,
                       struct raft_uv_fs *req,
                       struct uv_loop_s *loop,
                       const char *path,
                       size_t size,
                       unsigned max_n_writes,
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
    f->ctx = 0;

    f->events = NULL; /* We'll allocate this in the create callback */
    f->n_events = max_n_writes;

    req->file = f;
    req->cb = cb;
    req->path = path;
    req->size = size;
    req->status = 0;
    req->work.data = req;

    rv = uv_queue_work(f->loop, &req->work, raft_uv_fs__create_work_cb,
                       raft_uv_fs__create_after_work_cb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
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
        /* UNTESTED: with the current libuv implementation this can't fail. */
        return rv;
    }

    uv_close((struct uv_handle_s *)&f->event_poller, NULL);

    rv = close(f->event_fd);
    if (rv == -1) {
        /* UNTESTED: not clear how this could fail. */
        return uv_translate_sys_error(errno);
    }

    rv = io_destroy(f->ctx);
    if (rv == -1) {
        /* UNTESTED: not clear how this could fail. */
        return uv_translate_sys_error(errno);
    }

    rv = close(f->fd);
    if (rv == -1) {
        /* UNTESTED: not clear how this could fail. */
        return uv_translate_sys_error(errno);
    }

    free(f->events);

    return 0;
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
    assert(f->ctx != 0);

    assert(req != NULL);

    assert(bufs != NULL);
    assert(n > 0);

    req->file = f;
    req->cb = cb;

    memset(&req->iocb, 0, sizeof req->iocb);

    req->iocb.aio_data = (uint64_t)req;
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
     * without using the threadpool, unless we had previously detected that this
     * mode is not supported. */
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

        /* If no error occurred, we're done, the write request was
         * submitted. */
        if (rv != -1) {
            assert(rv == 1); /* TODO: can 0 be returned? */
            goto done;
        }

        /* Check the reason of the error. */
        switch (errno) {
            case EOPNOTSUPP:
                /* NOWAIT is not supported, fallback to sync mode */
                f->async = false;
            case EAGAIN:
                /* Submitting the write would block, or NOWAIT is not
                 * supported. Let's run this request in the threadpool. */
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
        /* UNTESTED: with the current libuv implementation this can't fail. */
        goto err;
    }

done:
    return 0;

err:
    assert(rv != 0);

    return rv;
}
