#include "uv_fs.h"

#include <stdlib.h>
#include <string.h>
#include <sys/vfs.h>
#include <unistd.h>

#include "assert.h"
#include "err.h"
#include "heap.h"
#include "uv_error.h"
#include "uv_os.h"

int UvFsEnsureDir(const char *dir, struct ErrMsg *errmsg)
{
    struct uv_fs_s req;
    int rv;

    /* Make sure we have a directory we can write into. */
    rv = uv_fs_stat(NULL, &req, dir, NULL);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "stat", rv);
        return RAFT_IOERR;
    }

    if ((req.statbuf.st_mode & S_IFMT) != S_IFDIR) {
        ErrMsgPrintf(errmsg, "%s", uv_strerror(UV_ENOTDIR));
        return RAFT_IOERR;
    }

    return 0;
}

int UvFsSyncDir(const char *dir, struct ErrMsg *errmsg)
{
    uv_file fd;
    int rv;
    rv = UvOsOpen(dir, UV_FS_O_RDONLY | UV_FS_O_DIRECTORY, 0, &fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "open directory", rv);
        return UV__ERROR;
    }
    rv = UvOsFsync(fd);
    UvOsClose(fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "fsync directory", fd);
        return UV__ERROR;
    }
    return 0;
}

int UvFsFileExists(const char *dir,
                   const char *filename,
                   bool *exists,
                   struct ErrMsg *errmsg)
{
    uv_stat_t sb;
    char path[UV__PATH_SZ];
    int rv;

    UvOsJoin(dir, filename, path);

    rv = UvOsStat(path, &sb);
    if (rv != 0) {
        if (rv == UV_ENOENT) {
            *exists = false;
            goto out;
        }
        UvErrMsgSys(errmsg, "stat", rv);
        return UV__ERROR;
    }

    *exists = true;

out:
    return 0;
}

int UvFsFileIsEmpty(const char *dir,
                    const char *filename,
                    bool *empty,
                    struct ErrMsg *errmsg)
{
    uv_stat_t sb;
    char path[UV__PATH_SZ];
    int rv;

    UvOsJoin(dir, filename, path);

    rv = UvOsStat(path, &sb);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "stat", rv);
        return UV__ERROR;
    }
    *empty = sb.st_size == 0 ? true : false;
    return 0;
}

/* Open a file in a directory. */
static int uvFsOpenFile(const char *dir,
                        const char *filename,
                        int flags,
                        int mode,
                        uv_file *fd,
                        struct ErrMsg *errmsg)
{
    char path[UV__PATH_SZ];
    int rv;
    UvOsJoin(dir, filename, path);
    rv = UvOsOpen(path, flags, mode, fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "open", rv);
        return UV__ERROR;
    }
    return 0;
}

int UvFsOpenFileForReading(const char *dir,
                           const char *filename,
                           uv_file *fd,
                           struct ErrMsg *errmsg)
{
    char path[UV__PATH_SZ];
    int flags = O_RDONLY;

    UvOsJoin(dir, filename, path);

    return uvFsOpenFile(dir, filename, flags, 0, fd, errmsg);
}

int UvFsAllocateFile(const char *dir,
                     const char *filename,
                     size_t size,
                     uv_file *fd,
                     struct ErrMsg *errmsg)
{
    char path[UV__PATH_SZ];
    int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
    int rv = 0;

    UvOsJoin(dir, filename, path);

    rv = uvFsOpenFile(dir, filename, flags, S_IRUSR | S_IWUSR, fd, errmsg);
    if (rv != 0) {
        goto err;
    }

    /* Allocate the desired size. */
    rv = UvOsFallocate(*fd, 0, size);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        UvErrMsgSys(errmsg, "posix_fallocate", rv);
        rv = UV__ERROR;
        goto err_after_open;
    }

    return 0;

err_after_open:
    UvOsClose(*fd);
    UvOsUnlink(path);
err:
    assert(rv != 0);
    return rv;
}

static int uvFsWriteFile(const char *dir,
                         const char *filename,
                         int flags,
                         struct raft_buffer *bufs,
                         unsigned n_bufs,
                         struct ErrMsg *errmsg)
{
    uv_file fd;
    int rv;
    size_t size;
    unsigned i;
    size = 0;
    for (i = 0; i < n_bufs; i++) {
        size += bufs[i].len;
    }
    rv = uvFsOpenFile(dir, filename, flags, S_IRUSR | S_IWUSR, &fd, errmsg);
    if (rv != 0) {
        goto err;
    }
    rv = UvOsWrite(fd, (const uv_buf_t *)bufs, n_bufs, 0);
    if (rv != (int)(size)) {
        if (rv < 0) {
            UvErrMsgSys(errmsg, "write", rv);
        } else {
            ErrMsgPrintf(errmsg, "short write: %d only bytes written", rv);
        }
        goto err_after_file_open;
    }
    rv = UvOsFsync(fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "fsync", rv);
        goto err_after_file_open;
    }
    rv = UvOsClose(fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "close", rv);
        goto err;
    }
    return 0;

err_after_file_open:
    UvOsClose(fd);
err:
    return rv;
}

int UvFsMakeFile(const char *dir,
                 const char *filename,
                 struct raft_buffer *bufs,
                 unsigned n_bufs,
                 struct ErrMsg *errmsg)
{
    int flags = UV_FS_O_WRONLY | UV_FS_O_CREAT | UV_FS_O_EXCL;
    return uvFsWriteFile(dir, filename, flags, bufs, n_bufs, errmsg);
}

int UvFsMakeOrReplaceFile(const char *dir,
                          const char *filename,
                          struct raft_buffer *bufs,
                          unsigned n_bufs,
                          struct ErrMsg *errmsg)
{
    int flags = O_WRONLY | O_CREAT | O_SYNC | O_TRUNC;
    return uvFsWriteFile(dir, filename, flags, bufs, n_bufs, errmsg);
}

int UvFsFileHasOnlyTrailingZeros(uv_file fd, bool *flag, struct ErrMsg *errmsg)
{
    struct raft_buffer buf;
    off_t size;
    off_t offset;
    size_t i;
    int rv;

    /* Save the current offset. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Figure the size of the rest of the file. */
    size = lseek(fd, 0, SEEK_END);
    if (size == -1) {
        UvErrMsgSys(errmsg, "lseek", -errno);
        return UV__ERROR;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        UvErrMsgSys(errmsg, "lseek", -errno);
        return UV__ERROR;
    }

    buf.len = size;
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        ErrMsgPrintf(errmsg, "can't allocate read buffer");
        return UV__ERROR;
    }

    rv = UvFsReadInto(fd, &buf, errmsg);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < (size_t)size; i++) {
        if (((char *)buf.base)[i] != 0) {
            *flag = false;
            goto done;
        }
    }

    *flag = true;

done:
    raft_free(buf.base);

    return 0;
}

bool UvFsIsAtEof(uv_file fd)
{
    off_t offset;
    off_t size;
    offset = lseek(fd, 0, SEEK_CUR); /* Get the current offset */
    size = lseek(fd, 0, SEEK_END);   /* Get file size */
    lseek(fd, offset, SEEK_SET);     /* Restore current offset */
    return offset == size;           /* Compare current offset and size */
}

int UvFsReadInto(uv_file fd, struct raft_buffer *buf, struct ErrMsg *errmsg)
{
    int rv;
    /* TODO: use uv_fs_read() */
    rv = read(fd, buf->base, buf->len);
    if (rv == -1) {
        UvErrMsgSys(errmsg, "read", -errno);
        return UV__ERROR;
    }
    assert(rv >= 0);
    if ((size_t)rv < buf->len) {
        ErrMsgPrintf(errmsg, "short read: %d bytes instead of %ld", rv,
                     buf->len);
        return UV__NODATA;
    }
    return 0;
}

int UvFsReadFile(const char *dir,
                 const char *filename,
                 struct raft_buffer *buf,
                 struct ErrMsg *errmsg)
{
    uv_stat_t sb;
    char path[UV__PATH_SZ];
    uv_file fd;
    int rv;

    UvOsJoin(dir, filename, path);

    rv = UvOsStat(path, &sb);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "stat", rv);
        rv = RAFT_IOERR;
        goto err;
    }

    rv = uvFsOpenFile(dir, filename, O_RDONLY, 0, &fd, errmsg);
    if (rv != 0) {
        goto err;
    }

    buf->len = sb.st_size;
    buf->base = HeapMalloc(buf->len);
    if (buf->base == NULL) {
        ErrMsgPrintf(errmsg, "out of memory");
        rv = RAFT_NOMEM;
        goto err_after_open;
    }

    rv = UvFsReadInto(fd, buf, errmsg);
    if (rv != 0) {
        goto err_after_buf_alloc;
    }

    UvOsClose(fd);

    return 0;

err_after_buf_alloc:
    HeapFree(buf->base);
err_after_open:
    UvOsClose(fd);
err:
    return rv;
}

int UvFsReadFileInto(const char *dir,
                     const char *filename,
                     struct raft_buffer *buf,
                     struct ErrMsg *errmsg)
{
    uv_stat_t sb;
    char path[UV__PATH_SZ];
    uv_file fd;
    int rv;

    UvOsJoin(dir, filename, path);

    rv = UvOsStat(path, &sb);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "stat", rv);
        rv = UV__ERROR;
        goto err;
    }

    if (buf->len != sb.st_size) {
        ErrMsgPrintf(errmsg, "file has size %ld instead of %ld", sb.st_size,
                     buf->len);
        if (sb.st_size < buf->len) {
            rv = UV__NODATA;
        } else {
            rv = RAFT_CORRUPT;
        }
        goto err;
    }

    rv = uvFsOpenFile(dir, filename, O_RDONLY, 0, &fd, errmsg);
    if (rv != 0) {
        goto err;
    }

    rv = UvFsReadInto(fd, buf, errmsg);
    if (rv != 0) {
        goto err_after_open;
    }

    UvOsClose(fd);

    return 0;

err_after_open:
    UvOsClose(fd);
err:
    return rv;
}

int UvFsRemoveFile(const char *dir, const char *filename, struct ErrMsg *errmsg)
{
    char path[UV__PATH_SZ];
    int rv;
    UvOsJoin(dir, filename, path);
    rv = UvOsUnlink(path);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "unlink", rv);
        return UV__ERROR;
    }
    return 0;
}

int UvFsTruncateAndRenameFile(const char *dir,
                              size_t size,
                              const char *filename1,
                              const char *filename2,
                              struct ErrMsg *errmsg)
{
    char path1[UV__PATH_SZ];
    char path2[UV__PATH_SZ];
    uv_file fd;
    int rv;

    UvOsJoin(dir, filename1, path1);
    UvOsJoin(dir, filename2, path2);

    /* Truncate and rename. */
    rv = UvOsOpen(path1, UV_FS_O_RDWR, 0, &fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "open", rv);
        goto err;
    }
    rv = UvOsTruncate(fd, size);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "truncate", rv);
        goto err_after_open;
    }
    rv = UvOsFsync(fd);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "fsync", rv);
        goto err_after_open;
    }
    UvOsClose(fd);

    rv = UvOsRename(path1, path2);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "rename", rv);
        goto err;
    }

    return 0;

err_after_open:
    UvOsClose(fd);
err:
    return UV__ERROR;
}

/* Check if direct I/O is possible on the given fd. */
static int probeDirectIO(int fd, size_t *size, struct ErrMsg *errmsg)
{
    int flags;             /* Current fcntl flags. */
    struct statfs fs_info; /* To check the file system type. */
    void *buf;             /* Buffer to use for the probe write. */
    int rv;

    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);

    if (rv == -1) {
        if (errno != EINVAL) {
            /* UNTESTED: the parameters are ok, so this should never happen. */
            UvErrMsgSys(errmsg, "fnctl", -errno);
            return RAFT_IOERR;
        }
        rv = fstatfs(fd, &fs_info);
        if (rv == -1) {
            /* UNTESTED: in practice ENOMEM should be the only failure mode */
            UvErrMsgSys(errmsg, "fstatfs", -errno);
            return RAFT_IOERR;
        }
        switch (fs_info.f_type) {
            case 0x01021994: /* TMPFS_MAGIC */
            case 0x2fc12fc1: /* ZFS magic */
                *size = 0;
                return 0;
            default:
                /* UNTESTED: this is an unsupported file system. */
                ErrMsgPrintf(errmsg, "unsupported file system: %lx",
                             fs_info.f_type);
                return RAFT_IOERR;
        }
    }

    /* Try to peform direct I/O, using various buffer size. */
    *size = 4096;
    while (*size >= 512) {
        buf = raft_aligned_alloc(*size, *size);
        if (buf == NULL) {
            /* UNTESTED: TODO */
            ErrMsgPrintf(errmsg, "can't allocate write buffer");
            return RAFT_IOERR;
        }
        memset(buf, 0, *size);
        rv = write(fd, buf, *size);
        raft_free(buf);
        if (rv > 0) {
            /* Since we fallocate'ed the file, we should never fail because of
             * lack of disk space, and all bytes should have been written. */
            assert(rv == (int)(*size));
            return 0;
        }
        assert(rv == -1);
        if (errno != EIO && errno != EOPNOTSUPP) {
            /* UNTESTED: this should basically fail only because of disk errors,
             * since we allocated the file with posix_fallocate. */

            /* FIXME: this is a workaround because shiftfs doesn't return EINVAL
             * in the fnctl call above, for example when the underlying fs is
             * ZFS. */
            if (errno == EINVAL && *size == 4096) {
                *size = 0;
                return 0;
            }

            UvErrMsgSys(errmsg, "write", -errno);
            return RAFT_IOERR;
        }
        *size = *size / 2;
    }

    *size = 0;
    return 0;
}

#if defined(RWF_NOWAIT)
/* Check if fully non-blocking async I/O is possible on the given fd. */
static int probeAsyncIO(int fd, size_t size, bool *ok, struct ErrMsg *errmsg)
{
    void *buf;                  /* Buffer to use for the probe write */
    aio_context_t ctx = 0;      /* KAIO context handle */
    struct iocb iocb;           /* KAIO request object */
    struct iocb *iocbs = &iocb; /* Because the io_submit() API sucks */
    struct io_event event;      /* KAIO response object */
    int n_events;
    int rv;

    /* Setup the KAIO context handle */
    rv = UvOsIoSetup(1, &ctx);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "io_setup", rv);
        /* UNTESTED: in practice this should fail only with ENOMEM */
        return rv;
    }

    /* Allocate the write buffer */
    buf = raft_aligned_alloc(size, size);
    if (buf == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        ErrMsgPrintf(errmsg, "can't allocate write buffer");
        return RAFT_NOMEM;
    }
    memset(buf, 0, size);

    /* Prepare the KAIO request object */
    memset(&iocb, 0, sizeof iocb);
    iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
    *((void **)(&iocb.aio_buf)) = buf;
    iocb.aio_nbytes = size;
    iocb.aio_offset = 0;
    iocb.aio_fildes = fd;
    iocb.aio_reqprio = 0;
    iocb.aio_rw_flags |= RWF_NOWAIT | RWF_DSYNC;

    /* Submit the KAIO request */
    rv = UvOsIoSubmit(ctx, 1, &iocbs);
    if (rv != 0) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        raft_free(buf);
        UvOsIoDestroy(ctx);
        /* On ZFS 0.8 this is not properly supported yet. */
        if (errno == EOPNOTSUPP) {
            *ok = false;
            return 0;
        }
        UvErrMsgSys(errmsg, "io_submit", rv);
        return RAFT_IOERR;
    }

    /* Fetch the response: will block until done. */
    n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
    assert(n_events == 1);

    /* Release the write buffer. */
    raft_free(buf);

    /* Release the KAIO context handle. */
    rv = UvOsIoDestroy(ctx);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "io_destroy", rv);
        return RAFT_IOERR;
    }

    if (event.res > 0) {
        assert(event.res == (int)size);
        *ok = true;
    } else {
        /* UNTESTED: this should basically fail only because of disk errors,
         * since we allocated the file with posix_fallocate and the block size
         * is supposed to be correct. */
        assert(event.res != EAGAIN);
        *ok = false;
    }

    return 0;
}
#endif /* RWF_NOWAIT */

int UvFsProbeCapabilities(const char *dir,
                          size_t *direct,
                          bool *async,
                          struct ErrMsg *errmsg)
{
    char filename[UV__FILENAME_LEN]; /* Filename of the probe file */
    char path[UV__PATH_SZ];          /* Full path of the probe file */
    int fd;                          /* File descriptor of the probe file */
    int rv;

    /* Create a temporary probe file. */
    strcpy(filename, ".probe-XXXXXX");
    UvOsJoin(dir, filename, path);
    fd = mkstemp(path);
    if (fd == -1) {
        UvErrMsgSys(errmsg, "mkstemp", -errno);
        rv = RAFT_IOERR;
        goto err;
    }
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        UvErrMsgSys(errmsg, "posix_fallocate", -rv);
        rv = RAFT_IOERR;
        goto err_after_file_open;
    }
    UvOsUnlink(path);

    /* Check if we can use direct I/O. */
    rv = probeDirectIO(fd, direct, errmsg);
    if (rv != 0) {
        goto err_after_file_open;
    }

#if !defined(RWF_NOWAIT)
    /* We can't have fully async I/O, since io_submit might potentially block.
     */
    *async = false;
#else
    /* If direct I/O is not possible, we can't perform fully asynchronous
     * I/O, because io_submit might potentially block. */
    if (*direct == 0) {
        *async = false;
        goto out;
    }
    rv = probeAsyncIO(fd, *direct, async, errmsg);
    if (rv != 0) {
        goto err_after_file_open;
    }
#endif /* RWF_NOWAIT */

#if defined(RWF_NOWAIT)
out:
#endif /* RWF_NOWAIT */
    close(fd);
    return 0;

err_after_file_open:
    close(fd);
err:
    return rv;
}
