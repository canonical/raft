#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <unistd.h>

#include "assert.h"
#include "uv_error.h"
#include "uv_os.h"

/* Default permissions when creating a directory. */
#define DEFAULT_DIR_PERM 0700

void uvJoin(const uvDir dir, const uvFilename filename, uvPath path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

int uvEnsureDir(const uvDir dir, char *errmsg)
{
    struct stat sb;
    int rv;

    /* Check that the given path doesn't exceed our static buffer limit */
    assert(strnlen(dir, UV__DIR_MAX_LEN + 1) <= UV__DIR_MAX_LEN);

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, DEFAULT_DIR_PERM);
            if (rv != 0) {
                uvErrMsgSys(errmsg, mkdir, errno);
                return UV__ERROR;
            }
        } else {
            uvErrMsgSys(errmsg, stat, errno);
            return UV__ERROR;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        uvErrMsgPrintf(errmsg, "%s", strerror(ENOTDIR));
        return UV__ERROR;
    }

    return 0;
}

int uvSyncDir(const uvDir dir, char *errmsg)
{
    int fd;
    int rv;
    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        uvErrMsgSys(errmsg, open, errno);
        return UV__ERROR;
    }
    rv = fsync(fd);
    close(fd);
    if (rv == -1) {
        uvErrMsgSys(errmsg, fsync, errno);
        return UV__ERROR;
    }
    return 0;
}

int uvScanDir(const uvDir dir,
              struct dirent ***entries,
              int *n_entries,
              char *errmsg)
{
    int rv;
    rv = scandir(dir, entries, NULL, alphasort);
    if (rv == -1) {
        uvErrMsgSys(errmsg, scandir, errno);
        return UV__ERROR;
    }
    *n_entries = rv;
    return 0;
}

int uvOpenFile(const uvDir dir,
               const uvFilename filename,
               int flags,
               int *fd,
               char *errmsg)
{
    uvPath path;
    uvJoin(dir, filename, path);
    *fd = open(path, flags, S_IRUSR | S_IWUSR);
    if (*fd == -1) {
        uvErrMsgSys(errmsg, open, errno);
        return errno == ENOENT ? UV__NOENT : UV__ERROR;
    }
    return 0;
}

int uvStatFile(const uvDir dir,
               const uvFilename filename,
               struct stat *sb,
               char *errmsg)
{
    uvPath path;
    int rv;
    uvJoin(dir, filename, path);
    rv = stat(path, sb);
    if (rv == -1) {
        uvErrMsgSys(errmsg, stat, errno);
        return errno == ENOENT ? UV__NOENT : UV__ERROR;
    }
    return 0;
}

int uvMakeFile(const uvDir dir,
               const uvFilename filename,
               struct raft_buffer *bufs,
               unsigned n_bufs,
               char *errmsg)
{
    int flags = O_WRONLY | O_CREAT | O_EXCL;
    int fd;
    int rv;
    size_t size;
    unsigned i;
    size = 0;
    for (i = 0; i < n_bufs; i++) {
        size += bufs[i].len;
    }
    rv = uvOpenFile(dir, filename, flags, &fd, errmsg);
    if (rv != 0) {
        return rv;
    }
    rv = writev(fd, (const struct iovec *)bufs, n_bufs);
    if (rv != (int)(size)) {
        if (rv == -1) {
            uvErrMsgSys(errmsg, writev, errno);
        } else {
            assert(rv >= 0);
            uvErrMsgPrintf(errmsg, "short write: %d only bytes written", rv);
        }
        goto err_after_file_open;
    }
    rv = fsync(fd);
    if (rv == -1) {
        uvErrMsgSys(errmsg, fsync, errno);
        goto err_after_file_open;
    }
    rv = close(fd);
    if (rv == -1) {
        uvErrMsgSys(errmsg, close, errno);
        goto err;
    }
    return 0;

err_after_file_open:
    close(fd);
err:
    return UV__ERROR;
}

int uvUnlinkFile(const char *dir, const char *filename, char *errmsg)
{
    uvPath path;
    int rv;
    uvJoin(dir, filename, path);
    rv = unlink(path);
    if (rv == -1) {
        uvErrMsgSys(errmsg, unlink, errno);
        return UV__ERROR;
    }
    return 0;
}

void uvTryUnlinkFile(const char *dir, const char *filename)
{
    uvErrMsg errmsg;
    uvUnlinkFile(dir, filename, errmsg);
}

int uvTruncateFile(const uvDir dir,
                   const uvFilename filename,
                   size_t offset,
                   char *errmsg)
{
    uvPath path;
    int fd;
    int rv;
    uvJoin(dir, filename, path);
    rv = uvOpenFile(dir, filename, O_RDWR, &fd, errmsg);
    if (rv != 0) {
        goto err;
    }
    rv = ftruncate(fd, offset);
    if (rv == -1) {
        uvErrMsgSys(errmsg, ftruncate, errno);
        goto err_after_open;
    }
    rv = fsync(fd);
    if (rv == -1) {
        uvErrMsgSys(errmsg, fsync, errno);
        goto err_after_open;
    }
    close(fd);
    return 0;

err_after_open:
    close(fd);
err:
    return UV__ERROR;
}

int uvRenameFile(const uvDir dir,
                 const uvFilename filename1,
                 const uvFilename filename2,
                 char *errmsg)
{
    uvPath path1;
    uvPath path2;
    int rv;
    uvJoin(dir, filename1, path1);
    uvJoin(dir, filename2, path2);
    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        uvErrMsgSys(errmsg, rename, errno);
        return UV__ERROR;
    }
    rv = uvSyncDir(dir, errmsg);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

int uvIsEmptyFile(const uvDir dir,
                  const uvFilename filename,
                  bool *empty,
                  char *errmsg)
{
    struct stat sb;
    int rv;
    rv = uvStatFile(dir, filename, &sb, errmsg);
    if (rv != 0) {
        return rv;
    }
    *empty = sb.st_size == 0 ? true : false;
    return 0;
}

int uvReadFully(const int fd, void *buf, const size_t n, char *errmsg)
{
    int rv;
    rv = read(fd, buf, n);
    if (rv == -1) {
        uvErrMsgSys(errmsg, read, errno);
        return UV__ERROR;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        uvErrMsgPrintf(errmsg, "short read: %d bytes instead of %ld", rv, n);
        return UV__NODATA;
    }
    return 0;
}

int uvWriteFully(const int fd, void *buf, const size_t n, char *errmsg)
{
    int rv;
    rv = write(fd, buf, n);
    if (rv == -1) {
        uvErrMsgSys(errmsg, write, errno);
        return UV__ERROR;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        uvErrMsgPrintf(errmsg, "short write: %d bytes instead of %ld", rv, n);
        return UV__ERROR;
    }
    return 0;
}

int uvIsFilledWithTrailingZeros(const int fd, bool *flag, char *errmsg)
{
    off_t size;
    off_t offset;
    char *data;
    size_t i;
    int rv;

    /* Save the current offset. */
    offset = lseek(fd, 0, SEEK_CUR);

    /* Figure the size of the rest of the file. */
    size = lseek(fd, 0, SEEK_END);
    if (size == -1) {
        uvErrMsgSys(errmsg, lseek, errno);
        return UV__ERROR;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        uvErrMsgSys(errmsg, lseek, errno);
        return UV__ERROR;
    }

    data = raft_malloc(size);
    if (data == NULL) {
        uvErrMsgPrintf(errmsg, "can't allocate read buffer");
        return UV__ERROR;
    }

    rv = uvReadFully(fd, data, size, errmsg);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < (size_t)size; i++) {
        if (data[i] != 0) {
            *flag = false;
            goto done;
        }
    }

    *flag = true;

done:
    raft_free(data);

    return 0;
}

bool uvIsAtEof(const int fd)
{
    off_t offset;
    off_t size;
    offset = lseek(fd, 0, SEEK_CUR); /* Get the current offset */
    size = lseek(fd, 0, SEEK_END);   /* Get file size */
    lseek(fd, offset, SEEK_SET);     /* Restore current offset */
    return offset == size;           /* Compare current offset and size */
}

/* Check if direct I/O is possible on the given fd. */
static int probeDirectIO(int fd, size_t *size, char *errmsg)
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
            uvErrMsgSys(errmsg, fnctl, errno);
            return UV__ERROR;
        }
        rv = fstatfs(fd, &fs_info);
        if (rv == -1) {
            /* UNTESTED: in practice ENOMEM should be the only failure mode */
            uvErrMsgSys(errmsg, fstatfs, errno);
            return UV__ERROR;
        }
        switch (fs_info.f_type) {
            case 0x01021994: /* TMPFS_MAGIC */
            case 0x2fc12fc1: /* ZFS magic */
                *size = 0;
                return 0;
            default:
                /* UNTESTED: this is an unsupported file system. */
                uvErrMsgPrintf(errmsg, "unsupported file system: %lx",
                               fs_info.f_type);
                return UV__ERROR;
        }
    }

    /* Try to peform direct I/O, using various buffer size. */
    *size = 4096;
    while (*size >= 512) {
        buf = raft_aligned_alloc(*size, *size);
        if (buf == NULL) {
            /* UNTESTED: TODO */
            uvErrMsgPrintf(errmsg, "can't allocate write buffer");
            return UV__ERROR;
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

            uvErrMsgSys(errmsg, write, errno);
            return UV__ERROR;
        }
        *size = *size / 2;
    }

    *size = 0;
    return 0;
}

#if defined(RWF_NOWAIT)
/* Check if fully non-blocking async I/O is possible on the given fd. */
static int probeAsyncIO(int fd, size_t size, bool *ok, char *errmsg)
{
    void *buf;                  /* Buffer to use for the probe write */
    aio_context_t ctx = 0;      /* KAIO context handle */
    struct iocb iocb;           /* KAIO request object */
    struct iocb *iocbs = &iocb; /* Because the io_submit() API sucks */
    struct io_event event;      /* KAIO response object */
    int n_events;
    int rv;

    /* Setup the KAIO context handle */
    rv = uvIoSetup(1, &ctx, errmsg);
    if (rv != 0) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        return rv;
    }

    /* Allocate the write buffer */
    buf = raft_aligned_alloc(size, size);
    if (buf == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        uvErrMsgPrintf(errmsg, "can't allocate write buffer");
        return UV__ERROR;
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
    rv = uvIoSubmit(ctx, 1, &iocbs, errmsg);
    if (rv != 0) {
        /* UNTESTED: in practice this should fail only with ENOMEM */
        raft_free(buf);
        uvTryIoDestroy(ctx);
        /* On ZFS 0.8 this is not properly supported yet. */
        if (errno == EOPNOTSUPP) {
            *ok = false;
            return 0;
        }
        return rv;
    }

    /* Fetch the response: will block until done. */
    rv = uvIoGetevents(ctx, 1, 1, &event, NULL, &n_events, errmsg);
    assert(rv == 0);
    assert(n_events == 1);

    /* Release the write buffer. */
    raft_free(buf);

    /* Release the KAIO context handle. */
    rv = uvIoDestroy(ctx, errmsg);
    if (rv != 0) {
        return rv;
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

int uvProbeIoCapabilities(const uvDir dir,
                          size_t *direct,
                          bool *async,
                          char *errmsg)
{
    uvFilename filename; /* Filename of the probe file */
    uvPath path;         /* Full path of the probe file */
    int fd;              /* File descriptor of the probe file */
    int rv;

    /* Create a temporary probe file. */
    strcpy(filename, ".probe-XXXXXX");
    uvJoin(dir, filename, path);
    fd = mkstemp(path);
    if (fd == -1) {
        uvErrMsgSys(errmsg, mkstemp, errno);
        goto err;
    }
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        uvErrMsgSys(errmsg, posix_fallocate, rv);
        goto err_after_file_open;
    }
    unlink(path);

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
    return UV__ERROR;
}

int uvSetDirectIo(int fd, char *errmsg)
{
    int flags; /* Current fcntl flags */
    int rv;
    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);
    if (rv == -1) {
        uvErrMsgSys(errmsg, fnctl, errno);
        return UV__ERROR;
    }
    return 0;
}

int uvIoSetup(unsigned nr, aio_context_t *ctxp, char *errmsg)
{
    int rv;
    rv = syscall(__NR_io_setup, nr, ctxp);
    if (rv == -1) {
        uvErrMsgSys(errmsg, io_setup, errno);
        return UV__ERROR;
    }
    return 0;
}

int uvIoDestroy(aio_context_t ctx, char *errmsg)
{
    int rv;
    rv = syscall(__NR_io_destroy, ctx);
    if (rv == -1) {
        uvErrMsgSys(errmsg, io_destroy, errno);
        return UV__ERROR;
    }
    return 0;
}

void uvTryIoDestroy(aio_context_t ctx)
{
    uvErrMsg errmsg;
    uvIoDestroy(ctx, errmsg);
}

int uvIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp, char *errmsg)
{
    int rv;
    rv = syscall(__NR_io_submit, ctx, nr, iocbpp);
    if (rv == -1) {
        uvErrMsgSys(errmsg, io_submit, errno);
        switch (errno) {
            case EOPNOTSUPP:
                return UV__NOTSUPP;
            case EAGAIN:
                return UV__AGAIN;
            default:
                return UV__ERROR;
        }
    }
    assert(rv == nr); /* TODO: can something else be returned? */
    return 0;
}

int uvIoGetevents(aio_context_t ctx,
                  long min_nr,
                  long max_nr,
                  struct io_event *events,
                  struct timespec *timeout,
                  int *nr,
                  char *errmsg)
{
    int rv;
    do {
        rv = syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        uvErrMsgSys(errmsg, io_getevents, errno);
        return UV__ERROR;
    }
    assert(rv >= min_nr);
    assert(rv <= max_nr);
    *nr = rv;
    return 0;
}
