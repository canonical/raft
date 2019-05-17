#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <unistd.h>

#include "aio.h"
#include "assert.h"
#include "os.h"

void osJoin(const osDir dir, const osFilename filename, osPath path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

void osDirname(const osPath path, osDir dir)
{
    strncpy(dir, path, OS_MAX_DIR_LEN);
    dirname(dir);
}

int osEnsureDir(const osDir dir)
{
    struct stat sb;
    int rv;

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                return errno;
            }
        } else {
            return errno;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        return ENOTDIR;
    }

    return 0;
}

int osOpen(const osDir dir, const osFilename filename, int flags, int *fd)
{
    osPath path;
    osJoin(dir, filename, path);
    *fd = open(path, flags, S_IRUSR | S_IWUSR);
    if (*fd == -1) {
        return errno;
    }
    return 0;
}

int osStat(const osDir dir, const osFilename filename, struct stat *sb)
{
    osPath path;
    int rv;
    osJoin(dir, filename, path);
    rv = stat(path, sb);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osUnlink(const char *dir, const char *filename)
{
    osPath path;
    int rv;
    osJoin(dir, filename, path);
    rv = unlink(path);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osTruncate(const osDir dir, const osFilename filename, size_t offset)
{
    osPath path;
    int fd;
    int rv;
    osJoin(dir, filename, path);
    fd = open(path, O_RDWR);
    if (fd == -1) {
        return errno;
    }
    rv = ftruncate(fd, offset);
    if (rv == -1) {
        close(fd);
        return errno;
    }
    rv = fsync(fd);
    if (rv == -1) {
        close(fd);
        return errno;
    }
    close(fd);
    return 0;
}

int osRename(const osDir dir,
             const osFilename filename1,
             const osFilename filename2)
{
    struct stat sb;
    osPath path1;
    osPath path2;
    int rv;
    osJoin(dir, filename1, path1);
    osJoin(dir, filename2, path2);
    /* Check that filename2 does not exist. */
    rv = stat(path2, &sb);
    if (rv == 0) {
        return EEXIST;
    } else if (errno != ENOENT) {
        return errno;
    }
    rv = rename(path1, path2);
    if (rv == -1) {
        return errno;
    }
    rv = osSyncDir(dir);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

int osSyncDir(const osDir dir)
{
    int fd;
    int rv;
    fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        return errno;
    }
    rv = fsync(fd);
    close(fd);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

int osScanDir(const osDir dir, struct dirent ***entries, int *n_entries)
{
    int rv;
    rv = scandir(dir, entries, NULL, alphasort);
    if (rv == -1) {
        return errno;
    }
    *n_entries = rv;
    return 0;
}

int osIsEmpty(const osDir dir, const osFilename filename, bool *empty)
{
    osPath path;
    struct stat sb;
    int rv;
    osJoin(dir, filename, path);
    rv = stat(path, &sb);
    if (rv == -1) {
        return errno;
    }
    *empty = sb.st_size == 0 ? true : false;
    return 0;
}

int osHasTrailingZeros(const int fd, bool *flag)
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
        return errno;
    }
    size -= offset;

    /* Reposition the file descriptor offset to the original offset. */
    offset = lseek(fd, offset, SEEK_SET);
    if (offset == -1) {
        return errno;
    }

    data = malloc(size);
    if (data == NULL) {
        return ENOMEM;
    }

    rv = osReadN(fd, data, size);
    if (rv != 0) {
        free(data);
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
    free(data);
    return 0;
}

int osReadN(const int fd, void *buf, const size_t n)
{
    int rv;
    rv = read(fd, buf, n);
    if (rv == -1) {
        return errno;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        return ENODATA;
    }
    return 0;
}

int osWriteN(const int fd, void *buf, const size_t n)
{
    int rv;
    rv = write(fd, buf, n);
    if (rv == -1) {
        return errno;
    }
    assert(rv >= 0);
    if ((size_t)rv < n) {
        return ENODATA;
    }
    return 0;
}

bool osIsAtEof(const int fd)
{
    off_t offset;
    off_t size;
    offset = lseek(fd, 0, SEEK_CUR); /* Get the current offset */
    size = lseek(fd, 0, SEEK_END);   /* Get file size */
    lseek(fd, offset, SEEK_SET);     /* Restore current offset */
    return offset == size;           /* Compare current offset and size */
}

int osCreateFile(const osDir dir,
                 const osFilename filename,
                 struct raft_buffer *bufs,
                 unsigned n_bufs)
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
    rv = osOpen(dir, filename, flags, &fd);
    if (rv != 0) {
        goto err;
    }
    rv = writev(fd, (const struct iovec *)bufs, n_bufs);
    if (rv != (int)(size)) {
        rv = errno;
        goto err_after_file_open;
    }
    rv = fsync(fd);
    if (rv == -1) {
        goto err_after_file_open;
    }
    rv = close(fd);
    if (rv == -1) {
        rv = errno;
        goto err;
    }
    return 0;

err_after_file_open:
    close(fd);
err:
    return rv;
}

/* Check if direct I/O is possible on the given fd. Return the appropriate
 * buffer size in that case, or 0 if direct I/O is not supported. */
static int probeDirectIO(int fd, size_t *size)
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
            return errno;
        }
        rv = fstatfs(fd, &fs_info);
        if (rv == -1) {
            /* UNTESTED: in practice ENOMEM should be the only failure mode */
            return errno;
        }
        switch (fs_info.f_type) {
            case 0x01021994: /* TMPFS_MAGIC */
            case 0x2fc12fc1: /* ZFS magic */
                *size = 0;
                return 0;
            default:
                /* UNTESTED: this is an unsupported file system. */
                return EINVAL;
        }
    }

    /* Try to peform direct I/O, using various buffer size. */
    *size = 4096;
    while (*size >= 512) {
        buf = aligned_alloc(*size, *size);
        if (buf == NULL) {
            /* UNTESTED: define a configurable allocator that can fail? */
            return ENOMEM;
        }
        memset(buf, 0, *size);
        rv = write(fd, buf, *size);
        free(buf);
        if (rv > 0) {
            assert(rv == (int)(*size));
            return 0;
        }
        if (rv != EIO && rv != EOPNOTSUPP) {
            /* UNTESTED: this should basically fail only because of disk errors,
             * since we allocated the file with posix_fallocate. */
            return errno;
        }
        *size = *size / 2;
    }

    *size = 0;
    return 0;
}

#if defined(RWF_NOWAIT)
/* Check if async I/O is possible on the given fd. */
static int probeAsyncIO(int fd, size_t size, bool *ok)
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
        return errno;
    }

    /* Allocate the write buffer */
    buf = aligned_alloc(size, size);
    if (buf == NULL) {
        /* UNTESTED: define a configurable allocator that can fail? */
        return ENOMEM;
    }
    memset(buf, 0, size);

    /* Prepare the KAIO request object */
    memset(&iocb, 0, sizeof iocb);
    iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
    *((void**)(&iocb.aio_buf)) = buf;
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
        return errno;
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

int osProbeIO(const osDir dir, size_t *direct, bool *async)
{
    osFilename filename; /* Filename of the probe file */
    osPath path;         /* Full path of the probe file */
    int fd;              /* File descriptor of the probe file */
    int rv;

    /* Create a temporary probe file. */
    strcpy(filename, ".probe-XXXXXX");
    osJoin(dir, filename, path);
    fd = mkstemp(path);
    if (fd == -1) {
        rv = errno;
        goto err;
    }
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        goto err_after_file_open;
    }
    unlink(path);

    /* Check if we can use direct I/O. */
    rv = probeDirectIO(fd, direct);
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
    rv = probeAsyncIO(fd, *direct, async);
    if (rv != 0) {
        goto err_after_file_open;
    }
#endif /* RWF_NOWAIT */

out:
    close(fd);
    return 0;

err_after_file_open:
    close(fd);
err:
    assert(rv != 0);
    return rv;
}

int osSetDirectIO(int fd)
{
    int flags; /* Current fcntl flags */
    int rv;
    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);
    if (rv == -1) {
        return errno;
    }
    return 0;
}

char strErrorBuf[2048];

const char *osStrError(int rv)
{
    return strerror_r(rv, strErrorBuf, sizeof strErrorBuf);
}
