#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <xfs/xfs.h>

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

    /* Check that the given path doesn't exceed our static buffer limit */
    assert(strnlen(dir, OS_MAX_DIR_LEN + 1) <= OS_MAX_DIR_LEN);

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
    osPath path1;
    osPath path2;
    int fd;
    int rv;
    osJoin(dir, filename1, path1);
    osJoin(dir, filename2, path2);
    /* TODO: double check that filename2 does not exist. */
    rv = rename(path1, path2);
    if (rv == -1) {
        return errno;
    }
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

#if defined(RWF_NOWAIT)

/* Try to write @fd using a memory-aligned buffer of size @size. If the write is
 * successful, @ok will be set to #true. */
static int probeBlockSize(int fd, size_t size, bool *ok)
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

    /* We expect the write to either succeed or fail with EAGAIN (which means
     * this is not the correct block size). */
    if (event.res > 0) {
        *ok = true;
    } else {
        if (event.res != -EAGAIN) {
            /* UNTESTED: this should basically fail only because of disk errors,
             * since we allocated the file with posix_fallocate. */
            return -event.res;
        }
        *ok = false;
    }

    return 0;
}
#endif

int osBlockSize(const osDir dir, size_t *size)
{
    struct statfs fs_info; /* To get the type code of the underlying fs */
    struct stat info;      /* To get the block size reported by the fs */
    osFilename filename;   /* Filename of the probe file */
    osPath path;           /* Full path of the probe file */
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

    strcpy(filename, ".probe-XXXXXX");
    osJoin(dir, filename, path);

    /* Create a temporary probe file. */
    sprintf(path, "%s/%s", dir, filename);

    fd = mkstemp(path);
    if (fd == -1) {
        rv = errno;
        goto err;
    }

    unlink(path);

    /* For XFS, we can use the dedicated API to figure out the optimal block
     * size */
    if (platform_test_xfs_fd(fd)) {
        struct dioattr attr;

        rv = xfsctl(path, fd, XFS_IOC_DIOINFO, &attr);

        if (rv != 0) {
            /* UNTESTED: since the path and fd are valid, can this ever fail? */
            rv = errno;
            goto err_after_file_open;
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

        goto out;
    }

    /* Get the file system type */
    rv = fstatfs(fd, &fs_info);
    if (rv == -1) {
        /* UNTESTED: in practice ENOMEM should be the only failure mode */
        rv = errno;
        goto err_after_file_open;
    }

    /* Special-case the file systems that do not support O_DIRECT/NOWAIT */
    switch (fs_info.f_type) {
        case 0x01021994: /* tmpfs */
            /* 4096 is ok. */
            *size = 4096;
            goto out;

        case 0x2fc12fc1: /* ZFS */
            /* Let's use whatever stat() returns. */
            rv = fstat(fd, &info);
            if (rv != 0) {
                /* UNTESTED: ENOMEM should be the only failure mode */
                rv = errno;
                goto err_after_file_open;
            }
            /* TODO: The block size indicated in the ZFS case seems way to
             * high. Reducing it to 4096 seems safe since at the moment ZFS does
             * not support async writes. We might want to change this once ZFS
             * async support is released. */
            *size = info.st_blksize > 4096 ? 4096 : info.st_blksize;
            goto out;
    }

    /* For all other file systems, we try to probe the correct size by trial and
     * error. */
    rv = posix_fallocate(fd, 0, 4096);
    if (rv != 0) {
        goto err_after_file_open;
    }

    flags = fcntl(fd, F_GETFL);
    rv = fcntl(fd, F_SETFL, flags | O_DIRECT);
    if (rv == -1) {
        /* UNTESTED: this should actually never fail, for the file systems we
         * currently support. */
        rv = errno;
        goto err_after_file_open;
    }

    *size = 4096;
    while (*size >= 512) {
        bool ok = false;

        rv = probeBlockSize(fd, *size, &ok);
        if (rv != 0) {
            /* UNTESTED: all syscalls performed by underlying code should fail
             * at most with ENOMEM. */
            goto err_after_file_open;
        }

        if (ok) {
            goto out;
        }

        *size = *size / 2;
    }

    /* UNTESTED: at least one of the probed block sizes should work for the file
     * systems we currently support. */
    rv = EINVAL;
    goto err_after_file_open;

out:
    close(fd);
    return 0;

err_after_file_open:
    close(fd);
err:
    assert(rv != 0);
    return rv;
#endif /* RWF_NOWAIT */
}

int osSetDirectIO(int fd)
{
    int flags;             /* Current fcntl flags */
    struct statfs fs_info; /* To check the file system type */
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
                return ENOTSUP;
                break;
            default:
                /* UNTESTED: this is an unsupported file system. */
                return EINVAL;
        }
    }

    return 0;
}

char strErrorBuf[2048];

const char *osStrError(int rv)
{
    return strerror_r(rv, strErrorBuf, sizeof strErrorBuf);
}
