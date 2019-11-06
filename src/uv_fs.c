#include "uv_fs.h"

#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "err.h"
#include "heap.h"
#include "uv_error.h"

/* Default permissions when creating a directory. */
#define DEFAULT_DIR_PERM 0700

int UvFsEnsureDir(const char *dir, struct ErrMsg *errmsg)
{
    struct uv_fs_s req;
    int rv;

    /* Make sure we have a directory we can write into. */
    rv = uv_fs_stat(NULL, &req, dir, NULL);
    if (rv != 0) {
        if (rv == UV_ENOENT) {
            rv = uv_fs_mkdir(NULL, &req, dir, DEFAULT_DIR_PERM, NULL);
            if (rv != 0) {
                UvErrMsgSys(errmsg, "mkdir", rv);
                return UV__ERROR;
            }
        } else {
            UvErrMsgSys(errmsg, "stat", rv);
            return UV__ERROR;
        }
    } else if ((req.statbuf.st_mode & S_IFMT) != S_IFDIR) {
        ErrMsgPrintf(errmsg, "%s", uv_strerror(UV_ENOTDIR));
        return UV__ERROR;
    }

    return 0;
}

int UvFsSyncDir(const char *dir, struct ErrMsg *errmsg)
{
    uv_file fd;
    int rv;
    fd = UvOsOpen(dir, UV_FS_O_RDONLY | UV_FS_O_DIRECTORY, 0);
    if (fd < 0) {
        UvErrMsgSys(errmsg, "open directory", fd);
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
    rv = UvOsOpen(path, flags, mode);
    if (rv < 0) {
        UvErrMsgSys(errmsg, "open", rv);
        return UV__ERROR;
    }
    *fd = rv;
    return 0;
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
    rv = UvOsOpen(path1, UV_FS_O_RDWR, 0);
    if (rv < 0) {
        UvErrMsgSys(errmsg, "open", rv);
        goto err;
    }
    fd = rv;
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
