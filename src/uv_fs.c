#include "uv_fs.h"

#include <string.h>

#include "assert.h"
#include "err.h"
#include "heap.h"
#include "uv_error.h"

const char *UvFsErrMsg(struct UvFs *fs)
{
    return ErrMsgString(&fs->errmsg);
}

static int uvFsSyncDir(struct UvFs *fs, const char *dir)
{
    uv_file fd;
    int rv;
    fd = UvOsOpen(dir, UV_FS_O_RDONLY | UV_FS_O_DIRECTORY, 0);
    if (fd < 0) {
        UvErrMsgSys(&fs->errmsg, "open directory", fd);
        return UV__ERROR;
    }
    rv = UvOsFsync(fd);
    UvOsClose(fd);
    if (rv != 0) {
        UvErrMsgSys(&fs->errmsg, "fsync directory", fd);
        return UV__ERROR;
    }
    return 0;
}

int UvFsCreateFile(struct UvFs *fs,
                   const char *dir,
                   const char *filename,
                   size_t size,
                   uv_file *fd)
{
    char path[UV__PATH_SZ];
    int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
    int rv = 0;

    UvOsJoin(dir, filename, path);

    rv = UvOsOpen(path, flags, S_IRUSR | S_IWUSR);
    if (rv < 0) {
        UvErrMsgSys(&fs->errmsg, "open", rv);
        rv = UV__ERROR;
        goto err;
    }

    *fd = rv;

    /* Allocate the desired size. */
    rv = UvOsFallocate(*fd, 0, size);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        UvErrMsgSys(&fs->errmsg, "posix_fallocate", rv);
        rv = UV__ERROR;
        goto err_after_open;
    }

    rv = uvFsSyncDir(fs, dir);
    if (rv != 0) {
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

int UvFsRemoveFile(struct UvFs *fs, const char *dir, const char *filename)
{
    char path[UV__PATH_SZ];
    int rv;
    UvOsJoin(dir, filename, path);
    rv = UvOsUnlink(path);
    if (rv != 0) {
        UvErrMsgSys(&fs->errmsg, "unlink", rv);
        return UV__ERROR;
    }
    rv = uvFsSyncDir(fs, dir);
    if (rv != 0) {
        return 0;
    }
    return 0;
}

int UvFsTruncateAndRenameFile(struct UvFs *fs,
                              const char *dir,
                              size_t size,
                              const char *filename1,
                              const char *filename2)
{
    char path1[UV__PATH_SZ];
    char path2[UV__PATH_SZ];
    uv_file fd;
    int rv;

    UvOsJoin(dir, filename1, path1);
    UvOsJoin(dir, filename2, path2);

    /* If the desired size is zero, then let's just remove the original file. */
    if (size == 0) {
        rv = UvOsUnlink(path1);
        if (rv != 0) {
            UvErrMsgSys(&fs->errmsg, "unlink", rv);
            goto err;
        }
        goto sync;
    }

    /* Truncate and rename. */
    rv = UvOsOpen(path1, UV_FS_O_RDWR, 0);
    if (rv < 0) {
        UvErrMsgSys(&fs->errmsg, "open", rv);
        goto err;
    }
    fd = rv;
    rv = UvOsTruncate(fd, size);
    if (rv != 0) {
        UvErrMsgSys(&fs->errmsg, "truncate", rv);
        goto err_after_open;
    }
    rv = UvOsFsync(fd);
    if (rv != 0) {
        UvErrMsgSys(&fs->errmsg, "fsync", rv);
        goto err_after_open;
    }
    UvOsClose(fd);

    rv = UvOsRename(path1, path2);
    if (rv != 0) {
        UvErrMsgSys(&fs->errmsg, "rename", rv);
        goto err;
    }

sync:
    rv = uvFsSyncDir(fs, dir);
    if (rv != 0) {
        return rv;
    }
    return 0;

err_after_open:
    UvOsClose(fd);
err:
    return UV__ERROR;
}
