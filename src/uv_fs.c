#include "uv_fs.h"

#include <string.h>

#include "assert.h"
#include "err.h"
#include "heap.h"
#include "uv_error.h"

/* True if STR's length is at most LEN. */
#define LEN_AT_MOST(STR, LEN) (strnlen(STR, LEN + 1) <= LEN)

/* True if the given DIR string has at most UV__DIR_LEN chars. */
#define DIR_HAS_VALID_LEN(DIR) LEN_AT_MOST(DIR, UV__DIR_LEN)

/* True if the given FILENAME string has at most UV__FILENAME_LEN chars. */
#define FILENAME_HAS_VALID_LEN(FILENAME) LEN_AT_MOST(FILENAME, UV__FILENAME_LEN)

static int uvOsOpen(const char *path, int flags, int mode)
{
    struct uv_fs_s req;
    return uv_fs_open(NULL, &req, path, flags, mode, NULL);
}

static int uvOsClose(uv_file fd)
{
    struct uv_fs_s req;
    return uv_fs_close(NULL, &req, fd, NULL);
}

static int uvOsUnlink(const char *path)
{
    struct uv_fs_s req;
    return uv_fs_unlink(NULL, &req, path, NULL);
}

static void uvOsJoin(const char *dir, const char *filename, char *path)
{
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, filename);
}

void UvFsInit(struct UvFs *fs, struct uv_loop_s *loop)
{
    fs->loop = loop;
    fs->errmsg = NULL;
}

void UvFsClose(struct UvFs *fs)
{
    HeapFree(fs->errmsg);
}

const char *UvFsErrMsg(struct UvFs *fs)
{
    return fs->errmsg;
}

static void uvFsSetErrmsg(struct UvFs *fs, char *errmsg)
{
    HeapFree(fs->errmsg); /* Delete any previous error. */
    fs->errmsg = errmsg;
}

static void uvFsCreateFileWorkCb(uv_work_t *work)
{
    struct UvFsCreateFile *req = work->data;
    int flags = O_WRONLY | O_CREAT | O_EXCL; /* Common open flags */
    char *errmsg = NULL;
    uv_file fd;
    int rv = 0;

    rv = uvOsOpen(req->path, flags, S_IRUSR | S_IWUSR);
    if (rv < 0) {
        errmsg = uvSysErrMsg("open", rv);
        rv = UV__ERROR;
        goto err;
    }

    fd = rv;

    /* Allocate the desired size. */
    rv = posix_fallocate(fd, 0, req->size);
    if (rv != 0) {
        /* From the manual page:
         *
         *   posix_fallocate() returns zero on success, or an error number on
         *   failure.  Note that errno is not set.
         */
        errmsg = errMsgPrintf("posix_fallocate: %s", strerror(rv));
        rv = UV__ERROR;
        goto err_after_open;
    }

    req->errmsg = NULL;
    req->status = 0;
    req->fd = fd;
    return;

err_after_open:
    uvOsClose(fd);
    uvOsUnlink(req->path);
err:
    assert(errmsg != NULL);
    assert(rv != 0);
    req->errmsg = errmsg;
    req->status = rv;
    return;
}

static void uvFsCreateFileAfterWorkCb(uv_work_t *work, int status)
{
    struct UvFsCreateFile *req = work->data;
    assert(status == 0);

    /* If we were canceled, let's mark the request as canceled, regardless of
     * the actual outcome. */
    if (req->canceled) {
        if (req->status == 0) {
            uvOsClose(req->fd);
            uvOsUnlink(req->path);
        } else {
            HeapFree(req->errmsg);
        }
        req->errmsg = errMsgPrintf("canceled");
        req->status = UV__CANCELED;
    }

    /* Transfer possible errors back to the file system object. */
    uvFsSetErrmsg(req->fs, req->errmsg);

    if (req->cb != NULL) {
        req->cb(req, req->status);
    }
}

int UvFsCreateFile(struct UvFs *fs,
                   struct UvFsCreateFile *req,
                   const char *dir,
                   const char *filename,
                   size_t size,
                   UvFsCreateFileCb cb)
{
    int rv;

    assert(DIR_HAS_VALID_LEN(dir));
    assert(FILENAME_HAS_VALID_LEN(filename));

    req->fs = fs;
    req->work.data = req;
    uvOsJoin(dir, filename, req->path);
    req->size = size;
    req->cb = cb;
    req->canceled = false;
    req->errmsg = NULL;

    rv = uv_queue_work(fs->loop, &req->work, uvFsCreateFileWorkCb,
                       uvFsCreateFileAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        fs->errmsg = uvSysErrMsg("uv_queue_work", rv);
        return UV__ERROR;
    }

    return 0;
}

void UvFsCreateFileCancel(struct UvFsCreateFile *req)
{
    req->canceled = true;
}
