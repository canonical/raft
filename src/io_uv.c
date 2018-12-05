#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

#include "binary.h"
#include "error.h"
#include "logger.h"

/**
 * Maximum length of a file system path.
 */
#define RAFT_IO_UV__MAX_PATH_LEN 1024

/**
 * Maximum length of a file name.
 */
#define RAFT_IO_UV__MAX_FILENAME_LEN 128

/**
 * Maximum length of the data directory path.
 */
#define RAFT_IO_UV__MAX_DIR_LEN \
    (RAFT_IO_UV__MAX_PATH_LEN - RAFT_IO_UV__MAX_FILENAME_LEN - 1)

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    const char *dir;
    struct uv_loop_s *loop;
    struct uv_timer_s ticker;
    uint64_t last_tick;

    /* Parameters passed via raft_io->init */
    struct raft_io_queue *queue;
    void *p;
    void (*tick)(void *, const unsigned);
    void (*notify)(void *, const unsigned, const int);

    /* Error message buffer */
    char *errmsg;
};

struct raft_io_uv__metadata
{
    raft_term term;
    unsigned voted_for;
};

/**
 * Convenience for wrapping an error message from libuv.
 */
static void raft_error__uv(struct raft *r, const int rv, const char *fmt, ...)
{
    const char *msg = uv_strerror(rv);
    va_list args;

    strncpy(r->errmsg, msg, strlen(msg));

    if (fmt == NULL) {
        return;
    }

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}

/**
 * Convenience for wrapping an error message from the OS.
 */
static void raft_error__os(struct raft *r, const char *fmt, ...)
{
    const char *msg = strerror(errno);
    va_list args;

    strncpy(r->errmsg, msg, strlen(msg));

    if (fmt == NULL) {
        return;
    }

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}

static void raft_io_uv__init(struct raft_io *io,
                             struct raft_io_queue *queue,
                             void *p,
                             void (*tick)(void *, const unsigned),
                             void (*notify)(void *, const unsigned, const int))
{
    struct raft_io_uv *uv;

    uv = io->data;

    uv->queue = queue;
    uv->p = p;
    uv->tick = tick;
    uv->notify = notify;
}

/**
 * Periodic tick timer callback.
 */
static void raft_io_uv__ticker_cb(uv_timer_t *ticker)
{
    struct raft_io_uv *uv;
    uint64_t now;

    uv = ticker->data;

    now = uv_now(uv->loop);

    uv->tick(uv->p, now - uv->last_tick);

    uv->last_tick = now;
}

/**
 * Start the backend.
 */
static int raft_io_uv__start(struct raft_io *io, const unsigned msecs)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    uv->last_tick = uv_now(uv->loop);

    rv = uv_timer_init(uv->loop, &uv->ticker);
    if (rv != 0) {
        // raft_error__uv(r, rv, "init tick timer");
        return RAFT_ERR_INTERNAL;
    }
    uv->ticker.data = io;

    rv = uv_timer_start(&uv->ticker, raft_io_uv__ticker_cb, 0, msecs);
    if (rv != 0) {
        // raft_error__uv(r, rv, "start tick timer");
        return RAFT_ERR_INTERNAL;
    }

    return 0;
}

static int raft_io_uv__stop(struct raft_io *io)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    rv = uv_timer_stop(&uv->ticker);
    if (rv != 0) {
        // raft_error__uv(r, rv, "stop tick timer");
        return RAFT_ERR_INTERNAL;
    }

    uv_close((uv_handle_t *)&uv->ticker, NULL);

    return 0;
}

static void raft_io_uv__close(struct raft_io *io)
{
    raft_free(io->data);
}

/**
 * Build the full path for the given filename, including the data directory.
 */
void raft_io_uv__path(struct raft_io_uv *io, const char *filename, char *path)
{
    assert(filename != NULL);
    assert(strlen(filename) < RAFT_IO_UV__MAX_FILENAME_LEN);

    strcpy(path, io->dir);
    strcat(path, "/");
    strcat(path, filename);
}

static int raft_io_uv__read_metadata(struct raft_io_uv *uv,
                                     unsigned short n,
                                     struct raft_io_uv__metadata *metadata)
{
    char path[RAFT_IO_UV__MAX_PATH_LEN];
    char filename[strlen("metadataN") + 1];
    uint8_t buffer[16];
    void *cursor;
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    sprintf(filename, "metadata%d", n);

    raft_io_uv__path(uv, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno != ENOENT) {
            return RAFT_ERR_IO;
        }
        metadata->term = 0;
        metadata->voted_for = 0;
    }

    rv = read(fd, buffer, sizeof buffer);
    if (rv == -1) {
        close(fd);
        return RAFT_ERR_IO;
    }
    if (rv != sizeof buffer) {
        close(fd);
        return RAFT_ERR_IO;
    };

    close(fd);

    cursor = buffer;

    metadata->term = raft__get64(&cursor);
    metadata->voted_for = raft__get64(&cursor);

    return 0;
}

static int raft_io_uv__read_state(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    struct raft_io_uv__metadata metadata;
    int rv;

    rv = raft_io_uv__read_metadata(uv, 1, &metadata);
    if (rv != 0) {
        return rv;
    }

    request->result.read_state.term = metadata.term;
    request->result.read_state.voted_for = metadata.voted_for;

    return 0;
}

static int raft_io_uv__write_term(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    return 0;
}

static int raft_io_uv__write_vote(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    return 0;
}

static int raft_io_uv__write_log(struct raft_io_uv *uv,
                                 const unsigned request_id)
{
    return 0;
}

static int raft_io_uv__submit(struct raft_io *io, const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_uv *uv;
    int rv;

    assert(io != NULL);

    uv = io->data;

    request = raft_io_queue_get(uv->queue, request_id);

    assert(request != NULL);

    switch (request->type) {
        case RAFT_IO_READ_STATE:
            rv = raft_io_uv__read_state(uv, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_uv__write_term(uv, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_uv__write_vote(uv, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_uv__write_log(uv, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_uv_init(struct raft_io *io, struct uv_loop_s *loop, const char *dir)
{
    struct raft_io_uv *uv;
    struct stat sb;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT_IO_UV__MAX_DIR_LEN) {
        raft_errorf(io->errmsg, "dir exceeds %d characters",
                    RAFT_IO_UV__MAX_DIR_LEN);
        return RAFT_ERR_IO_PATH_TOO_LONG;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                // raft_error__os(r, "create data directory");
                return RAFT_ERR_IO;
            }
        } else {
            // raft_error__os(r, "check data directory");
            return RAFT_ERR_IO;
        }
    }
    if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        return RAFT_ERR_IO;
    }

    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        return RAFT_ERR_NOMEM;
    }

    uv->dir = dir;
    uv->loop = loop;
    uv->errmsg = io->errmsg;

    io->data = uv;
    io->init = raft_io_uv__init;
    io->start = raft_io_uv__start;
    io->stop = raft_io_uv__stop;
    io->close = raft_io_uv__close;
    io->submit = raft_io_uv__submit;

    return 0;
}
