#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

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
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    struct raft *raft;
    const char *dir;
    struct uv_loop_s *loop;
    struct uv_timer_s ticker;
    uint64_t last_tick;
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

/**
 * Periodic tick timer callback.
 */
static void raft_io_uv__ticker_cb(uv_timer_t *ticker)
{
    struct raft_io_uv *io;
    uint64_t now;

    io = ticker->data;

    now = uv_now(io->loop);

    raft_tick(io->raft, now - io->last_tick);

    io->last_tick = now;
}

/**
 * Start the backend.
 */
static int raft_io_uv__start(struct raft *r, unsigned tick)
{
    struct raft_io_uv *io;
    int rv;

    assert(r->state == RAFT_STATE_UNAVAILABLE);

    io = r->io_.data;

    io->last_tick = uv_now(io->loop);

    rv = uv_timer_init(io->loop, &io->ticker);
    if (rv != 0) {
        raft_error__uv(r, rv, "init tick timer");
        return RAFT_ERR_INTERNAL;
    }
    io->ticker.data = io;

    rv = uv_timer_start(&io->ticker, raft_io_uv__ticker_cb, 0, tick);
    if (rv != 0) {
        raft_error__uv(r, rv, "start tick timer");
        return RAFT_ERR_INTERNAL;
    }

    return 0;
}

static int raft_io_uv__stop(struct raft *r)
{
    struct raft_io_uv *io;
    int rv;

    io = r->io_.data;

    rv = uv_timer_stop(&io->ticker);
    if (rv != 0) {
        raft_error__uv(r, rv, "stop tick timer");
        return RAFT_ERR_INTERNAL;
    }

    uv_close((uv_handle_t *)&io->ticker, NULL);

    return 0;
}

static void raft_io_uv__close(struct raft *r)
{
    raft_free(r->io_.data);
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

static int raft_io_uv__read_metadata(struct raft_io_uv *io,
                                     unsigned short n,
                                     struct raft_io_uv__metadata *metadata)
{
    char path[RAFT_IO_UV__MAX_PATH_LEN];
    char filename[strlen("metadataN") + 1];
    uint8_t buffer[16];
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    sprintf(filename, "metadata%d", n);

    raft_io_uv__path(io, filename, path);

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

    return 0;
}

static int raft_io_uv__read_state(struct raft_io_uv *io,
                                  struct raft_io_request *request)
{
    char path1[RAFT_IO_UV__MAX_PATH_LEN];
    char path2[RAFT_IO_UV__MAX_PATH_LEN];

    raft_io_uv__path(io, "metadata1", path1);
    raft_io_uv__path(io, "metadata2", path2);

    return 0;
}

static int raft_io_uv__write_term(struct raft_io_uv *io,
                                  struct raft_io_request *request)
{
    return 0;
}

static int raft_io_uv__write_vote(struct raft_io_uv *io,
                                  struct raft_io_request *request)
{
    return 0;
}

static int raft_io_uv__write_log(struct raft_io_uv *io,
                                 const unsigned request_id)
{
    return 0;
}

static int raft_io_uv__submit(struct raft *r, const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_uv *io;
    int rv;

    assert(r != NULL);

    io = r->io_.data;

    request = raft_io_queue_get(r, request_id);

    assert(request != NULL);

    switch (request->type) {
        case RAFT_IO_READ_STATE:
            rv = raft_io_uv__read_state(io, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_uv__write_term(io, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_uv__write_vote(io, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_uv__write_log(io, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_uv_init(struct raft *r, struct uv_loop_s *loop, const char *dir)
{
    struct raft_io_uv *io;
    struct stat sb;
    int rv;

    assert(r != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) >=
        RAFT_IO_UV__MAX_PATH_LEN - RAFT_IO_UV__MAX_FILENAME_LEN) {
        rv = RAFT_ERR_PATH_TOO_LONG;
        raft_error__printf(r, rv, NULL);
        return rv;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_error__os(r, "create data directory");
                return RAFT_ERR_IO;
            }
        } else {
            raft_error__os(r, "check data directory");
            return RAFT_ERR_IO;
        }
    }
    if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        return RAFT_ERR_IO;
    }

    io = raft_malloc(sizeof *io);
    if (io == NULL) {
        return RAFT_ERR_NOMEM;
    }

    io->raft = r;
    io->loop = loop;

    r->io_.data = io;
    r->io_.start = raft_io_uv__start;
    r->io_.stop = raft_io_uv__stop;
    r->io_.close = raft_io_uv__close;
    r->io_.submit = raft_io_uv__submit;

    return 0;
}
