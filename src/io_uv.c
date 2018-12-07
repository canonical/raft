#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

#include "binary.h"
#include "error.h"
#include "io_uv_fs.h"
#include "logger.h"

/**
 * Current on-disk format.
 */
#define RAFT_IO_UV__FORMAT 1

/**
 * Format string for open segment filenames.
 *
 * First param: incrementing counter.
 */
#define RAFT_IO_UV__OPEN_SEGMENT_FORMAT "open-%lu"

/**
 * Format string for closed segment filenames.
 *
 * First param: start index, inclusive.
 * Second param: end index, inclusive.
 */
#define RAFT_IO_UV__CLOSED_SEGMENT_FORMAT "%020lu-%020lu"

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    char *dir;
    struct uv_loop_s *loop;
    struct uv_timer_s ticker;
    uint64_t last_tick;

    unsigned short next_metadata_n;       /* Next metadata file to write */
    unsigned short next_metadata_version; /* Next metadata version */

    raft_index first_index; /* Cache of persisted log first index */
    raft_term term;         /* Cache of persisted term */

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
    unsigned long long version;
    raft_term term;
    unsigned voted_for;
    raft_index first_index;
};

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
    struct raft_io_uv *uv;

    uv = io->data;

    raft_free(uv->dir);
    raft_free(uv);
}

/**
 * Build the full path for the given filename, including the data directory.
 */
void raft_io_uv__path(struct raft_io_uv *io, const char *filename, char *path)
{
    assert(filename != NULL);
    assert(strlen(filename) < RAFT_IO_UV_FS_MAX_FILENAME_LEN);

    strcpy(path, io->dir);
    strcat(path, "/");
    strcat(path, filename);
}

/**
 * Read the @n'th metadata file (with @n equal to 1 or 2) and decode the content
 * of the file, populating the given metadata buffer accordingly.
 */
static int raft_io_uv__read_metadata(struct raft_io_uv *uv,
                                     unsigned short n,
                                     struct raft_io_uv__metadata *metadata)
{
    char path[RAFT_IO_UV_FS_MAX_PATH_LEN];  /* Full path of metadata file */
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE];  /* Content of metadata file */
    void *cursor = buf;
    unsigned format;
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    sprintf(filename, "metadata%d", n);

    raft_io_uv__path(uv, filename, path);

    fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno != ENOENT) {
            raft_errorf(uv->errmsg, "open %s: %s", filename, strerror(errno));
            return RAFT_ERR_IO;
        }

        /* The file does not exist, just return. */
        metadata->version = 0;

        return 0;
    }

    rv = read(fd, buf, sizeof buf);
    if (rv == -1) {
        raft_errorf(uv->errmsg, "read %s: %s", filename, strerror(errno));
        close(fd);
        return RAFT_ERR_IO;
    }
    if (rv != sizeof buf) {
        /* Assume that the server crashed while writing this metadata file, and
         * pretend it has not been written at all. */
        metadata->version = 0;
        close(fd);
        return 0;
    };

    close(fd);

    cursor = buf;

    format = raft__get64(&cursor);

    if (format != RAFT_IO_UV__FORMAT) {
        raft_errorf(uv->errmsg, "parse %s: unknown format %d", filename,
                    format);
        return RAFT_ERR_IO;
    }

    metadata->version = raft__get64(&cursor);
    metadata->term = raft__get64(&cursor);
    metadata->voted_for = raft__get64(&cursor);
    metadata->first_index = raft__get64(&cursor);

    /* Sanity checks that values make sense */
    if (metadata->version == 0) {
        raft_errorf(uv->errmsg, "parse %s: version is set to zero", filename);
        return RAFT_ERR_IO;
    }

    if (metadata->term == 0) {
        raft_errorf(uv->errmsg, "parse %s: term is set to zero", filename);
        return RAFT_ERR_IO;
    }

    return 0;
}

static int raft_io_uv__read_state(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    struct raft_io_uv__metadata metadata1;
    struct raft_io_uv__metadata metadata2;
    struct raft_io_uv__metadata *metadata;
    int rv;

    rv = raft_io_uv__read_metadata(uv, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }

    rv = raft_io_uv__read_metadata(uv, 2, &metadata2);
    if (rv != 0) {
        return rv;
    }

    /* If neither metadata file exists, set everything to null and return. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        request->result.read_state.term = 0;
        request->result.read_state.voted_for = 0;
        request->result.read_state.first_index = 0;
        request->result.read_state.entries = NULL;
        request->result.read_state.n = 0;

        return 0;
    }

    /* It should never happen that the two metadata files have the same
     * version. */
    if (metadata1.version == metadata2.version) {
        raft_errorf(uv->errmsg,
                    "metadata1 and metadata2 are both at version %d",
                    metadata1.version);
        return RAFT_ERR_IO;
    }

    /* Pick the metadata with the grater version. */
    if (metadata1.version > metadata2.version) {
        metadata = &metadata1;

        uv->next_metadata_n = 2;
        uv->next_metadata_version = metadata1.version + 1;
    } else {
        metadata = &metadata2;

        uv->next_metadata_n = 1;
        uv->next_metadata_version = metadata2.version + 1;
    }

    request->result.read_state.term = metadata->term;
    request->result.read_state.voted_for = metadata->voted_for;
    request->result.read_state.first_index = metadata->first_index;
    request->result.read_state.entries = NULL;
    request->result.read_state.n = 0;

    /* Update our cache as well */
    uv->first_index = metadata->first_index;
    uv->term = metadata->term;

    return 0;
}

/**
 * Write the @n'th metadata file (with @n equal to 1 or 2), encoding the given
 * @metadata.
 */
static int raft_io_uv__write_metadata(struct raft_io_uv *uv,
                                      unsigned short n,
                                      struct raft_io_uv__metadata *metadata)
{
    char path[RAFT_IO_UV_FS_MAX_PATH_LEN];  /* Full path of metadata file */
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE];  /* Content of metadata file */
    void *cursor = buf;
    int fd;
    int rv;

    assert(n == 1 || n == 2);

    sprintf(filename, "metadata%d", n);

    raft__put64(&cursor, RAFT_IO_UV__FORMAT);
    raft__put64(&cursor, metadata->version);
    raft__put64(&cursor, metadata->term);
    raft__put64(&cursor, metadata->voted_for);
    raft__put64(&cursor, metadata->first_index);

    raft_io_uv__path(uv, filename, path);

    fd = open(path, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(uv->errmsg, "open %s: %s", filename, strerror(errno));
        return RAFT_ERR_IO;
    }

    rv = write(fd, buf, sizeof buf);

    close(fd);

    if (rv == -1) {
        /* TODO: handle EINTR? */
        raft_errorf(uv->errmsg, "write %s: %s", filename, strerror(errno));
        return RAFT_ERR_IO;
    }

    if (rv != sizeof buf) {
        /* TODO: when can this happen? */
        raft_errorf(uv->errmsg, "write %s: only %d bytes written", filename,
                    rv);
        return RAFT_ERR_IO;
    };

    return 0;
}

static int raft_io_uv__write_term(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    struct raft_io_uv__metadata metadata;
    unsigned short n;
    int rv;

    assert(uv->next_metadata_n == 1 || uv->next_metadata_n == 2);
    assert(uv->next_metadata_version > 0);

    n = uv->next_metadata_n;
    metadata.version = uv->next_metadata_version;
    metadata.term = request->args.write_term.term;
    metadata.voted_for = 0;
    metadata.first_index = uv->first_index;

    rv = raft_io_uv__write_metadata(uv, n, &metadata);
    if (rv != 0) {
        return rv;
    }

    uv->next_metadata_n = n == 1 ? 2 : 1;
    uv->next_metadata_version = metadata.version + 1;
    uv->term = metadata.term;

    return 0;
}

static int raft_io_uv__write_vote(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    struct raft_io_uv__metadata metadata;
    unsigned short n;
    int rv;

    assert(uv->next_metadata_n == 1 || uv->next_metadata_n == 2);
    assert(uv->next_metadata_version > 0);

    n = uv->next_metadata_n;
    metadata.version = uv->next_metadata_version;
    metadata.term = uv->term;
    metadata.voted_for = request->args.write_vote.server_id;
    metadata.first_index = uv->first_index;

    rv = raft_io_uv__write_metadata(uv, n, &metadata);
    if (rv != 0) {
        return rv;
    }

    uv->next_metadata_n = n == 1 ? 2 : 1;
    uv->next_metadata_version = metadata.version + 1;
    uv->term = metadata.term;

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

    /* Get the request object */
    request = raft_io_queue_get(uv->queue, request_id);

    assert(request != NULL);

    /* Dispatch the request */
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

/**
 * Check that the given directory exists, and try to create it if it doesn't.
 */
static int raft_io_uv__ensure_dir(const char *dir, char *errmsg)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT_IO_UV_FS_MAX_DIR_LEN) {
        raft_errorf(errmsg, "data directory exceeds %d characters",
                    RAFT_IO_UV_FS_MAX_DIR_LEN);
        return RAFT_ERR_IO;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv != 0) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_errorf(errmsg, "create data directory '%s': %s", dir,
                            strerror(errno));
                return RAFT_ERR_IO;
            }
        } else {
            raft_errorf(errmsg, "access data directory '%s': %s", dir,
                        strerror(errno));
            return RAFT_ERR_IO;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        raft_errorf(errmsg, "path '%s' is not a directory", dir);
        return RAFT_ERR_IO;
    }

    return 0;
}

int raft_io_uv_init(struct raft_io *io, struct uv_loop_s *loop, const char *dir)
{
    struct raft_io_uv *uv;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    rv = raft_io_uv__ensure_dir(dir, io->errmsg);
    if (rv != 0) {
        return rv;
    }

    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        raft_errorf(io->errmsg, "can't allocate I/O implementation instance");
        return RAFT_ERR_NOMEM;
    }

    uv->dir = raft_malloc(strlen(dir) + 1);
    if (uv->dir == NULL) {
        raft_free(uv);
        raft_errorf(io->errmsg, "can't copy data directory path");
        return RAFT_ERR_NOMEM;
    }
    strcpy(uv->dir, dir);

    /* Strip any trailing slash */
    if (uv->dir[strlen(uv->dir) - 1] == '/') {
        uv->dir[strlen(uv->dir) - 1] = 0;
    }

    uv->loop = loop;
    uv->next_metadata_n = 1;
    uv->next_metadata_version = 1;
    uv->first_index = 0;
    uv->term = 0;
    uv->errmsg = io->errmsg;

    io->data = uv;
    io->init = raft_io_uv__init;
    io->start = raft_io_uv__start;
    io->stop = raft_io_uv__stop;
    io->close = raft_io_uv__close;
    io->submit = raft_io_uv__submit;

    return 0;
}
