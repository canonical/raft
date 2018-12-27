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
 * Deserialized content of a single metadata file.
 */
struct raft_io_uv__metadata
{
    unsigned long long version;
    raft_term term;
    unsigned voted_for;
    raft_index first_index;
};

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    char *dir;                /* Data directory */
    size_t block_size;        /* File system block size */
    struct uv_loop_s *loop;   /* UV event loop */
    struct uv_timer_s ticker; /* Timer for periodic calls to raft_tick */
    uint64_t last_tick;       /* Timestamp of the last raft_tick call */

    /* Cache of the last metadata file that was written. */
    struct raft_io_uv__metadata metadata;

    /* The file containing the entries for the current open segment. This must
     * always be a valid file. */
    struct raft_io_uv_file open_segment;

    /* Parameters passed via raft_io->init */
    struct raft_io_queue *queue;                       /* Request queue */
    struct raft_logger *logger;                        /* Logger */
    void *p;                                           /* Custom data pointer */
    void (*tick)(void *, const unsigned);              /* Tick function */
    void (*notify)(void *, const unsigned, const int); /* Notify function */

    char *errmsg; /* Error message buffer */
};

/**
 * Initializer.
 */
static void raft_io_uv__init(struct raft_io *io,
                             struct raft_io_queue *queue,
                             struct raft_logger *logger,
                             void *p,
                             void (*tick)(void *, const unsigned),
                             void (*notify)(void *, const unsigned, const int))
{
    struct raft_io_uv *uv;

    uv = io->data;

    uv->queue = queue;
    uv->logger = logger;
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

    /* Get current time */
    now = uv_now(uv->loop);

    /* Invoke the ticker */
    uv->tick(uv->p, now - uv->last_tick);

    /* Update the last tick timestamp */
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

    /* Initialize the tick timer handle. */
    rv = uv_timer_init(uv->loop, &uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "init tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }
    uv->ticker.data = uv;

    /* Start the tick timer handle.. */
    rv = uv_timer_start(&uv->ticker, raft_io_uv__ticker_cb, 0, msecs);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "start tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Stop the backend.
 */
static int raft_io_uv__stop(struct raft_io *io)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    /* Stop the tick timer handle. */
    rv = uv_timer_stop(&uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "stop tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_INTERNAL;
    }

    /* Close the ticker timer handle. */
    uv_close((uv_handle_t *)&uv->ticker, NULL);

    return 0;
}

/**
 * Close the backend, releasing all resources it allocated.
 */
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
void raft_io_uv__path(const struct raft_io_uv *io,
                      const char *filename,
                      char *path)
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
static int raft_io_uv__read_metadata(const struct raft_io_uv *uv,
                                     const unsigned short n,
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

    /* Render the metadata path */
    sprintf(filename, "metadata%d", n);
    raft_io_uv__path(uv, filename, path);

    /* Open the metadata file, if it exists. */
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

    /* Read the content of the metadata file. */
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

    /* Decode the content of the metadata file. */
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

/**
 * Flush our metadata cache to disk, writing the appropriate metadata file
 * according to the current version (if the version is odd, write metadata1,
 * otherwise write metadata2).
 */
static int raft_io_uv__flush_metadata(const struct raft_io_uv *uv)
{
    unsigned short n;
    char path[RAFT_IO_UV_FS_MAX_PATH_LEN];  /* Full path of metadata file */
    char filename[strlen("metadataN") + 1]; /* Pattern of metadata filename */
    uint8_t buf[RAFT_IO_UV_METADATA_SIZE];  /* Content of metadata file */
    void *cursor = buf;
    int fd;
    int rv;

    assert(uv->metadata.version > 0);
    assert(uv->metadata.first_index > 0);

    /* Encode the given metadata. */
    raft__put64(&cursor, RAFT_IO_UV__FORMAT);
    raft__put64(&cursor, uv->metadata.version);
    raft__put64(&cursor, uv->metadata.term);
    raft__put64(&cursor, uv->metadata.voted_for);
    raft__put64(&cursor, uv->metadata.first_index);

    /* Render the metadata file name. */
    n = uv->metadata.version % 2 == 1 ? 1 : 2;
    sprintf(filename, "metadata%d", n);
    raft_io_uv__path(uv, filename, path);

    /* Write the metadata file, creating it if it does not exist. */
    fd = open(path, O_WRONLY | O_CREAT | O_SYNC | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(uv->errmsg, "open %s: %s", filename, strerror(errno));
        return RAFT_ERR_IO;
    }

    do {
        rv = write(fd, buf, sizeof buf);
    } while (rv == -1 && errno == EINTR);

    close(fd);

    if (rv == -1) {
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

/**
 Update both metadata files, so they are created if they didn't exist.
*/
static int raft_io_uv__ensure_metadata(struct raft_io_uv *uv)
{
    int i;
    int fd;
    int rv;

    /* Update both metadata files, so they are created if they didn't
     * exist. Also sync the data directory so the entries get created. */
    for (i = 0; i < 2; i++) {
        uv->metadata.version++;
        rv = raft_io_uv__flush_metadata(uv);
        if (rv != 0) {
            return rv;
        }
    }

    /* Also sync the data directory so the entries get created. */
    fd = open(uv->dir, O_RDONLY | O_DIRECTORY);
    if (fd == -1) {
        raft_errorf(uv->errmsg, "open data directory: %s", strerror(errno));

        return RAFT_ERR_IO;
    }

    rv = fsync(fd);

    close(fd);

    if (rv == -1) {
        raft_errorf(uv->errmsg, "sync data directory: %s", strerror(errno));

        return RAFT_ERR_IO;
    }

    return 0;
}

static int raft_io_uv__read_state(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    struct raft_io_uv__metadata metadata1;
    struct raft_io_uv__metadata metadata2;
    int rv;

    /* The RAFT_IO_READ_STATE request is supposed to be invoked just once, right
     * after backend initialization */
    assert(uv->metadata.version == 0);

    /* Read the two metadata files (if available). */
    rv = raft_io_uv__read_metadata(uv, 1, &metadata1);
    if (rv != 0) {
        return rv;
    }

    rv = raft_io_uv__read_metadata(uv, 2, &metadata2);
    if (rv != 0) {
        return rv;
    }

    /* Check the versions. */
    if (metadata1.version == 0 && metadata2.version == 0) {
        /* Neither metadata file exists: have a brand new server. */
        uv->metadata.first_index = 1;
    } else if (metadata1.version == metadata2.version) {
        /* The two metadata files can't have the same version. */
        raft_errorf(uv->errmsg,
                    "metadata1 and metadata2 are both at version %d",
                    metadata1.version);
        return RAFT_ERR_IO;
    } else {
        /* Pick the metadata with the grater version. */
        if (metadata1.version > metadata2.version) {
            uv->metadata = metadata1;
        } else {
            uv->metadata = metadata2;
        }
    }

    /* Update the metadata files, so they are created if they did not exist. */
    rv = raft_io_uv__ensure_metadata(uv);
    if (rv != 0) {
        return rv;
    }

    /* Fill the result. */
    request->result.read_state.term = uv->metadata.term;
    request->result.read_state.voted_for = uv->metadata.voted_for;
    request->result.read_state.first_index = uv->metadata.first_index;
    request->result.read_state.entries = NULL;
    request->result.read_state.n = 0;

    return 0;
}

static int raft_io_uv__write_term(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    int rv;

    assert(uv->metadata.version > 0);

    uv->metadata.version++;
    uv->metadata.term = request->args.write_term.term;
    uv->metadata.voted_for = 0;

    rv = raft_io_uv__flush_metadata(uv);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft_io_uv__write_vote(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    int rv;

    assert(uv->metadata.version > 0);

    uv->metadata.version++;
    uv->metadata.voted_for = request->args.write_vote.server_id;

    rv = raft_io_uv__flush_metadata(uv);
    if (rv != 0) {
        return rv;
    }

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

    /* Ensure that we have a valid data directory */
    rv = raft_io_uv__ensure_dir(dir, io->errmsg);
    if (rv != 0) {
        return rv;
    }

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        raft_errorf(io->errmsg, "can't allocate I/O implementation instance");
        return RAFT_ERR_NOMEM;
    }

    /* Make a copy of the directory string, stripping any trailing slash */
    uv->dir = raft_malloc(strlen(dir) + 1);
    if (uv->dir == NULL) {
        raft_free(uv);
        raft_errorf(io->errmsg, "can't copy data directory path");
        return RAFT_ERR_NOMEM;
    }
    strcpy(uv->dir, dir);

    if (uv->dir[strlen(uv->dir) - 1] == '/') {
        uv->dir[strlen(uv->dir) - 1] = 0;
    }

    /* Detect the file system block size */
    rv = raft_io_uv_fs__block_size(uv->dir, &uv->block_size);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "detect block size: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    uv->loop = loop;
    uv->last_tick = 0;
    memset(&uv->metadata, 0, sizeof uv->metadata);
    uv->errmsg = io->errmsg;

    io->data = uv;
    io->init = raft_io_uv__init;
    io->start = raft_io_uv__start;
    io->stop = raft_io_uv__stop;
    io->close = raft_io_uv__close;
    io->submit = raft_io_uv__submit;

    return 0;
}
