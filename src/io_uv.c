#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

#include "assert.h"
#include "binary.h"
#include "checksum.h"
#include "io_uv_encoding.h"
#include "io_uv_fs.h"
#include "io_uv_loader.h"
#include "io_uv_metadata.h"
#include "io_uv_preparer.h"
#include "io_uv_rpc.h"
#include "io_uv_writer.h"

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    struct raft_io *io;                     /* Instance we are hooked to */
    struct raft_logger *logger;             /* Logger */
    struct uv_loop_s *loop;                 /* UV event loop */
    char *dir;                              /* Data directory */
    struct raft__io_uv_metadata metadata;   /* Cache of metadata on disk */
    struct raft__io_uv_loader loader;       /* Segment loader */
    struct raft__io_uv_preparer preparer;   /* Segment preparer */
    struct raft__io_uv_closer closer;       /* Segment closer */
    struct raft__io_uv_writer writer;       /* Segment writer */
    struct raft_io_uv_transport *transport; /* Network transport */
    struct raft__io_uv_rpc rpc;             /* Implement network RPC */
    struct uv_timer_s ticker;               /* Timer for periodic ticks */
    uint64_t last_tick;                     /* Timestamp of the last tick */

    /* Track the number of active asynchronous operations that need to be
     * completed before this backend can be considered fully stopped. */
    unsigned n_active;

    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;

    void (*stop_cb)(struct raft_io *io);
};

/**
 * Check that the given directory exists, and try to create it if it doesn't.
 */
static int raft__io_uv_ensure_dir(struct raft_logger *logger, const char *dir);

/**
 * Implementation of the #raft_io interface.
 */
static int raft_io_uv__init(struct raft_io *io,
                            unsigned id,
                            const char *address);

static int raft_io_uv__start(struct raft_io *io,
                             unsigned msecs,
                             raft_io_tick_cb tick_cb,
                             raft_io_recv_cb recv_cb);

static int raft_io_uv__close(struct raft_io *io,
                             void (*cb)(struct raft_io *io));

static int raft_io_uv__load(struct raft_io *io,
                            raft_term *term,
                            unsigned *voted_for,
                            struct raft_snapshot **snapshot,
                            struct raft_entry **entries,
                            size_t *n_entries);

static int raft_io_uv__bootstrap(struct raft_io *io,
                                 const struct raft_configuration *conf);

static int raft_io_uv__set_term(struct raft_io *io, const raft_term term);

static int raft_io_uv__set_vote(struct raft_io *io, const unsigned server_id);

static int raft_io_uv__append(struct raft_io *io,
                              const struct raft_entry entries[],
                              unsigned n,
                              void *data,
                              void (*cb)(void *data, int status));

static int raft_io_uv__truncate(struct raft_io *io, raft_index index);

static int raft_io_uv__send(struct raft_io *io,
                            struct raft_io_send *req,
                            const struct raft_message *message,
                            raft_io_send_cb cb);

/**
 * Periodic tick timer callback, for invoking the ticker function.
 */
static void raft_io_uv__ticker_cb(uv_timer_t *ticker);
static void raft_io_uv__ticker_close_cb(uv_handle_t *handle);

static void raft__io_uv_preparer__close_cb(struct raft__io_uv_preparer *p);
static void raft__io_uv_closer__close_cb(struct raft__io_uv_closer *p);
static void raft__io_uv_writer__close_cb(struct raft__io_uv_writer *w);

static void raft_io_uv__rpc_stop_cb(struct raft__io_uv_rpc *rpc);

/**
 * Open and allocate the first closed segment, containing just one entry, and
 * return its file descriptor.
 */
static int raft__io_uv_create_closed_1_1(struct raft_io_uv *uv,
                                         const struct raft_buffer *buf);

/**
 * Write the first block of the first closed segment.
 */
static int raft__io_uv_write_closed_1_1(struct raft_io_uv *uv,
                                        const int fd,
                                        const struct raft_buffer *buf);

/**
 * Invoke the stop callback if we were asked to be stopped and there are no more
 * pending asynchronous activities.
 */
static void raft_io_uv__maybe_stopped(struct raft_io_uv *uv);

int raft_io_uv_init(struct raft_io *io,
                    struct raft_logger *logger,
                    struct uv_loop_s *loop,
                    const char *dir,
                    struct raft_io_uv_transport *transport)
{
    struct raft_io_uv *uv;
    size_t block_size;
    unsigned n_blocks;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    /* Make a copy of the data directory string, stripping any trailing slash */
    uv->dir = raft_malloc(strlen(dir) + 1);
    if (uv->dir == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_uv_alloc;
    }
    strcpy(uv->dir, dir);

    if (uv->dir[strlen(uv->dir) - 1] == '/') {
        uv->dir[strlen(uv->dir) - 1] = 0;
    }

    /* Ensure that the data directory exists and is accessible */
    rv = raft__io_uv_ensure_dir(logger, uv->dir);
    if (rv != 0) {
        goto err_after_dir_alloc;
    }

    /* Load current metadata */
    rv = raft__io_uv_metadata_load(logger, dir, &uv->metadata);
    if (rv != 0) {
        goto err_after_dir_alloc;
    }

    /* Detect the file system block size */
    rv = raft__uv_file_block_size(uv->dir, &block_size);
    if (rv != 0) {
        raft_errorf(logger, "detect block size: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_dir_alloc;
    }

    /* We expect the maximum segment size to be a multiple of the block size */
    assert(RAFT_IO_UV_MAX_SEGMENT_SIZE % block_size == 0);
    n_blocks = RAFT_IO_UV_MAX_SEGMENT_SIZE / block_size;

    uv->n_active = 0;

    /* Initialize the segment loader, preparer, closer and writer */
    raft__io_uv_loader_init(&uv->loader, logger, dir);

    rv = raft__io_uv_preparer_init(&uv->preparer, loop, logger, dir, block_size,
                                   n_blocks);
    assert(rv == 0); /* At the moment this never fails */

    uv->preparer.data = uv;
    uv->n_active++;

    rv = raft__io_uv_closer_init(&uv->closer, loop, logger, &uv->loader, dir);
    assert(rv == 0); /* At the moment this never fails */

    uv->closer.data = uv;
    uv->n_active++;

    raft__io_uv_writer_init(&uv->writer, loop, logger, &uv->preparer,
                            &uv->closer, uv->dir, block_size, n_blocks);
    assert(rv == 0); /* At the moment this never fails */

    uv->writer.data = uv;
    uv->n_active++;

    /* Initialize the RPC system */
    rv = raft__io_uv_rpc_init(&uv->rpc, logger, loop, transport);
    if (rv != 0) {
        goto err_after_writer_init;
    }
    uv->rpc.data = io;

    uv->io = io;

    uv->logger = logger;
    uv->loop = loop;
    uv->transport = transport;

    uv->ticker.data = io;

    uv->tick_cb = NULL;

    uv->last_tick = 0;

    uv->stop_cb = NULL;

    /* Set the raft_io implementation. */
    io->impl = uv;
    io->init = raft_io_uv__init;
    io->start = raft_io_uv__start;
    io->close = raft_io_uv__close;
    io->load = raft_io_uv__load;
    io->bootstrap = raft_io_uv__bootstrap;
    io->set_term = raft_io_uv__set_term;
    io->set_vote = raft_io_uv__set_vote;
    io->append = raft_io_uv__append;
    io->truncate = raft_io_uv__truncate;
    io->send = raft_io_uv__send;

    return 0;

err_after_writer_init:
    raft__io_uv_writer_close(&uv->writer, NULL);
    raft__io_uv_preparer_close(&uv->preparer, NULL);

err_after_dir_alloc:
    raft_free(uv->dir);

err_after_uv_alloc:
    raft_free(uv);

err:
    assert(rv != 0);
    return rv;
}

void raft_io_uv_close(struct raft_io *io)
{
    struct raft_io_uv *uv;

    uv = io->impl;

    raft_free(uv->dir);
    raft_free(uv);
}

static int raft__io_uv_ensure_dir(struct raft_logger *logger, const char *dir)
{
    struct stat sb;
    int rv;

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strlen(dir) > RAFT__IO_UV_FS_MAX_DIR_LEN) {
        return RAFT_ERR_IO_NAMETOOLONG;
    }

    /* Make sure we have a directory we can write into. */
    rv = stat(dir, &sb);
    if (rv == -1) {
        if (errno == ENOENT) {
            rv = mkdir(dir, 0700);
            if (rv != 0) {
                raft_errorf(logger, "create data directory %s: %s", dir,
                            uv_strerror(-errno));
                return RAFT_ERR_IO;
            }
        } else {
            raft_errorf(logger, "access data directory %s: %s", dir,
                        uv_strerror(-errno));
            return RAFT_ERR_IO;
        }
    } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
        raft_errorf(logger, "path %s is not a directory", dir);
        return RAFT_ERR_IO;
    }

    return 0;
}

static void raft__io_uv_recv_cb(struct raft__io_uv_rpc *rpc,
                                struct raft_message *msg)
{
    struct raft_io *io;
    struct raft_io_uv *uv;

    io = rpc->data;
    uv = io->impl;

    uv->recv_cb(io, msg);
}

static int raft_io_uv__init(struct raft_io *io,
                            unsigned id,
                            const char *address)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->impl;

    rv = uv->transport->init(uv->transport, id, address);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft_io_uv__start(struct raft_io *io,
                             unsigned msecs,
                             raft_io_tick_cb tick_cb,
                             raft_io_recv_cb recv_cb)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->impl;

    assert(uv->tick_cb == NULL);

    uv->tick_cb = tick_cb;
    uv->recv_cb = recv_cb;

    uv->last_tick = uv_now(uv->loop);

    /* Initialize the tick timer handle. */
    rv = uv_timer_init(uv->loop, &uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_timer_init: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err;
    }

    /* Start the tick timer handle. */
    rv = uv_timer_start(&uv->ticker, raft_io_uv__ticker_cb, 0, msecs);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_timer_start: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_ticker_init;
    }
    uv->n_active++;

    rv = raft__io_uv_rpc_start(&uv->rpc, raft__io_uv_recv_cb);
    if (rv != 0) {
        goto err_after_ticker_start;
    }
    uv->n_active++;

    uv->n_active++;  // The transport

    return 0;

err_after_ticker_start:
    uv_timer_stop(&uv->ticker);

err_after_ticker_init:
    uv_close((struct uv_handle_s *)&uv->ticker, NULL);
err:
    assert(rv != 0);

    return rv;
}

static void raft__io_uv_transport_close_cb(struct raft_io_uv_transport *t)
{
    struct raft__io_uv_rpc *rpc = t->data;
    struct raft_io *io = rpc->data;
    struct raft_io_uv *uv = io->impl;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static int raft_io_uv__close(struct raft_io *io, void (*cb)(struct raft_io *io))
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->impl;

    assert(uv->stop_cb == NULL);

    uv->stop_cb = cb;

    /* Stop the tick timer handle. */
    rv = uv_timer_stop(&uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_stop_timer: %s", uv_strerror(rv));
        return RAFT_ERR_INTERNAL;
    }

    /* Close the ticker timer handle. */
    uv_close((uv_handle_t *)&uv->ticker, raft_io_uv__ticker_close_cb);

    raft__io_uv_writer_close(&uv->writer, raft__io_uv_writer__close_cb);

    raft__io_uv_rpc_close(&uv->rpc, raft_io_uv__rpc_stop_cb);

    return 0;
}

static int raft_io_uv__load(struct raft_io *io,
                            raft_term *term,
                            unsigned *voted_for,
                            struct raft_snapshot **snapshot,
                            struct raft_entry **entries,
                            size_t *n_entries)
{
    struct raft_io_uv *uv;
    raft_index start_index = 1;
    raft_index last_index;
    int rv;

    uv = io->impl;

    assert(uv->metadata.version > 0);

    *term = uv->metadata.term;
    *voted_for = uv->metadata.voted_for;

    *snapshot = NULL;

    rv = raft__io_uv_loader_load_all(&uv->loader, snapshot, entries, n_entries);
    if (rv != 0) {
        return rv;
    }

    if (*snapshot != NULL) {
        start_index = (*snapshot)->index + 1;
    }

    last_index = start_index + *n_entries - 1;

    /* Set the index of the last entry that was persisted. */
    raft__io_uv_closer_set_last_index(&uv->closer, last_index);

    /* Set the index of the next entry that will be appended. */
    raft__io_uv_writer_set_next_index(&uv->writer, last_index + 1);

    return 0;
}

static int raft_io_uv__bootstrap(struct raft_io *io,
                                 const struct raft_configuration *configuration)
{
    struct raft_io_uv *uv;
    struct raft_buffer buf;
    int rv;

    uv = io->impl;

    /* We shouldn't have written anything else yet. */
    if (uv->metadata.term != 0) {
        rv = RAFT_ERR_IO_NOTEMPTY;
        goto err;
    }

    /* Encode the given configuration. */
    rv = raft_configuration_encode(configuration, &buf);
    if (rv != 0) {
        goto err;
    }

    /* Write the term */
    rv = raft_io_uv__set_term(io, 1);
    if (rv != 0) {
        goto err_after_configuration_encode;
    }

    /* Create the first closed segment file, containing just one entry. */
    rv = raft__io_uv_create_closed_1_1(uv, &buf);
    if (rv != 0) {
        goto err_after_configuration_encode;
    }

    raft_free(buf.base);

    return 0;

err_after_configuration_encode:
    raft_free(buf.base);

err:
    assert(rv != 0);
    return rv;
}

static int raft_io_uv__set_term(struct raft_io *io, const raft_term term)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->impl;

    assert(uv->metadata.version > 0);

    uv->metadata.version++;
    uv->metadata.term = term;
    uv->metadata.voted_for = 0;

    rv = raft__io_uv_metadata_store(uv->logger, uv->dir, &uv->metadata);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft_io_uv__set_vote(struct raft_io *io, const unsigned server_id)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->impl;

    assert(uv->metadata.version > 0);

    uv->metadata.version++;
    uv->metadata.voted_for = server_id;

    rv = raft__io_uv_metadata_store(uv->logger, uv->dir, &uv->metadata);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

struct raft__io_uv_append
{
    struct raft__io_uv_writer_append append;
    void *data;
    void (*cb)(void *data, int status);
};

static void raft__io_uv_append_cb(struct raft__io_uv_writer_append *append,
                                  int status)
{
    struct raft__io_uv_append *req = append->data;

    req->cb(req->data, status);
    raft_free(req);
}

static int raft_io_uv__append(struct raft_io *io,
                              const struct raft_entry entries[],
                              unsigned n,
                              void *data,
                              void (*cb)(void *data, int status))
{
    struct raft_io_uv *uv;
    struct raft__io_uv_append *req;
    int rv;

    uv = io->impl;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    req->append.data = req;
    req->data = data;
    req->cb = cb;

    rv = raft__io_uv_writer_append(&uv->writer, &req->append, entries, n,
                                   raft__io_uv_append_cb);
    if (rv != 0) {
        goto err_after_req_alloc;
    }

    return 0;

err_after_req_alloc:
    raft_free(req);

err:
    assert(rv != 0);
    return rv;
}

static int raft_io_uv__truncate(struct raft_io *io, raft_index index)
{
    struct raft_io_uv *uv;

    uv = io->impl;

    return raft__io_uv_writer_truncate(&uv->writer, index);
}

static int raft_io_uv__send(struct raft_io *io,
                            struct raft_io_send *req,
                            const struct raft_message *message,
                            raft_io_send_cb cb)
{
    struct raft_io_uv *uv;

    uv = io->impl;

    return raft__io_uv_rpc_send(&uv->rpc, req, message, cb);
}

static void raft_io_uv__ticker_cb(uv_timer_t *ticker)
{
    struct raft_io *io;
    struct raft_io_uv *uv;
    uint64_t now;

    io = ticker->data;
    uv = io->impl;

    /* Get current time */
    now = uv_now(uv->loop);

    /* Invoke the ticker */
    uv->tick_cb(io, now - uv->last_tick);

    /* Update the last tick timestamp */
    uv->last_tick = now;
}

static void raft_io_uv__ticker_close_cb(uv_handle_t *handle)
{
    struct raft_io *io;
    struct raft_io_uv *uv;

    io = handle->data;
    uv = io->impl;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static void raft__io_uv_preparer__close_cb(struct raft__io_uv_preparer *p)
{
    struct raft_io_uv *uv = p->data;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static void raft__io_uv_closer__close_cb(struct raft__io_uv_closer *p)
{
    struct raft_io_uv *uv = p->data;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static void raft__io_uv_writer__close_cb(struct raft__io_uv_writer *w)
{
    struct raft_io_uv *uv = w->data;

    uv->n_active--;

    raft__io_uv_preparer_close(&uv->preparer, raft__io_uv_preparer__close_cb);
    raft__io_uv_closer_close(&uv->closer, raft__io_uv_closer__close_cb);
}

static void raft_io_uv__rpc_stop_cb(struct raft__io_uv_rpc *rpc)
{
    struct raft_io *io = rpc->data;
    struct raft_io_uv *uv = io->impl;

    uv->n_active--;

    uv->transport->close(uv->transport, raft__io_uv_transport_close_cb);
}

static void raft_io_uv__maybe_stopped(struct raft_io_uv *uv)
{
    if (uv->stop_cb != NULL && uv->n_active == 0) {
        uv->stop_cb(uv->io);
    }
}

static int raft__io_uv_create_closed_1_1(struct raft_io_uv *uv,
                                         const struct raft_buffer *buf)
{
    raft__io_uv_fs_path path;
    raft__io_uv_fs_filename filename;
    int fd;
    int rv;

    /* Render the path */
    sprintf(filename, "%u-%u", 1, 1);
    raft__io_uv_fs_join(uv->dir, filename, path);

    /* Open the file. */
    fd = open(path, O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        raft_errorf(uv->logger, "open %s: %s", path, strerror(errno));
        return RAFT_ERR_IO;
    }

    /* Write the content */
    rv = raft__io_uv_write_closed_1_1(uv, fd, buf);
    if (rv != 0) {
        return rv;
    }

    close(fd);

    rv = raft__io_uv_fs_sync_dir(uv->dir);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static int raft__io_uv_write_closed_1_1(struct raft_io_uv *uv,
                                        const int fd,
                                        const struct raft_buffer *conf)
{
    void *buf;
    size_t len;
    size_t cap;
    void *cursor;
    void *header;
    unsigned crc1; /* Header checksum */
    unsigned crc2; /* Data checksum */
    void *crc1_p;  /* Pointer to header checksum slot */
    void *crc2_p;  /* Pointer to data checksum slot */
    int rv;

    /* Make sure that the given encoded configuration fits in the first
     * block */
    cap = uv->writer.block_size - (sizeof(uint64_t) /* Format version */ +
                                   sizeof(uint64_t) /* Checksums */ +
                                   raft_io_uv_sizeof__batch_header(1));
    if (conf->len > cap) {
        return RAFT_ERR_IO_TOOBIG;
    }

    len = sizeof(uint64_t) * 2 + raft_io_uv_sizeof__batch_header(1) + conf->len;
    buf = malloc(len);
    if (buf == NULL) {
        return RAFT_ERR_NOMEM;
    }
    memset(buf, 0, len);

    cursor = buf;

    raft__put64(&cursor, RAFT__IO_UV_WRITER_FORMAT); /* Format version */

    crc1_p = cursor;
    raft__put32(&cursor, 0);

    crc2_p = cursor;
    raft__put32(&cursor, 0);

    header = cursor;
    raft__put64(&cursor, 1);                     /* Number of entries */
    raft__put64(&cursor, 1);                     /* Entry term */
    raft__put8(&cursor, RAFT_LOG_CONFIGURATION); /* Entry type */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put8(&cursor, 0);                      /* Unused */
    raft__put32(&cursor, conf->len);             /* Size of entry data */

    memcpy(cursor, conf->base, conf->len);

    crc1 = raft__crc32(header, raft_io_uv_sizeof__batch_header(1), 0);
    raft__put32(&crc1_p, crc1);

    crc2 = raft__crc32(conf->base, conf->len, 0);
    raft__put32(&crc2_p, crc2);

    rv = write(fd, buf, len);
    if (rv == -1) {
        free(buf);
        raft_errorf(uv->logger, "write segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }
    if (rv != (int)len) {
        free(buf);
        raft_errorf(uv->logger, "write segment 1: only %d bytes written", rv);
        return RAFT_ERR_IO;
    }

    free(buf);

    rv = fsync(fd);
    if (rv == -1) {
        raft_errorf(uv->logger, "fsync segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }

    return 0;
}
