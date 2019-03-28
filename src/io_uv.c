#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"
#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "io_uv.h"
#include "io_uv_encoding.h"
#include "io_uv_fs.h"
#include "io_uv_load.h"
#include "logging.h"

/* Retry to connect to peer servers every second.
 *
 * TODO: implement an exponential backoff instead.  */
#define IO_UV__CONNECT_RETRY_DELAY 1000

/* Implementation of raft_io->init. */
static int io_uv__init(struct raft_io *io, unsigned id, const char *address)
{
    struct io_uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->state == 0);
    uv->id = id;
    rv = uv->transport->init(uv->transport, id, address);
    if (rv != 0) {
        return rv;
    }
    rv = uv_timer_init(uv->loop, &uv->timer);
    assert(rv == 0); /* This should never fail */
    uv->timer.data = uv;
    uv->state = IO_UV__ACTIVE;
    return 0;
}

/* Periodic timer callback */
static void timer_cb(uv_timer_t *timer)
{
    struct io_uv *uv;
    uv = timer->data;
    if (uv->tick_cb != NULL) {
        uv->tick_cb(uv->io);
    }
}

/* Implementation of raft_io->start. */
static int io_uv__start(struct raft_io *io,
                        unsigned msecs,
                        raft_io_tick_cb tick_cb,
                        raft_io_recv_cb recv_cb)
{
    struct io_uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->state == IO_UV__ACTIVE);
    uv->tick_cb = tick_cb;
    uv->recv_cb = recv_cb;
    rv = io_uv__listen(uv);
    if (rv != 0) {
        return rv;
    }
    rv = uv_timer_start(&uv->timer, timer_cb, msecs, msecs);
    assert(rv == 0);
    return 0;
}

static bool has_pending_disk_io(struct io_uv *uv)
{
    return !RAFT__QUEUE_IS_EMPTY(&uv->append_segments) ||
           !RAFT__QUEUE_IS_EMPTY(&uv->finalize_reqs) ||
           uv->finalize_work.data != NULL ||
           !RAFT__QUEUE_IS_EMPTY(&uv->truncate_reqs) ||
           uv->truncate_work.data != NULL ||
           !RAFT__QUEUE_IS_EMPTY(&uv->snapshot_put_reqs) ||
           !RAFT__QUEUE_IS_EMPTY(&uv->snapshot_get_reqs);
}

void io_uv__maybe_close(struct io_uv *uv)
{
    if (uv->state == IO_UV__ACTIVE) {
        return;
    }

    if (uv->state == IO_UV__CLOSED) {
        assert(!has_pending_disk_io(uv));
        return;
    }

    assert(uv->state == IO_UV__CLOSING);

    if (has_pending_disk_io(uv)) {
        return;
    }

    uv->state = IO_UV__CLOSED;
    if (uv->close_cb != NULL) {
        uv->close_cb(uv->io);
    }
}

static void transport_close_cb(struct raft_io_uv_transport *t)
{
    struct io_uv *uv = t->data;
    io_uv__maybe_close(uv);
}

static void timer_close_cb(uv_handle_t *handle)
{
    struct io_uv *uv = handle->data;
    io_uv__clients_stop(uv);
    io_uv__servers_stop(uv);
    io_uv__prepare_stop(uv);
    io_uv__append_stop(uv);
    io_uv__truncate_stop(uv);
    uv->transport->close(uv->transport, transport_close_cb);
}

/* Implementation of raft_io->close. */
static int io_uv__close(struct raft_io *io, void (*cb)(struct raft_io *io))
{
    struct io_uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->state == IO_UV__ACTIVE);
    uv->close_cb = cb;
    uv->state = IO_UV__CLOSING;
    rv = uv_timer_stop(&uv->timer);
    assert(rv == 0);
    /* Start the shutdown sequence by closing the timer handle. */
    uv_close((uv_handle_t *)&uv->timer, timer_close_cb);
    return 0;
}

/* Implementation of raft_io->load. */
static int io_uv__load(struct raft_io *io,
                       raft_term *term,
                       unsigned *voted_for,
                       struct raft_snapshot **snapshot,
                       raft_index *start_index,
                       struct raft_entry **entries,
                       size_t *n_entries)
{
    struct io_uv *uv;
    raft_index last_index;
    int rv;

    uv = io->impl;
    assert(uv->metadata.version > 0);

    *term = uv->metadata.term;
    *voted_for = uv->metadata.voted_for;
    *snapshot = NULL;

    rv = io_uv__load_all(uv, snapshot, start_index, entries, n_entries);
    if (rv != 0) {
        return rv;
    }

    last_index = *start_index + *n_entries - 1;
    if (*snapshot != NULL && last_index < (*snapshot)->index) {
        /* TODO: all entries are behind the snapshot, we should delete them
         * instead of aborting */
        errorf(io, "last index %llu is behind snapshot %llu", last_index,
               (*snapshot)->index);
        return RAFT_ERR_IO_CORRUPT;
    }

    /* Set the index of the last entry that was persisted. */
    uv->finalize_last_index = last_index;

    /* Set the index of the next entry that will be appended. */
    uv->append_next_index = last_index + 1;

    return 0;
}

static int io_uv__bootstrap(struct raft_io *io,
                            const struct raft_configuration *conf);

/* Implementation of raft_io->set_term. */
static int io_uv__set_term(struct raft_io *io, const raft_term term)
{
    struct io_uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->metadata.version > 0);
    uv->metadata.version++;
    uv->metadata.term = term;
    uv->metadata.voted_for = 0;
    rv = io_uv__metadata_store(uv->io, uv->dir, &uv->metadata);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Implementation of raft_io->set_term. */
static int io_uv__set_vote(struct raft_io *io, const unsigned server_id)
{
    struct io_uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->metadata.version > 0);
    uv->metadata.version++;
    uv->metadata.voted_for = server_id;
    rv = io_uv__metadata_store(uv->io, uv->dir, &uv->metadata);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Implementation of raft_io->time. */
static raft_time io_uv__time(struct raft_io *io)
{
    struct io_uv *uv;
    uv = io->impl;
    return uv_now(uv->loop);
}

/* Implementation of raft_io->random. */
static int io_uv__random(struct raft_io *io, int min, int max)
{
    (void)io;
    return min + (abs(rand()) % (max - min));
}

/* Implementation of raft_io->emit. */
static void io_uv__emit(struct raft_io *io, int level, const char *format, ...)
{
    struct io_uv *uv;
    va_list args;
    uv = io->impl;
    va_start(args, format);
    emit_to_stream(stderr, uv->id, uv_now(uv->loop), level, format, args);
    va_end(args);
}

/**
 * Open and allocate the first closed segment, containing just one entry, and
 * return its file descriptor.
 */
static int raft__io_uv_create_closed_1_1(struct io_uv *uv,
                                         const struct raft_buffer *buf);

/**
 * Write the first block of the first closed segment.
 */
static int raft__io_uv_write_closed_1_1(struct io_uv *uv,
                                        const int fd,
                                        const struct raft_buffer *buf);

/* Copy dir1 into dir2 and strip any trailing slash. */
static void copy_dir(const char *dir1, char **dir2)
{
    *dir2 = raft_malloc(strlen(dir1) + 1);
    if (*dir2 == NULL) {
        return;
    }
    strcpy(*dir2, dir1);

    if ((*dir2)[strlen(*dir2) - 1] == '/') {
        (*dir2)[strlen(*dir2) - 1] = 0;
    }
}

int raft_io_uv_init(struct raft_io *io,
                    struct uv_loop_s *loop,
                    const char *dir,
                    struct raft_io_uv_transport *transport)
{
    struct io_uv *uv;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    uv->io = io;
    uv->loop = loop;
    copy_dir(dir, &uv->dir);
    if (uv->dir == NULL) {
        rv = RAFT_ENOMEM;
        goto err_after_uv_alloc;
    }
    uv->transport = transport;
    uv->transport->data = uv;
    uv->id = 0;
    uv->state = 0;
    uv->errored = false;
    uv->block_size = 0; /* Detected in raft_io->init() */
    uv->n_blocks = 0;   /* Calculated in raft_io->init() */
    uv->clients = NULL;
    uv->n_clients = 0;
    uv->servers = NULL;
    uv->n_servers = 0;
    uv->connect_retry_delay = IO_UV__CONNECT_RETRY_DELAY;
    uv->preparing = NULL;
    RAFT__QUEUE_INIT(&uv->prepare_reqs);
    RAFT__QUEUE_INIT(&uv->prepare_pool);
    uv->prepare_next_counter = 1;
    uv->append_next_index = 1;
    RAFT__QUEUE_INIT(&uv->append_segments);
    RAFT__QUEUE_INIT(&uv->append_pending_reqs);
    RAFT__QUEUE_INIT(&uv->append_writing_reqs);
    RAFT__QUEUE_INIT(&uv->finalize_reqs);
    uv->finalize_last_index = 0;
    uv->finalize_work.data = NULL;
    RAFT__QUEUE_INIT(&uv->truncate_reqs);
    uv->truncate_work.data = NULL;
    RAFT__QUEUE_INIT(&uv->snapshot_put_reqs);
    RAFT__QUEUE_INIT(&uv->snapshot_get_reqs);
    uv->snapshot_put_work.data = NULL;

    io->emit = io_uv__emit; /* Used below */
    io->impl = uv;

    /* Ensure that the data directory exists and is accessible */
    rv = io_uv__ensure_dir(uv->io, uv->dir);
    if (rv != 0) {
        goto err_after_dir_alloc;
    }

    /* Load current metadata */
    rv = io_uv__metadata_load(io, dir, &uv->metadata);
    if (rv != 0) {
        goto err_after_dir_alloc;
    }

    /* Detect the file system block size */
    rv = uv__file_block_size(uv->dir, &uv->block_size);
    if (rv != 0) {
        errorf(io, "detect block size: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_dir_alloc;
    }

    /* We expect the maximum segment size to be a multiple of the block size */
    assert(RAFT_IO_UV_MAX_SEGMENT_SIZE % uv->block_size == 0);
    uv->n_blocks = RAFT_IO_UV_MAX_SEGMENT_SIZE / uv->block_size;

    uv->tick_cb = NULL;
    uv->close_cb = NULL;

    /* Set the raft_io implementation. */
    io->init = io_uv__init;
    io->start = io_uv__start;
    io->close = io_uv__close;
    io->load = io_uv__load;
    io->bootstrap = io_uv__bootstrap;
    io->set_term = io_uv__set_term;
    io->set_vote = io_uv__set_vote;
    io->append = io_uv__append;
    io->truncate = io_uv__truncate;
    io->send = io_uv__send;
    io->snapshot_put = io_uv__snapshot_put;
    io->snapshot_get = io_uv__snapshot_get;
    io->time = io_uv__time;
    io->random = io_uv__random;

    return 0;

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
    struct io_uv *uv;
    uv = io->impl;
    if (uv->clients != NULL) {
        raft_free(uv->clients);
    }
    if (uv->servers != NULL) {
        raft_free(uv->servers);
    }
    raft_free(uv->dir);
    raft_free(uv);
}

static int io_uv__bootstrap(struct raft_io *io,
                            const struct raft_configuration *configuration)
{
    struct io_uv *uv;
    struct raft_buffer buf;
    int rv;

    uv = io->impl;

    /* We shouldn't have written anything else yet. */
    if (uv->metadata.term != 0) {
        rv = RAFT_ERR_IO_NOTEMPTY;
        goto err;
    }

    /* Encode the given configuration. */
    rv = configuration__encode(configuration, &buf);
    if (rv != 0) {
        goto err;
    }

    /* Write the term */
    rv = io_uv__set_term(io, 1);
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

static int raft__io_uv_create_closed_1_1(struct io_uv *uv,
                                         const struct raft_buffer *buf)
{
    io_uv__filename filename;
    int fd;
    int rv;

    /* Render the path */
    sprintf(filename, "%u-%u", 1, 1);

    /* Open the file. */
    fd = raft__io_uv_fs_open(uv->dir, filename, O_WRONLY | O_CREAT | O_EXCL);
    if (fd == -1) {
        errorf(uv->io, "open %s: %s", filename, strerror(errno));
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

static int raft__io_uv_write_closed_1_1(struct io_uv *uv,
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
    cap = uv->block_size -
          (sizeof(uint64_t) /* Format version */ +
           sizeof(uint64_t) /* Checksums */ + io_uv__sizeof_batch_header(1));
    if (conf->len > cap) {
        return RAFT_ERR_IO_TOOBIG;
    }

    len = sizeof(uint64_t) * 2 + io_uv__sizeof_batch_header(1) + conf->len;
    buf = malloc(len);
    if (buf == NULL) {
        return RAFT_ENOMEM;
    }
    memset(buf, 0, len);

    cursor = buf;

    byte__put64(&cursor, IO_UV__DISK_FORMAT); /* Format version */

    crc1_p = cursor;
    byte__put32(&cursor, 0);

    crc2_p = cursor;
    byte__put32(&cursor, 0);

    header = cursor;
    byte__put64(&cursor, 1);                 /* Number of entries */
    byte__put64(&cursor, 1);                 /* Entry term */
    byte__put8(&cursor, RAFT_CONFIGURATION); /* Entry type */
    byte__put8(&cursor, 0);                  /* Unused */
    byte__put8(&cursor, 0);                  /* Unused */
    byte__put8(&cursor, 0);                  /* Unused */
    byte__put32(&cursor, conf->len);         /* Size of entry data */

    memcpy(cursor, conf->base, conf->len);

    crc1 = byte__crc32(header, io_uv__sizeof_batch_header(1), 0);
    byte__put32(&crc1_p, crc1);

    crc2 = byte__crc32(conf->base, conf->len, 0);
    byte__put32(&crc2_p, crc2);

    rv = write(fd, buf, len);
    if (rv == -1) {
        free(buf);
        errorf(uv->io, "write segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }
    if (rv != (int)len) {
        free(buf);
        errorf(uv->io, "write segment 1: only %d bytes written", rv);
        return RAFT_ERR_IO;
    }

    free(buf);

    rv = fsync(fd);
    if (rv == -1) {
        errorf(uv->io, "fsync segment 1: %s", strerror(errno));
        return RAFT_ERR_IO;
    }

    return 0;
}
