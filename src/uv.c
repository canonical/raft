#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
#include "configuration.h"
#include "entry.h"
#include "logging.h"
#include "uv.h"
#include "uv_encoding.h"
#include "uv_os.h"

/* Retry to connect to peer servers every second.
 *
 * TODO: implement an exponential backoff instead.  */
#define CONNECT_RETRY_DELAY 1000

/* Implementation of raft_io->init. */
static int uvInit(struct raft_io *io,
                  struct raft_logger *logger,
                  unsigned id,
                  const char *address)
{
    struct uv *uv;
    size_t direct_io;
    int rv;

    uv = io->impl;

    uv->logger = logger;

    /* Ensure that the data directory exists and is accessible */
    rv = uvEnsureDir(uv->tracer, uv->dir);
    if (rv != 0) {
        uvErrorf(uv, "data dir %s: %s", uv->dir, osStrError(rv));
        rv = RAFT_IOERR;
        goto err;
    }

    /* Load current metadata */
    rv = uvMetadataLoad(uv, &uv->metadata);
    if (rv != 0) {
        goto err;
    }

    /* Detect the I/O capabilities of the underlying file system. */
    rv = uvProbeIoCapabilities(uv->dir, &direct_io, &uv->async_io);
    if (rv != 0) {
        uvErrorf(uv, "probe I/O capabilities: %s", uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err;
    }
    uv->direct_io = direct_io != 0;
    uv->block_size = direct_io != 0 ? direct_io : 4096;

    /* We expect the maximum segment size to be a multiple of the block size */
    assert(UV__MAX_SEGMENT_SIZE % uv->block_size == 0);
    uv->n_blocks = UV__MAX_SEGMENT_SIZE / uv->block_size;

    assert(uv->state == 0);
    uv->id = id;
    rv = uv->transport->init(uv->transport, id, address);
    if (rv != 0) {
        return rv;
    }
    rv = uv_timer_init(uv->loop, &uv->timer);
    assert(rv == 0); /* This should never fail */
    uv->timer.data = uv;
    uv->state = UV__ACTIVE;
    uv->log_level = RAFT_INFO;

    return 0;

err:
    assert(rv != 0);
    return rv;
}

/* Periodic timer callback */
static void timerCb(uv_timer_t *timer)
{
    struct uv *uv;
    uv = timer->data;
    if (uv->tick_cb != NULL) {
        uv->tick_cb(uv->io);
    }
}

/* Implementation of raft_io->start. */
static int uvStart(struct raft_io *io,
                   unsigned msecs,
                   raft_io_tick_cb tick_cb,
                   raft_io_recv_cb recv_cb)
{
    struct uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->state == UV__ACTIVE);
    uv->tick_cb = tick_cb;
    uv->recv_cb = recv_cb;
    rv = uvRecv(uv);
    if (rv != 0) {
        return rv;
    }
    rv = uv_timer_start(&uv->timer, timerCb, msecs, msecs);
    assert(rv == 0);
    return 0;
}

static bool hasPendingDiskIO(struct uv *uv)
{
    return !QUEUE_IS_EMPTY(&uv->append_segments) ||
           !QUEUE_IS_EMPTY(&uv->finalize_reqs) ||
           uv->finalize_work.data != NULL ||
           !QUEUE_IS_EMPTY(&uv->truncate_reqs) ||
           uv->truncate_work.data != NULL ||
           !QUEUE_IS_EMPTY(&uv->snapshot_put_reqs) ||
           !QUEUE_IS_EMPTY(&uv->snapshot_get_reqs);
}

void uvMaybeClose(struct uv *uv)
{
    if (!uv->closing) {
        return;
    }

    if (uv->state == UV__CLOSED) {
        assert(!hasPendingDiskIO(uv));
        return;
    }

    if (hasPendingDiskIO(uv)) {
        return;
    }

    uv->state = UV__CLOSED;
    if (uv->close_cb != NULL) {
        uv->close_cb(uv->io);
    }
}

static void timerCloseCb(uv_handle_t *handle)
{
    struct uv *uv = handle->data;
    uvMaybeClose(uv);
}

static void transportCloseCb(struct raft_uv_transport *t)
{
    struct uv *uv = t->data;
    uv_close((uv_handle_t *)&uv->timer, timerCloseCb);
}

/* Implementation of raft_io->close. */
static int uvClose(struct raft_io *io, void (*cb)(struct raft_io *io))
{
    struct uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->state == UV__ACTIVE);
    uv->close_cb = cb;
    uv->closing = true;
    rv = uv_timer_stop(&uv->timer);
    assert(rv == 0);
    uvSendClose(uv);
    uvRecvClose(uv);
    uvPrepareClose(uv);
    uvAppendClose(uv);
    uvTruncateClose(uv);
    uv->transport->close(uv->transport, transportCloseCb);
    return 0;
}

/* Load the last snapshot (if any) and all entries contained in all segment
 * files of the data directory. */
static int loadSnapshotAndEntries(struct uv *uv,
                                  struct raft_snapshot **snapshot,
                                  raft_index *start_index,
                                  struct raft_entry *entries[],
                                  size_t *n)
{
    struct uvSnapshotInfo *snapshots;
    struct uvSegmentInfo *segments;
    size_t n_snapshots;
    size_t n_segments;
    raft_index last_index;
    int rv;

    *snapshot = NULL;
    *start_index = 1;
    *entries = NULL;
    *n = 0;

    /* List available snapshots and segments. */
    rv = uvList(uv, &snapshots, &n_snapshots, &segments, &n_segments);
    if (rv != 0) {
        goto err;
    }

    /* Load the most recent snapshot, if any. */
    if (snapshots != NULL) {
        *snapshot = raft_malloc(sizeof **snapshot);
        if (*snapshot == NULL) {
            rv = RAFT_NOMEM;
            goto err;
        }
        rv = uvSnapshotLoad(uv, &snapshots[n_snapshots - 1], *snapshot);
        if (rv != 0) {
            goto err;
        }
        raft_free(snapshots);
        snapshots = NULL;

        last_index = (*snapshot)->index;
        /* Update the start index. If there are closed segments on disk and the
         * first index of the first closed segment is lower than the snapshot's
         * last index, let's retain those entries. TODO: implement a trailing
         * amount. */
        if (segments != NULL && !segments[0].is_open &&
            segments[0].first_index <= last_index) {
            *start_index = segments[0].first_index;
        } else {
            *start_index = (*snapshot)->index + 1;
        }
    }

    /* Read data from segments, closing any open segments. */
    if (segments != NULL) {
        rv = uvSegmentLoadAll(uv, *start_index, segments, n_segments, entries,
                              n);
        if (rv != 0) {
            goto err;
        }
        raft_free(segments);
        segments = NULL;
    }

    last_index = *start_index + *n - 1;
    if (*snapshot != NULL && last_index < (*snapshot)->index) {
        /* TODO: entries are behind the snapshot, we should delete them from
         * disk. */
        *start_index = (*snapshot)->index + 1;
        entryBatchesDestroy(*entries, *n);
        *entries = NULL;
        *n = 0;
    }

    return 0;

err:
    assert(rv != 0);
    if (snapshots != NULL) {
        raft_free(snapshots);
    }
    if (segments != NULL) {
        raft_free(segments);
    }
    if (*snapshot != NULL) {
        raft_free(*snapshot);
    }
    return rv;
}

/* Implementation of raft_io->load. */
static int uvLoad(struct raft_io *io,
                  raft_term *term,
                  unsigned *voted_for,
                  struct raft_snapshot **snapshot,
                  raft_index *start_index,
                  struct raft_entry **entries,
                  size_t *n_entries)
{
    struct uv *uv;
    raft_index last_index;
    int rv;
    uv = io->impl;
    assert(uv->metadata.version > 0);

    *term = uv->metadata.term;
    *voted_for = uv->metadata.voted_for;
    *snapshot = NULL;

    rv = loadSnapshotAndEntries(uv, snapshot, start_index, entries, n_entries);
    if (rv != 0) {
        return rv;
    }

    last_index = *start_index + *n_entries - 1;

    /* Set the index of the last entry that was persisted. */
    uv->finalize_last_index = last_index;

    /* Set the index of the next entry that will be appended. */
    uv->append_next_index = last_index + 1;

    return 0;
}

/* Implementation of raft_io->set_term. */
static int uvSetTerm(struct raft_io *io, const raft_term term)
{
    struct uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->metadata.version > 0);
    uv->metadata.version++;
    uv->metadata.term = term;
    uv->metadata.voted_for = 0;
    rv = uvMetadataStore(uv, &uv->metadata);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Implementation of raft_io->bootstrap. */
static int uvBootstrap(struct raft_io *io,
                       const struct raft_configuration *configuration)
{
    struct uv *uv;
    int rv;

    uv = io->impl;

    /* We shouldn't have written anything else yet. */
    if (uv->metadata.term != 0) {
        return RAFT_CANTBOOTSTRAP;
    }

    /* Write the term */
    rv = uvSetTerm(io, 1);
    if (rv != 0) {
        return rv;
    }

    /* Create the first closed segment file, containing just one entry. */
    rv = uvSegmentCreateFirstClosed(uv, configuration);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/* Implementation of raft_io->set_term. */
static int uvSetVote(struct raft_io *io, const unsigned server_id)
{
    struct uv *uv;
    int rv;
    uv = io->impl;
    assert(uv->metadata.version > 0);
    uv->metadata.version++;
    uv->metadata.voted_for = server_id;
    rv = uvMetadataStore(uv, &uv->metadata);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

/* Implementation of raft_io->append (defined in uv_append.c).*/
int uvAppend(struct raft_io *io,
             struct raft_io_append *req,
             const struct raft_entry entries[],
             unsigned n,
             raft_io_append_cb cb);

/* Implementation of raft_io->truncate (defined in uv_truncate.c). */
int uvTruncate(struct raft_io *io, raft_index index);

/* Implementation of raft_io->send (defined in uv_send.c). */
int uvSend(struct raft_io *io,
           struct raft_io_send *req,
           const struct raft_message *message,
           raft_io_send_cb cb);

/* Implementation raft_io->snapshot_put (defined in uv_snapshot.c). */
int uvSnapshotPut(struct raft_io *io,
                  struct raft_io_snapshot_put *req,
                  const struct raft_snapshot *snapshot,
                  raft_io_snapshot_put_cb cb);

/* Implementation of raft_io->snapshot_get (defined in uv_snapshot.c). */
int uvSnapshotGet(struct raft_io *io,
                  struct raft_io_snapshot_get *req,
                  raft_io_snapshot_get_cb cb);

/* Implementation of raft_io->time. */
static raft_time uvTime(struct raft_io *io)
{
    struct uv *uv;
    uv = io->impl;
    return uv_now(uv->loop);
}

/* Implementation of raft_io->random. */
static int uvRandom(struct raft_io *io, int min, int max)
{
    (void)io;
    return min + (abs(rand()) % (max - min));
}

int raft_uv_init(struct raft_io *io,
                 struct uv_loop_s *loop,
                 const char *dir,
                 struct raft_uv_transport *transport)
{
    struct uv *uv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Ensure that the given path doesn't exceed our static buffer limit */
    if (strnlen(dir, UV__DIR_MAX_LEN + 1) > UV__DIR_MAX_LEN) {
        return RAFT_NAMETOOLONG;
    }

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        return RAFT_NOMEM;
    }

    uv->io = io;
    uv->loop = loop;
    strcpy(uv->dir, dir);
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
    uv->connect_retry_delay = CONNECT_RETRY_DELAY;
    uv->prepare_file = NULL;
    QUEUE_INIT(&uv->prepare_reqs);
    QUEUE_INIT(&uv->prepare_pool);
    uv->prepare_next_counter = 1;
    uv->append_next_index = 1;
    QUEUE_INIT(&uv->append_segments);
    QUEUE_INIT(&uv->append_pending_reqs);
    QUEUE_INIT(&uv->append_writing_reqs);
    QUEUE_INIT(&uv->finalize_reqs);
    uv->finalize_last_index = 0;
    uv->finalize_work.data = NULL;
    QUEUE_INIT(&uv->truncate_reqs);
    uv->truncate_work.data = NULL;
    QUEUE_INIT(&uv->snapshot_put_reqs);
    QUEUE_INIT(&uv->snapshot_get_reqs);
    uv->snapshot_put_work.data = NULL;
    uv->tick_cb = NULL;
    uv->closing = false;
    uv->close_cb = NULL;

    /* Set the raft_io implementation. */
    io->impl = uv;
    io->init = uvInit;
    io->start = uvStart;
    io->close = uvClose;
    io->load = uvLoad;
    io->bootstrap = uvBootstrap;
    io->set_term = uvSetTerm;
    io->set_vote = uvSetVote;
    io->append = uvAppend;
    io->truncate = uvTruncate;
    io->send = uvSend;
    io->snapshot_put = uvSnapshotPut;
    io->snapshot_get = uvSnapshotGet;
    io->time = uvTime;
    io->random = uvRandom;

    return 0;
}

void raft_uv_close(struct raft_io *io)
{
    struct uv *uv;
    uv = io->impl;
    if (uv->clients != NULL) {
        raft_free(uv->clients);
    }
    if (uv->servers != NULL) {
        raft_free(uv->servers);
    }
    raft_free(uv);
}
