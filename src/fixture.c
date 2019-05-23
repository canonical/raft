#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft/fixture.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "logging.h"
#include "queue.h"
#include "snapshot.h"

/* Defaults */
#define HEARTBEAT_TIMEOUT 100
#define ELECTION_TIMEOUT 1000
#define NETWORK_LATENCY 15
#define DISK_LATENCY 10

/* To keep in sync with raft.h */
#define N_MESSAGE_TYPES 5

/* Set to 1 to enable tracing. */
#if 0
static char messageDescription[1024];

/* Return a human-readable description of the given message */
static char *describeMessage(const struct raft_message *m)
{
    char *d = messageDescription;
    switch (m->type) {
        case RAFT_IO_REQUEST_VOTE:
            sprintf(d, "request vote");
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            sprintf(d, "request vote result");
            break;
        case RAFT_IO_APPEND_ENTRIES:
            sprintf(d, "append entries (n %u prev %llu/%llu)",
                    m->append_entries.n_entries,
                    m->append_entries.prev_log_index,
                    m->append_entries.prev_log_term);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            sprintf(d, "append entries result");
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            sprintf(d, "install snapshot");
            break;
        default:
            assert(0);
    }
    return d;
}
#define tracef(MSG, ...) debugf(io->io, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* Maximum number of peer stub instances connected to a certain stub
 * instance. This should be enough for testing purposes. */
#define MAX_PEERS 8

/* Fields common across all request types. */
#define REQUEST                \
    int type;                  \
    raft_time completion_time; \
    queue queue

/* Request type codes. */
enum { APPEND = 1, SEND, TRANSMIT, SNAPSHOT_PUT, SNAPSHOT_GET };

/* Abstract base type for an asynchronous request submitted to the stub I/o
 * implementation. */
struct request
{
    REQUEST;
};

/* Pending request to append entries to the log. */
struct append
{
    REQUEST;
    struct raft_io_append *req;
    const struct raft_entry *entries;
    unsigned n;
    unsigned start; /* Request timestamp. */
};

/* Pending request to send a message. */
struct send
{
    REQUEST;
    struct raft_io_send *req;
    struct raft_message message;
};

/* Pending request to store a snapshot. */
struct snapshot_put
{
    REQUEST;
    struct raft_io_snapshot_put *req;
    const struct raft_snapshot *snapshot;
};

/* Pending request to load a snapshot. */
struct snapshot_get
{
    REQUEST;
    struct raft_io_snapshot_get *req;
};

/* Message that has been written to the network and is waiting to be delivered
 * (or discarded). */
struct transmit
{
    REQUEST;
    struct raft_message message; /* Message to deliver */
    int timer;                   /* Deliver after this n of msecs. */
};

/* Information about a peer server. */
struct peer
{
    struct io *io;  /* The peer's I/O backend. */
    bool connected; /* Whether a connection is established. */
    bool saturated; /* Whether the established connection is saturated. */
};

/* Stub I/O implementation implementing all operations in-memory. */
struct io
{
    struct raft_io *io;  /* I/O object we're implementing. */
    unsigned index;      /* Fixture server index. */
    raft_time *time;     /* Global cluster time. */
    raft_time next_tick; /* Time the next tick should occurs. */

    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    struct raft_snapshot *snapshot; /* Latest snapshot */
    struct raft_entry *entries;     /* Array or persisted entries */
    size_t n;                       /* Size of the persisted entries array */

    /* Parameters passed via raft_io->init and raft_io->start */
    unsigned id;
    const char *address;
    unsigned tick_interval;
    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;

    /* Queue of pending asynchronous requests, whose callbacks still haven't
     * been fired. */
    queue requests;

    /* Peers connected to us. */
    struct peer peers[MAX_PEERS];
    unsigned n_peers;

    unsigned randomized_election_timeout; /* Value returned by io->random() */
    unsigned network_latency;             /* Milliseconds to deliver RPCs */
    unsigned disk_latency;                /* Milliseconds to perform disk I/O */

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
    } fault;

    bool drop[5];

    /* Counters of events that happened so far. */
    unsigned n_send[N_MESSAGE_TYPES];
    unsigned n_recv[N_MESSAGE_TYPES];
    unsigned n_append;
};

/* Advance the fault counters and return @true if an error should occurr. */
static bool ioFaultTick(struct io *s)
{
    if (s->fault.countdown < 0) {
        return false;
    }

    if (s->fault.countdown > 0) {
        s->fault.countdown--;
        return false;
    }

    assert(s->fault.countdown == 0);

    if (s->fault.n < 0) {
        /* Trigger the fault forever. */
        return true;
    }

    if (s->fault.n > 0) {
        /* Trigger the fault at least this time. */
        s->fault.n--;
        return true;
    }

    assert(s->fault.n == 0);

    /* We reached 'repeat' ticks, let's stop triggering the fault. */
    s->fault.countdown--;

    return false;
}

static int ioMethodInit(struct raft_io *io, unsigned id, const char *address)
{
    struct io *s;

    s = io->impl;

    s->id = id;
    s->address = address;

    return 0;
}

static int ioMethodStart(struct raft_io *io,
                         unsigned msecs,
                         raft_io_tick_cb tick_cb,
                         raft_io_recv_cb recv_cb)
{
    struct io *s;
    (void)msecs;
    assert(io != NULL);
    s = io->impl;
    if (ioFaultTick(s)) {
        return RAFT_IOERR;
    }
    s->tick_interval = msecs;
    s->tick_cb = tick_cb;
    s->recv_cb = recv_cb;
    s->next_tick = *s->time + s->tick_interval;
    return 0;
}

/* Release the memory used by the given message tramsmit object. */
static void ioDropTransmit(struct io *io, struct transmit *transmit)
{
    struct raft_message *message;
    (void)io;
    message = &transmit->message;

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            if (message->append_entries.entries != NULL) {
                raft_free(message->append_entries.entries[0].batch);
                raft_free(message->append_entries.entries);
            }
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_configuration_close(&message->install_snapshot.conf);
            raft_free(message->install_snapshot.data.base);
            break;
    }

    raft_free(transmit);
}

/* Flush an append entries request, appending its entries in the local in-memory
   log. */
static void ioFlushAppend(struct io *s, struct append *append)
{
    struct raft_entry *entries;
    unsigned i;

    /* Allocate an array for the old entries plus the new ones. */
    entries = raft_realloc(s->entries, (s->n + append->n) * sizeof *s->entries);
    assert(entries != NULL);

    /* Copy new entries into the new array. */
    memcpy(entries + s->n, append->entries, append->n * sizeof *entries);
    for (i = 0; i < append->n; i++) {
        struct raft_entry *entry = &entries[s->n + i];

        /* Make a copy of the actual entry data. */
        entry->buf.base = raft_malloc(entry->buf.len);
        assert(entry->buf.base != NULL);
        memcpy(entry->buf.base, append->entries[i].buf.base, entry->buf.len);
    }

    s->entries = entries;
    s->n += append->n;

    if (append->req->cb != NULL) {
        append->req->cb(append->req, 0);
    }
    free(append);
}

/* Copy all entries in @src into @dst. */
static void io_stub__copy_entries(const struct raft_entry *src,
                                  struct raft_entry **dst,
                                  unsigned n)
{
    size_t size = 0;
    void *batch;
    void *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }

    batch = raft_malloc(size);
    assert(batch != NULL);

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    assert(*dst != NULL);

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i] = src[i];

        (*dst)[i].buf.base = cursor;
        memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);

        (*dst)[i].batch = batch;

        cursor += src[i].buf.len;
    }
}

static void snapshot_copy(const struct raft_snapshot *s1,
                          struct raft_snapshot *s2)
{
    int rv;
    unsigned i;
    size_t size;
    void *cursor;

    raft_configuration_init(&s2->configuration);

    s2->term = s1->term;
    s2->index = s1->index;

    rv = configurationCopy(&s1->configuration, &s2->configuration);
    assert(rv == 0);

    size = 0;
    for (i = 0; i < s1->n_bufs; i++) {
        size += s1->bufs[i].len;
    }

    s2->bufs = raft_malloc(sizeof *s2->bufs);
    assert(s2->bufs != NULL);

    s2->bufs[0].base = raft_malloc(size);
    s2->bufs[0].len = size;
    assert(s2->bufs[0].base != NULL);

    cursor = s2->bufs[0].base;

    for (i = 0; i < s1->n_bufs; i++) {
        memcpy(cursor, s1->bufs[i].base, s1->bufs[i].len);
        cursor += s1->bufs[i].len;
    }

    s2->n_bufs = 1;

    return;
}

/* Flush a snapshot put request, copying the snapshot data. */
static void ioFlushSnapshotPut(struct io *s, struct snapshot_put *r)
{
    if (s->snapshot == NULL) {
        s->snapshot = raft_malloc(sizeof *s->snapshot);
        assert(s->snapshot != NULL);
    } else {
        snapshot__close(s->snapshot);
    }

    snapshot_copy(r->snapshot, s->snapshot);

    if (r->req->cb != NULL) {
        r->req->cb(r->req, 0);
    }
    raft_free(r);
}

/* Flush a snapshot get request, returning to the client a copy of the local
   snapshot (if any). */
static void ioFlushSnapshotGet(struct io *s, struct snapshot_get *r)
{
    struct raft_snapshot *snapshot = raft_malloc(sizeof *snapshot);
    assert(snapshot != NULL);
    snapshot_copy(s->snapshot, snapshot);
    r->req->cb(r->req, snapshot, 0);
    raft_free(r);
}

/* Search for the peer with the given ID. */
static struct peer *ioGetPeer(struct io *io, unsigned id)
{
    unsigned i;
    for (i = 0; i < io->n_peers; i++) {
        struct peer *peer = &io->peers[i];
        if (peer->io->id == id) {
            return peer;
        }
    }
    return NULL;
}

/* Flush a raft_io_send request, copying the message content into a new struct
 * transmit object and invoking the user callback. */
static void ioFlushSend(struct io *io, struct send *send)
{
    struct peer *peer;
    struct transmit *transmit;
    struct raft_message *src;
    struct raft_message *dst;
    int status;
    int rv;

    /* If the peer doesn't exist or was disconnected, fail the request. */
    peer = ioGetPeer(io, send->message.server_id);
    if (peer == NULL || !peer->connected) {
        status = RAFT_NOCONNECTION;
        goto out;
    }

    transmit = raft_malloc(sizeof *transmit);
    assert(transmit != NULL);

    transmit->type = TRANSMIT;
    transmit->completion_time = *io->time + io->network_latency;

    src = &send->message;
    dst = &transmit->message;

    QUEUE_PUSH(&io->requests, &transmit->queue);

    *dst = *src;
    switch (dst->type) {
        case RAFT_IO_APPEND_ENTRIES:
            /* Make a copy of the entries being sent */
            io_stub__copy_entries(src->append_entries.entries,
                                  &dst->append_entries.entries,
                                  src->append_entries.n_entries);
            dst->append_entries.n_entries = src->append_entries.n_entries;
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            raft_configuration_init(&dst->install_snapshot.conf);
            rv = configurationCopy(&src->install_snapshot.conf,
                                   &dst->install_snapshot.conf);
            dst->install_snapshot.data.base =
                raft_malloc(dst->install_snapshot.data.len);
            assert(dst->install_snapshot.data.base != NULL);
            assert(rv == 0);
            memcpy(dst->install_snapshot.data.base,
                   src->install_snapshot.data.base,
                   src->install_snapshot.data.len);
            break;
    }

    /* tracef("io: flush: %s", describeMessage(&send->message)); */
    io->n_send[send->message.type]++;
    status = 0;

out:
    if (send->req->cb != NULL) {
        send->req->cb(send->req, status);
    }

    raft_free(send);
}

/* Flush all requests in the queue. */
static void ioFlushAll(struct io *io)
{
    while (!QUEUE_IS_EMPTY(&io->requests)) {
        queue *head;
        struct request *r;

        head = QUEUE_HEAD(&io->requests);
        QUEUE_REMOVE(head);

        r = QUEUE_DATA(head, struct request, queue);
        switch (r->type) {
            case APPEND:
                ioFlushAppend(io, (struct append *)r);
                break;
            case SEND:
                ioFlushSend(io, (struct send *)r);
                break;
            case TRANSMIT:
                ioDropTransmit(io, (struct transmit *)r);
                break;
            case SNAPSHOT_PUT:
                ioFlushSnapshotPut(io, (struct snapshot_put *)r);
                break;
            case SNAPSHOT_GET:
                ioFlushSnapshotGet(io, (struct snapshot_get *)r);
                break;
            default:
                assert(0);
        }
    }
}

static int ioMethodClose(struct raft_io *io, void (*cb)(struct raft_io *io))
{
    struct io *s;
    size_t i;

    s = io->impl;

    for (i = 0; i < s->n; i++) {
        struct raft_entry *entry = &s->entries[i];
        raft_free(entry->buf.base);
    }

    if (s->entries != NULL) {
        raft_free(s->entries);
    }

    if (s->snapshot != NULL) {
        snapshot__close(s->snapshot);
        raft_free(s->snapshot);
    }

    if (cb != NULL) {
        cb(io);
    }

    return 0;
}

static int ioMethodLoad(struct raft_io *io,
                        raft_term *term,
                        unsigned *voted_for,
                        struct raft_snapshot **snapshot,
                        raft_index *start_index,
                        struct raft_entry **entries,
                        size_t *n_entries)
{
    struct io *s;
    size_t i;
    void *batch;
    void *cursor;
    size_t size = 0; /* Size of the batch */
    int rv;

    s = io->impl;

    if (ioFaultTick(s)) {
        return RAFT_IOERR;
    }

    *term = s->term;
    *voted_for = s->voted_for;
    *start_index = 1;

    if (s->n == 0) {
        *entries = NULL;
        *n_entries = 0;
        goto snapshot;
    }

    /* Make a copy of the persisted entries, storing their data into a single
     * batch. */
    *n_entries = s->n;
    *entries = raft_calloc(s->n, sizeof **entries);
    if (*entries == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    for (i = 0; i < s->n; i++) {
        size += s->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_entries_alloc;
    }

    cursor = batch;

    for (i = 0; i < s->n; i++) {
        struct raft_entry *entry = &(*entries)[i];
        memcpy(cursor, s->entries[i].buf.base, s->entries[i].buf.len);

        entry->term = s->entries[i].term;
        entry->type = s->entries[i].type;
        entry->buf.base = cursor;
        entry->buf.len = s->entries[i].buf.len;
        entry->batch = batch;

        cursor += entry->buf.len;
    }

snapshot:
    if (s->snapshot != NULL) {
        *snapshot = raft_malloc(sizeof **snapshot);
        assert(*snapshot != NULL);
        snapshot_copy(s->snapshot, *snapshot);
        *start_index = (*snapshot)->index + 1;
    } else {
        *snapshot = NULL;
    }

    return 0;

err_after_entries_alloc:
    raft_free(*entries);

err:
    assert(rv != 0);
    return rv;
}

static int ioMethodBootstrap(struct raft_io *io,
                             const struct raft_configuration *conf)
{
    struct io *s;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    s = io->impl;

    if (ioFaultTick(s)) {
        return RAFT_IOERR;
    }

    if (s->term != 0) {
        return RAFT_CANTBOOTSTRAP;
    }

    assert(s->voted_for == 0);
    assert(s->snapshot == NULL);
    assert(s->entries == NULL);
    assert(s->n == 0);

    /* Encode the given configuration. */
    rv = configurationEncode(conf, &buf);
    if (rv != 0) {
        return rv;
    }

    entries = raft_calloc(1, sizeof *s->entries);
    if (entries == NULL) {
        return RAFT_NOMEM;
    }

    entries[0].term = 1;
    entries[0].type = RAFT_CHANGE;
    entries[0].buf = buf;

    s->term = 1;
    s->voted_for = 0;
    s->snapshot = NULL;
    s->entries = entries;
    s->n = 1;

    return 0;
}

static int ioMethodSetTerm(struct raft_io *io, const raft_term term)
{
    struct io *s;

    s = io->impl;

    if (ioFaultTick(s)) {
        return RAFT_IOERR;
    }

    s->term = term;
    s->voted_for = 0;

    return 0;
}

static int ioMethodSetVote(struct raft_io *raft_io, const unsigned server_id)
{
    struct io *io;
    io = raft_io->impl;
    if (ioFaultTick(io)) {
        return RAFT_IOERR;
    }
    io->voted_for = server_id;
    tracef("io: set vote: %d %d", server_id, io->index);
    return 0;
}

static int ioMethodAppend(struct raft_io *raft_io,
                          struct raft_io_append *req,
                          const struct raft_entry entries[],
                          unsigned n,
                          raft_io_append_cb cb)
{
    struct io *io;
    struct append *r;

    io = raft_io->impl;

    if (ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = APPEND;
    r->completion_time = *io->time + io->disk_latency;
    r->req = req;
    r->entries = entries;
    r->n = n;

    req->cb = cb;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static int ioMethodTruncate(struct raft_io *io, raft_index index)
{
    struct io *s;
    size_t n;
    raft_index start_index;

    s = io->impl;

    if (s->snapshot == NULL) {
        start_index = 1;
    } else {
        start_index = s->snapshot->index;
    }

    assert(index >= start_index);

    if (ioFaultTick(s)) {
        return RAFT_IOERR;
    }

    n = index - 1; /* Number of entries left after truncation */

    if (n > 0) {
        struct raft_entry *entries;

        /* Create a new array of entries holding the non-truncated entries */
        entries = raft_malloc(n * sizeof *entries);
        if (entries == NULL) {
            return RAFT_NOMEM;
        }
        memcpy(entries, s->entries, n * sizeof *s->entries);

        /* Release any truncated entry */
        if (s->entries != NULL) {
            size_t i;
            for (i = n; i < s->n; i++) {
                raft_free(s->entries[i].buf.base);
            }
            raft_free(s->entries);
        }
        s->entries = entries;
    } else {
        /* Release everything we have */
        if (s->entries != NULL) {
            size_t i;
            for (i = 0; i < s->n; i++) {
                raft_free(s->entries[i].buf.base);
            }
            raft_free(s->entries);
            s->entries = NULL;
        }
    }

    s->n = n;

    return 0;
}

static int ioMethodSnapshotPut(struct raft_io *raft_io,
                               struct raft_io_snapshot_put *req,
                               const struct raft_snapshot *snapshot,
                               raft_io_snapshot_put_cb cb)
{
    struct io *io;
    struct snapshot_put *r;
    io = raft_io->impl;

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_PUT;
    r->req = req;
    r->req->cb = cb;
    r->snapshot = snapshot;
    r->completion_time = *io->time + io->disk_latency;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static int ioMethodSnapshotGet(struct raft_io *raft_io,
                               struct raft_io_snapshot_get *req,
                               raft_io_snapshot_get_cb cb)
{
    struct io *io;
    struct snapshot_get *r;
    io = raft_io->impl;

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_GET;
    r->req = req;
    r->req->cb = cb;
    r->completion_time = *io->time + io->disk_latency;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static raft_time ioMethodTime(struct raft_io *io)
{
    struct io *s;
    s = io->impl;
    return *s->time;
}

static int ioMethodRandom(struct raft_io *raft_io, int min, int max)
{
    struct io *io;
    (void)min;
    (void)max;
    io = raft_io->impl;
    return io->randomized_election_timeout;
}

static void ioMethodEmit(struct raft_io *io, int level, const char *format, ...)
{
    struct io *s;
    va_list args;
    va_start(args, format);
    s = io->impl;
    emitToStream(stderr, s->id, *s->time, level, format, args);
    va_end(args);
}

/**
 * Queue up a request which will be processed later, when io_stub_flush()
 * is invoked.
 */
static int ioMethodSend(struct raft_io *raft_io,
                        struct raft_io_send *req,
                        const struct raft_message *message,
                        raft_io_send_cb cb)
{
    struct io *io;
    struct send *r;

    io = raft_io->impl;

    tracef("io: send: %s to server %d", describeMessage(message),
           message->server_id);

    if (ioFaultTick(io)) {
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SEND;
    r->req = req;
    r->message = *message;
    r->req->cb = cb;

    /* TODO: simulate the presence of an OS send buffer, whose available size
     * might delay the completion of send requests */
    r->completion_time = *io->time;

    QUEUE_PUSH(&io->requests, &r->queue);

    return 0;
}

static void ioReceive(struct io *io, struct raft_message *message)
{
    tracef("io: recv: %s from server %d", describeMessage(message),
           message->server_id);
    io->recv_cb(io->io, message);
    io->n_recv[message->type]++;
}

static void ioDeliverTransmit(struct io *io, struct transmit *transmit)
{
    struct raft_message *message = &transmit->message;
    struct peer *peer; /* Destination peer */

    /* If this message type is in the drop list, let's discard it */
    if (io->drop[message->type - 1]) {
        ioDropTransmit(io, transmit);
        return;
    }

    peer = ioGetPeer(io, message->server_id);

    /* We don't have any peer with this ID or it's disconnected or if the
     * connection is saturated, let's drop the message */
    if (peer == NULL || !peer->connected || peer->saturated) {
        ioDropTransmit(io, transmit);
        return;
    }

    /* Update the message object with our details. */
    message->server_id = io->id;
    message->server_address = io->address;

    ioReceive(peer->io, message);
    raft_free(transmit);
}

/* Connect @io to @other, enabling delivery of messages sent from @io to @other.
 */
static void ioConnect(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    s = io->impl;
    s_other = other->impl;
    assert(s->n_peers < MAX_PEERS);
    s->peers[s->n_peers].io = s_other;
    s->peers[s->n_peers].connected = true;
    s->peers[s->n_peers].saturated = false;
    s->n_peers++;
}

/* Return whether the connection with the given peer is saturated. */
static bool ioSaturated(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    return peer != NULL && peer->saturated;
}

/* Disconnect @io and @other, causing calls to @io->send() to fail
 * asynchronously when sending messages to @other. */
static void ioDisconnect(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    assert(peer != NULL);
    peer->connected = false;
}

/* Reconnect @io and @other. */
static void ioReconnect(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    assert(peer != NULL);
    peer->connected = true;
}

/* Saturate the connection from @io to @other, causing messages sent from @io to
 * @other to be dropped. */
static void ioSaturate(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    assert(peer != NULL && peer->connected);
    peer->saturated = true;
}

/* Desaturate the connection from @io to @other, re-enabling delivery of
 * messages sent from @io to @other. */
static void ioDesaturate(struct raft_io *io, struct raft_io *other)
{
    struct io *s;
    struct io *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = ioGetPeer(s, s_other->id);
    assert(peer != NULL && peer->connected);
    peer->saturated = false;
}

/* Enable or disable silently dropping all outgoing messages of type @type. */
void ioDrop(struct io *io, int type, bool flag)
{
    io->drop[type - 1] = flag;
}

static int ioInit(struct raft_io *raft_io, unsigned index, raft_time *time)
{
    struct io *io;
    io = raft_malloc(sizeof *io);
    assert(io != NULL);
    io->io = raft_io;
    io->index = index;
    io->time = time;
    io->term = 0;
    io->voted_for = 0;
    io->snapshot = NULL;
    io->entries = NULL;
    io->n = 0;
    QUEUE_INIT(&io->requests);
    io->n_peers = 0;
    io->randomized_election_timeout = ELECTION_TIMEOUT + index * 100;
    io->network_latency = NETWORK_LATENCY;
    io->disk_latency = DISK_LATENCY;
    io->fault.countdown = -1;
    io->fault.n = -1;
    memset(io->drop, 0, sizeof io->drop);
    memset(io->n_send, 0, sizeof io->n_send);
    memset(io->n_recv, 0, sizeof io->n_recv);
    io->n_append = 0;

    raft_io->impl = io;
    raft_io->init = ioMethodInit;
    raft_io->start = ioMethodStart;
    raft_io->close = ioMethodClose;
    raft_io->load = ioMethodLoad;
    raft_io->bootstrap = ioMethodBootstrap;
    raft_io->set_term = ioMethodSetTerm;
    raft_io->set_vote = ioMethodSetVote;
    raft_io->append = ioMethodAppend;
    raft_io->truncate = ioMethodTruncate;
    raft_io->send = ioMethodSend;
    raft_io->snapshot_put = ioMethodSnapshotPut;
    raft_io->snapshot_get = ioMethodSnapshotGet;
    raft_io->time = ioMethodTime;
    raft_io->random = ioMethodRandom;
    raft_io->emit = ioMethodEmit;

    return 0;
}

/* Release all memory held by the given stub I/O implementation. */
void ioClose(struct raft_io *io)
{
    raft_free(io->impl);
}

static int serverInit(struct raft_fixture *f, unsigned i, struct raft_fsm *fsm)
{
    int rc;
    struct raft_fixture_server *s = &f->servers[i];
    s->alive = true;
    s->id = i + 1;
    sprintf(s->address, "%u", s->id);
    rc = ioInit(&s->io, i, &f->time);
    if (rc != 0) {
        return rc;
    }
    rc = raft_init(&s->raft, &s->io, fsm, s->id, s->address);
    if (rc != 0) {
        return rc;
    }
    raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
    return 0;
}

static void serverClose(struct raft_fixture_server *s)
{
    raft_close(&s->raft, NULL);
    ioClose(&s->io);
}

/* Connect the server with the given index to all others */
static void serverConnectToAll(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        if (i == j) {
            continue;
        }
        ioConnect(io1, io2);
    }
}

int raft_fixture_init(struct raft_fixture *f, unsigned n, struct raft_fsm *fsms)
{
    unsigned i;
    int rc;
    assert(n >= 1);

    f->time = 0;
    f->n = n;

    /* Initialize all servers */
    for (i = 0; i < n; i++) {
        rc = serverInit(f, i, &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        serverConnectToAll(f, i);
    }

    logInit(&f->log);
    f->commit_index = 0;
    f->hook = NULL;

    return 0;
}

void raft_fixture_close(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct io *io = f->servers[i].io.impl;
        ioFlushAll(io);
    }
    for (i = 0; i < f->n; i++) {
        serverClose(&f->servers[i]);
    }
    logClose(&f->log);
}

int raft_fixture_configuration(struct raft_fixture *f,
                               unsigned n_voting,
                               struct raft_configuration *configuration)
{
    unsigned i;

    assert(f->n > 0);
    assert(n_voting > 0);
    assert(n_voting <= f->n);

    raft_configuration_init(configuration);

    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s;
        bool voting = i < n_voting;
        int rc;
        s = &f->servers[i];
        rc = raft_configuration_add(configuration, s->id, s->address, voting);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int raft_fixture_bootstrap(struct raft_fixture *f,
                           struct raft_configuration *configuration)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        int rc;
        rc = raft_bootstrap(raft, configuration);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int raft_fixture_start(struct raft_fixture *f)
{
    unsigned i;
    int rc;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rc = raft_start(&s->raft);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

unsigned raft_fixture_n(struct raft_fixture *f)
{
    return f->n;
}

raft_time raft_fixture_time(struct raft_fixture *f)
{
    return f->time;
}

struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return &f->servers[i].raft;
}

bool raft_fixture_alive(struct raft_fixture *f, unsigned i)
{
    assert(i < f->n);
    return f->servers[i].alive;
}

unsigned raft_fixture_leader_index(struct raft_fixture *f)
{
    if (f->leader_id != 0) {
        return f->leader_id - 1;
    }
    return f->n;
}

unsigned raft_fixture_voted_for(struct raft_fixture *f, unsigned i)
{
    struct io *io = f->servers[i].io.impl;
    return io->voted_for;
}

/* Update the leader and check for election safety.
 *
 * From figure 3.2:
 *
 *   Election Safety -> At most one leader can be elected in a given
 *   term.
 *
 * Return true if the current leader turns out to be different from the one at
 * the time this function was called.
 */
static bool updateLeader(struct raft_fixture *f)
{
    unsigned leader_id = 0;
    unsigned leader_i = 0;
    raft_term leader_term = 0;
    unsigned i;
    bool changed;

    for (i = 0; i < f->n; i++) {
        struct raft *raft = raft_fixture_get(f, i);
        unsigned j;

        if (!raft_fixture_alive(f, i)) {
            continue;
        }

        if (raft_state(raft) == RAFT_LEADER) {
            /* No other server is leader for this term. */
            for (j = 0; j < f->n; j++) {
                struct raft *other = raft_fixture_get(f, j);

                if (other->id == raft->id) {
                    continue;
                }

                if (other->state == RAFT_LEADER) {
                    if (other->current_term == raft->current_term) {
                        fprintf(
                            stderr,
                            "server %u and %u are both leaders in term %llu",
                            raft->id, other->id, raft->current_term);
                        abort();
                    }
                }
            }

            if (raft->current_term > leader_term) {
                leader_id = raft->id;
                leader_i = i;
                leader_term = raft->current_term;
            }
        }
    }

    /* Check that the leader is stable, in the sense that it has been
     * acknowledged by all alive servers connected to it, and those servers
     * together with the leader form a majority. */
    if (leader_id != 0) {
        unsigned n_acks = 0;
        bool acked = true;

        for (i = 0; i < f->n; i++) {
            struct raft *raft = raft_fixture_get(f, i);
            if (i == leader_i) {
                continue;
            }
            if (!raft_fixture_alive(f, i) ||
                raft_fixture_saturated(f, leader_i, i)) {
                /* This server is not alive or not connected to the leader, so
                 * don't count it in for stability. */
                continue;
            }

            if (raft->current_term != leader_term) {
                acked = false;
                break;
            }

            if (raft->state != RAFT_FOLLOWER) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id == 0) {
                acked = false;
                break;
            }

            if (raft->follower_state.current_leader.id != leader_id) {
                acked = false;
                break;
            }

            n_acks++;
        }

        if (!acked || n_acks < (f->n / 2)) {
            leader_id = 0;
        }
    }

    changed = leader_id != f->leader_id;
    f->leader_id = leader_id;

    return changed;
}

/* Check for leader append-only.
 *
 * From figure 3.2:
 *
 *   Leader Append-Only -> A leader never overwrites or deletes entries in its
 *   own log; it only appends new entries.
 */
static void checkLeaderAppendOnly(struct raft_fixture *f)
{
    struct raft *raft;
    raft_index index;
    raft_index last = logLastIndex(&f->log);

    /* If the cached log is empty it means there was no leader before. */
    if (last == 0) {
        return;
    }

    /* If there's no new leader, just return. */
    if (f->leader_id == 0) {
        return;
    }

    raft = raft_fixture_get(f, f->leader_id - 1);
    last = logLastIndex(&f->log);

    for (index = 1; index <= last; index++) {
        const struct raft_entry *entry1;
        const struct raft_entry *entry2;

        entry1 = logGet(&f->log, index);
        entry2 = logGet(&raft->log, index);

        assert(entry1 != NULL);

        /* Check if the entry was snapshotted. */
        if (entry2 == NULL) {
            assert(raft->log.snapshot.last_index >= index);
            continue;
        }

        /* TODO: check other entry types too. */
        if (entry1->type != RAFT_COMMAND) {
            continue;
        }

        /* Entry was not overwritten. TODO: check all content. */
        assert(entry1->term == entry2->term);
        assert(*(uint32_t *)entry1->buf.base == *(uint32_t *)entry2->buf.base);
    }
}

/* Make a copy of the the current leader log, in order to perform the Leader
 * Append-Only check at the next iteration. */
static void copyLeaderLog(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rc;

    logClose(&f->log);
    logInit(&f->log);

    rc = logAcquire(&raft->log, 1, &entries, &n);
    assert(rc == 0);

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;
        buf.len = entry->buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entry->buf.base, buf.len);
        rc = logAppend(&f->log, entry->term, entry->type, &buf, NULL);
        assert(rc == 0);
    }

    logRelease(&raft->log, 1, entries, n);
}

/* Update the commit index to match the one from the current leader. */
static void updateCommitIndex(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    if (raft->commit_index > f->commit_index) {
        f->commit_index = raft->commit_index;
    }
}

/* Return the lowest tick time across all servers, along with the associated
 * server index */
static void getLowestTickTime(struct raft_fixture *f, raft_time *t, unsigned *i)
{
    unsigned j;
    *t = -1 /* Maximum value */;
    for (j = 0; j < f->n; j++) {
        struct io *io = f->servers[j].io.impl;
        if (io->next_tick < *t) {
            *t = io->next_tick;
            *i = j;
        }
    }
}

/* Return the completion time of the request with the lowest completion time
 * across all servers, along with the associated server index. */
static void getLowestRequestCompletionTime(struct raft_fixture *f,
                                           raft_time *t,
                                           unsigned *i)
{
    unsigned j;
    *t = -1 /* Maximum value */;
    for (j = 0; j < f->n; j++) {
        struct io *io = f->servers[j].io.impl;
        queue *head;
        QUEUE_FOREACH(head, &io->requests)
        {
            struct request *r = QUEUE_DATA(head, struct request, queue);
            if (r->completion_time < *t) {
                *t = r->completion_time;
                *i = j;
            }
        }
    }
}

/* Fire the tick callback of the i'th server. */
static void fireTick(struct raft_fixture *f, unsigned i)
{
    struct io *io = f->servers[i].io.impl;
    f->time = io->next_tick;
    f->event.server_index = i;
    f->event.type = RAFT_FIXTURE_TICK;
    io->next_tick += io->tick_interval;
    io->tick_cb(io->io);
}

/* Complete the first request with completion time @t on the @i'th server. */
static void completeRequest(struct raft_fixture *f, unsigned i, raft_time t)
{
    struct io *io = f->servers[i].io.impl;
    queue *head;
    struct request *r;
    bool found = false;
    f->time = t;
    f->event.server_index = i;
    QUEUE_FOREACH(head, &io->requests)
    {
        r = QUEUE_DATA(head, struct request, queue);
        if (r->completion_time == t) {
            found = true;
            break;
        }
    }
    assert(found);
    QUEUE_REMOVE(head);
    switch (r->type) {
        case APPEND:
            ioFlushAppend(io, (struct append *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        case SEND:
            ioFlushSend(io, (struct send *)r);
            f->event.type = RAFT_FIXTURE_NETWORK;
            break;
        case TRANSMIT:
            ioDeliverTransmit(io, (struct transmit *)r);
            f->event.type = RAFT_FIXTURE_NETWORK;
            break;
        case SNAPSHOT_PUT:
            ioFlushSnapshotPut(io, (struct snapshot_put *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        case SNAPSHOT_GET:
            ioFlushSnapshotGet(io, (struct snapshot_get *)r);
            f->event.type = RAFT_FIXTURE_DISK;
            break;
        default:
            assert(0);
    }
}

struct raft_fixture_event *raft_fixture_step(struct raft_fixture *f)
{
    raft_time tick_time;
    raft_time completion_time;
    unsigned i;
    unsigned j;

    getLowestTickTime(f, &tick_time, &i);
    getLowestRequestCompletionTime(f, &completion_time, &j);

    if (tick_time < completion_time ||
        (tick_time == completion_time && i <= j)) {
        fireTick(f, i);
    } else {
        completeRequest(f, j, completion_time);
    }

    /* If the leader has not changed check the Leader Append-Only
     * guarantee. */
    if (!updateLeader(f)) {
        checkLeaderAppendOnly(f);
    }

    /* If we have a leader, update leader-related state . */
    if (f->leader_id != 0) {
        copyLeaderLog(f);
        updateCommitIndex(f);
    }

    if (f->hook != NULL) {
        f->hook(f, &f->event);
    }

    return &f->event;
}

struct raft_fixture_event *raft_fixture_step_n(struct raft_fixture *f,
                                               unsigned n)
{
    unsigned i;
    assert(n > 0);
    for (i = 0; i < n - 1; i++) {
        raft_fixture_step(f);
    }
    return raft_fixture_step(f);
}

bool raft_fixture_step_until(struct raft_fixture *f,
                             bool (*stop)(struct raft_fixture *f, void *arg),
                             void *arg,
                             unsigned max_msecs)
{
    unsigned start = f->time;
    while (!stop(f, arg) && (f->time - start) < max_msecs) {
        raft_fixture_step(f);
    }
    return f->time - start < max_msecs;
}

static bool spin(struct raft_fixture *f, void *arg)
{
    (void)f;
    (void)arg;
    return false;
}

void raft_fixture_step_until_elapsed(struct raft_fixture *f, unsigned msecs)
{
    raft_fixture_step_until(f, spin, NULL, msecs);
}

static bool hasLeader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id != 0;
}

bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
                                        unsigned max_msecs)
{
    return raft_fixture_step_until(f, hasLeader, NULL, max_msecs);
}

static bool hasNoLeader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id == 0;
}

bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
                                           unsigned max_msecs)
{
    return raft_fixture_step_until(f, hasNoLeader, NULL, max_msecs);
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void dropAllExcept(struct raft_fixture *f,
                          int type,
                          bool flag,
                          unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_fixture_server *s = &f->servers[j];
        if (j == i) {
            continue;
        }
        ioDrop(s->io.impl, type, flag);
    }
}

/* Set the randomized election timeout of the given server to the minimum value
 * compatible with its current state and timers. */
static void minimizeRandomizedElectionTimeout(struct raft_fixture *f,
                                              unsigned i)
{
    struct raft *raft = &f->servers[i].raft;
    raft_time now = raft->io->time(raft->io);
    unsigned timeout = raft->election_timeout;
    assert(raft->state == RAFT_FOLLOWER);

    /* If the minimum election timeout value would make the timer expire in the
     * past, cap it. */
    if (now - raft->election_timer_start > timeout) {
        timeout = now - raft->election_timer_start;
    }

    raft->follower_state.randomized_election_timeout = timeout;
}

/* Set the randomized election timeout to the maximum value on all servers
 * except the given one. */
static void maximizeAllRandomizedElectionTimeoutsExcept(struct raft_fixture *f,
                                                        unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft *raft = &f->servers[j].raft;
        unsigned timeout = raft->election_timeout * 2;
        if (j == i) {
            continue;
        }
        assert(raft->state == RAFT_FOLLOWER);
        raft->follower_state.randomized_election_timeout = timeout;
    }
}

void raft_fixture_hook(struct raft_fixture *f, raft_fixture_event_cb hook)
{
    f->hook = hook;
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
    struct raft *raft = raft_fixture_get(f, i);
    unsigned j;

    /* Make sure there's currently no leader. */
    assert(f->leader_id == 0);

    /* Make sure that the given server is voting. */
    assert(configurationGet(&raft->configuration, raft->id)->voting);

    /* Make sure all servers are currently followers. */
    for (j = 0; j < f->n; j++) {
        assert(raft_state(&f->servers[j].raft) == RAFT_FOLLOWER);
    }

    /* Pretend that the last randomized election timeout was set at the maximum
     * value on all server expect the one to be elected, which is instead set to
     * the minimum possible value compatible with its current state. */
    minimizeRandomizedElectionTimeout(f, i);
    maximizeAllRandomizedElectionTimeoutsExcept(f, i);

    raft_fixture_step_until_has_leader(f, ELECTION_TIMEOUT * 20);
    assert(f->leader_id == raft->id);
}

void raft_fixture_depose(struct raft_fixture *f)
{
    unsigned leader_i;
    unsigned i;

    /* Make sure there's a leader. */
    assert(f->leader_id != 0);
    leader_i = f->leader_id - 1;
    assert(raft_state(&f->servers[leader_i].raft) == RAFT_LEADER);

    /* Make sure all server have a default election timeout. */
    for (i = 0; i < f->n; i++) {
        assert(f->servers[i].raft.election_timeout == ELECTION_TIMEOUT);
    }

    /* Set a very large election timeout on all followers, to prevent them from
     * starting an election. */
    maximizeAllRandomizedElectionTimeoutsExcept(f, leader_i);

    /* Prevent all servers from sending append entries results, so the leader
     * will eventually step down. */
    dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader_i);

    raft_fixture_step_until_has_no_leader(f, ELECTION_TIMEOUT * 3);
    assert(f->leader_id == 0);

    dropAllExcept(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader_i);
}

struct step_apply
{
    unsigned i;
    raft_index index;
};

static bool has_applied_index(struct raft_fixture *f, void *arg)
{
    struct step_apply *apply = (struct step_apply *)arg;
    struct raft *raft;
    unsigned n = 0;
    unsigned i;

    if (apply->i < f->n) {
        raft = raft_fixture_get(f, apply->i);
        return raft_last_applied(raft) >= apply->index;
    }

    for (i = 0; i < f->n; i++) {
        raft = raft_fixture_get(f, i);
        if (raft_last_applied(raft) >= apply->index) {
            n++;
        }
    }
    return n == f->n;
}

bool raft_fixture_step_until_applied(struct raft_fixture *f,
                                     unsigned i,
                                     raft_index index,
                                     unsigned max_msecs)
{
    struct step_apply apply = {i, index};
    return raft_fixture_step_until(f, has_applied_index, &apply, max_msecs);
}

struct step_state
{
    unsigned i;
    int state;
};

static bool has_state(struct raft_fixture *f, void *arg)
{
    struct step_state *target = (struct step_state *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft_state(raft) == target->state;
}

bool raft_fixture_step_until_state_is(struct raft_fixture *f,
                                      unsigned i,
                                      int state,
                                      unsigned max_msecs)
{
    struct step_state target = {i, state};
    return raft_fixture_step_until(f, has_state, &target, max_msecs);
}

struct step_term
{
    unsigned i;
    raft_term term;
};

static bool has_term(struct raft_fixture *f, void *arg)
{
    struct step_term *target = (struct step_term *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft->current_term == target->term;
}

bool raft_fixture_step_until_term_is(struct raft_fixture *f,
                                     unsigned i,
                                     raft_term term,
                                     unsigned max_msecs)
{
    struct step_term target = {i, term};
    return raft_fixture_step_until(f, has_term, &target, max_msecs);
}

struct step_vote
{
    unsigned i;
    unsigned j;
};

static bool has_voted_for(struct raft_fixture *f, void *arg)
{
    struct step_vote *target = (struct step_vote *)arg;
    struct raft *raft;
    raft = raft_fixture_get(f, target->i);
    return raft->voted_for == target->j + 1;
}

bool raft_fixture_step_until_voted_for(struct raft_fixture *f,
                                       unsigned i,
                                       unsigned j,
                                       unsigned max_msecs)
{
    struct step_vote target = {i, j};
    return raft_fixture_step_until(f, has_voted_for, &target, max_msecs);
}

struct step_deliver
{
    unsigned i;
    unsigned j;
};

static bool has_delivered(struct raft_fixture *f, void *arg)
{
    struct step_deliver *target = (struct step_deliver *)arg;
    struct raft *raft;
    struct io *io;
    struct raft_message *message;
    raft = raft_fixture_get(f, target->i);
    io = raft->io->impl;
    queue *head;
    QUEUE_FOREACH(head, &io->requests)
    {
        struct request *r;
        r = QUEUE_DATA(head, struct request, queue);
        message = NULL;
        switch (r->type) {
            case SEND:
                message = &((struct send *)r)->message;
                break;
            case TRANSMIT:
                message = &((struct transmit *)r)->message;
                break;
        }
        if (message != NULL && message->server_id == target->j + 1) {
            return false;
        }
    }
    return true;
}

bool raft_fixture_step_until_delivered(struct raft_fixture *f,
                                       unsigned i,
                                       unsigned j,
                                       unsigned max_msecs)
{
    struct step_deliver target = {i, j};
    return raft_fixture_step_until(f, has_delivered, &target, max_msecs);
}

void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioDisconnect(io1, io2);
}

void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j) {
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioReconnect(io1, io2);
}

void raft_fixture_saturate(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioSaturate(io1, io2);
}

static void disconnectFromAll(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        if (j == i) {
            continue;
        }
        raft_fixture_saturate(f, i, j);
        raft_fixture_saturate(f, j, i);
    }
}

bool raft_fixture_saturated(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    return ioSaturated(io1, io2);
}

void raft_fixture_desaturate(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    ioDesaturate(io1, io2);
}

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
    disconnectFromAll(f, i);
    f->servers[i].alive = false;
}

int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm)
{
    unsigned i;
    unsigned j;
    int rc;
    i = f->n;
    f->n++;

    rc = serverInit(f, i, fsm);
    if (rc != 0) {
        return rc;
    }

    serverConnectToAll(f, i);
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        ioConnect(io2, io1);
    }

    return 0;
}

void raft_fixture_set_randomized_election_timeout(struct raft_fixture *f,
                                                  unsigned i,
                                                  unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->randomized_election_timeout = msecs;
}

void raft_fixture_set_network_latency(struct raft_fixture *f,
                                      unsigned i,
                                      unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->network_latency = msecs;
}

void raft_fixture_set_disk_latency(struct raft_fixture *f,
                                   unsigned i,
                                   unsigned msecs)
{
    struct io *io = f->servers[i].io.impl;
    io->disk_latency = msecs;
}

void raft_fixture_set_term(struct raft_fixture *f, unsigned i, raft_term term)
{
    struct io *io = f->servers[i].io.impl;
    io->term = term;
}

void raft_fixture_set_snapshot(struct raft_fixture *f,
                               unsigned i,
                               struct raft_snapshot *snapshot)
{
    struct io *io = f->servers[i].io.impl;
    io->snapshot = snapshot;
}

void raft_fixture_set_entries(struct raft_fixture *f,
                              unsigned i,
                              struct raft_entry *entries,
                              unsigned n)
{
    struct io *io = f->servers[i].io.impl;
    io->entries = entries;
    io->n = n;
}

void raft_fixture_add_entry(struct raft_fixture *f,
                            unsigned i,
                            struct raft_entry *entry)
{
    struct io *io = f->servers[i].io.impl;
    struct raft_entry *entries;
    entries = raft_realloc(io->entries, (io->n + 1) * sizeof *entries);
    assert(entries != NULL);
    entries[io->n] = *entry;
    io->entries = entries;
    io->n++;
}

void raft_fixture_io_fault(struct raft_fixture *f,
                           unsigned i,
                           int delay,
                           int repeat)
{
    struct io *io = f->servers[i].io.impl;
    io->fault.countdown = delay;
    io->fault.n = repeat;
}

unsigned raft_fixture_n_send(struct raft_fixture *f, unsigned i, int type)
{
    struct io *io = f->servers[i].io.impl;
    return io->n_send[type];
}

unsigned raft_fixture_n_recv(struct raft_fixture *f, unsigned i, int type)
{
    struct io *io = f->servers[i].io.impl;
    return io->n_recv[type];
}
