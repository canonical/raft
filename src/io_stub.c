#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"
#include "../include/raft/io_stub.h"

#include "assert.h"
#include "configuration.h"
#include "queue.h"
#include "snapshot.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(S, MSG, ...) raft_debugf(S->logger, MSG, __VA_ARGS__)
static const char *message_names[6] = {
    NULL,           "append entries",      "append entries result",
    "request vote", "request vote result", "install snapshot"};

#else
#define tracef(S, MSG, ...)
#endif

/* Maximum number of messages inflight on the network. This should be enough for
 * testing purposes. */
#define MAX_TRANSMIT 64

/* Maximum number of peer stub instances connected to a certain stub
 * instance. This should be enough for testing purposes. */
#define MAX_PEERS 8

#define REQUEST \
    int type;   \
    raft__queue queue

/* Request types. */
enum { APPEND = 1, SEND, SNAPSHOT_PUT, SNAPSHOT_GET };

/* Base type for an asynchronous request submitted to the stub I/o
 * implementation. */
struct request
{
    REQUEST;
};

/* Pending request to append entries to the log. */
struct append
{
    REQUEST;
    const struct raft_entry *entries;
    unsigned n;
    void *data;
    void (*cb)(void *data, int status);
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
 * (or discarded) */
struct transmit
{
    struct raft_message message; /* Message to deliver */
    int timer;                   /* Deliver after this n of msecs. */
    raft__queue queue;
};

/* Information about a peer server. */
struct peer
{
    struct io_stub *s;
    bool connected;
};

/**
 * Stub I/O implementation implementing all operations in-memory.
 */
struct io_stub
{
    /* Elapsed time since the backend was started. */
    raft_time time;

    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    struct raft_snapshot *snapshot; /* Latest snapshot */
    struct raft_entry *entries;     /* Array or persisted entries */
    size_t n;                       /* Size of the persisted entries array */

    /* Parameters passed via raft_io->init */
    struct raft_logger *logger;
    unsigned id;
    const char *address;
    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;

    /* Queue of pending asynchronous requests, whose callbacks still haven't
     * been fired. */
    raft__queue requests;

    unsigned n_append;       /* Number of pending append entries requests */
    unsigned n_send;         /* Number of pending send message requests */
    unsigned n_snapshot_put; /* Number of pending snapshot put requests */
    unsigned n_snapshot_get; /* Number of pending snapshot get requests */

    /* Queue of messages that have been written to the network, i.e. the
     * callback of the associated raft_io->send() request has been fired. */
    raft__queue transmit;
    unsigned n_transmit;

    /* Peers connected to us. */
    struct peer peers[MAX_PEERS];
    unsigned n_peers;

    /* Minimum and maximum values for the random latency assigned to messages in
     * the transmit queue. It models how much time the network needs to transmit
     * a certain message. This is zero by default, meaning that delivery is
     * instantaneous. */
    unsigned min_latency;
    unsigned max_latency;

    int (*randint)(int, int); /* Random integer generator */

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
    } fault;
};

/**
 * Advance the fault counters and return @true if an error should occurr.
 */
static bool io_stub__fault_tick(struct io_stub *s)
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

static int io_stub__init(struct raft_io *io, unsigned id, const char *address)
{
    struct io_stub *s;

    s = io->impl;

    s->id = id;
    s->address = address;

    return 0;
}

static int io_stub__start(struct raft_io *io,
                          unsigned msecs,
                          raft_io_tick_cb tick_cb,
                          raft_io_recv_cb recv_cb)
{
    struct io_stub *s;

    (void)msecs;

    assert(io != NULL);

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->tick_cb = tick_cb;
    s->recv_cb = recv_cb;

    return 0;
}

static void io_stub__reset_flushed(struct io_stub *s);

static int io_stub__close(struct raft_io *io, void (*cb)(struct raft_io *io))
{
    struct io_stub *s;
    size_t i;

    s = io->impl;

    raft_io_stub_flush_all(io);

    for (i = 0; i < s->n; i++) {
        struct raft_entry *entry = &s->entries[i];
        raft_free(entry->buf.base);
    }

    if (s->entries != NULL) {
        raft_free(s->entries);
    }

    io_stub__reset_flushed(s);

    if (s->snapshot != NULL) {
        raft_snapshot__close(s->snapshot);
        raft_free(s->snapshot);
    }

    if (cb != NULL) {
        cb(io);
    }

    return 0;
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

    rv = configuration__copy(&s1->configuration, &s2->configuration);
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

static int io_stub__load(struct raft_io *io,
                         raft_term *term,
                         unsigned *voted_for,
                         struct raft_snapshot **snapshot,
                         struct raft_entry **entries,
                         size_t *n_entries)
{
    struct io_stub *s;
    size_t i;
    void *batch;
    void *cursor;
    size_t size = 0; /* Size of the batch */
    int rv;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    *term = s->term;
    *voted_for = s->voted_for;

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
        rv = RAFT_ENOMEM;
        goto err;
    }

    for (i = 0; i < s->n; i++) {
        size += s->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        rv = RAFT_ENOMEM;
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

static int io_stub__bootstrap(struct raft_io *io,
                              const struct raft_configuration *conf)
{
    struct io_stub *s;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    if (s->term != 0) {
        return RAFT_ERR_BUSY;
    }

    assert(s->voted_for == 0);
    assert(s->snapshot == NULL);
    assert(s->entries == NULL);
    assert(s->n == 0);

    /* Encode the given configuration. */
    rv = configuration__encode(conf, &buf);
    if (rv != 0) {
        return rv;
    }

    entries = raft_calloc(1, sizeof *s->entries);
    if (entries == NULL) {
        return RAFT_ENOMEM;
    }

    entries[0].term = 1;
    entries[0].type = RAFT_CONFIGURATION;
    entries[0].buf = buf;

    s->term = 1;
    s->voted_for = 0;
    s->snapshot = NULL;
    s->entries = entries;
    s->n = 1;

    return 0;
}

static int io_stub__set_term(struct raft_io *io, const raft_term term)
{
    struct io_stub *s;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->term = term;
    s->voted_for = 0;

    return 0;
}

static int io_stub__set_vote(struct raft_io *io, const unsigned server_id)
{
    struct io_stub *s;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->voted_for = server_id;

    return 0;
}

static int io_stub__append(struct raft_io *io,
                           const struct raft_entry entries[],
                           unsigned n,
                           void *data,
                           void (*cb)(void *data, int status))
{
    struct io_stub *s;
    struct append *r;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = APPEND;
    r->entries = entries;
    r->n = n;
    r->data = data;
    r->cb = cb;

    RAFT__QUEUE_PUSH(&s->requests, &r->queue);

    s->n_append++;

    return 0;
}

static int io_stub__truncate(struct raft_io *io, raft_index index)
{
    struct io_stub *s;
    size_t n;
    raft_index start_index;

    s = io->impl;

    if (s->snapshot == NULL) {
        start_index = 1;
    } else {
        start_index = s->snapshot->index;
    }

    assert(index >= start_index);

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    n = index - 1; /* Number of entries left after truncation */

    if (n > 0) {
        struct raft_entry *entries;

        /* Create a new array of entries holding the non-truncated entries */
        entries = raft_malloc(n * sizeof *entries);
        if (entries == NULL) {
            return RAFT_ENOMEM;
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

static int io_stub__snapshot_put(struct raft_io *io,
                                 struct raft_io_snapshot_put *req,
                                 const struct raft_snapshot *snapshot,
                                 raft_io_snapshot_put_cb cb)
{
    struct io_stub *s;
    struct snapshot_put *r;
    s = io->impl;

    assert(s->n_snapshot_put == 0);

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_PUT;
    r->req = req;
    r->req->cb = cb;
    r->snapshot = snapshot;

    RAFT__QUEUE_PUSH(&s->requests, &r->queue);
    s->n_snapshot_put++;

    return 0;
}

static int io_stub__snapshot_get(struct raft_io *io,
                                 struct raft_io_snapshot_get *req,
                                 raft_io_snapshot_get_cb cb)
{
    struct io_stub *s;
    struct snapshot_get *r;
    s = io->impl;

    assert(s->n_snapshot_put == 0);

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SNAPSHOT_GET;
    r->req = req;
    r->req->cb = cb;

    RAFT__QUEUE_PUSH(&s->requests, &r->queue);
    s->n_snapshot_get++;

    return 0;
}

static raft_time io_stub__time(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;

    return s->time;
}

static int io_stub__randint(struct raft_io *io, int min, int max)
{
    struct io_stub *s;
    s = io->impl;

    if (s->randint == NULL) {
        return (max - min) / 2;
    }

    return s->randint(min, max);
}

/**
 * Queue up a request which will be processed later, when io_stub_flush()
 * is invoked.
 */
static int io_stub__send(struct raft_io *io,
                         struct raft_io_send *req,
                         const struct raft_message *message,
                         raft_io_send_cb cb)
{
    struct io_stub *s;
    struct send *r;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SEND;
    r->req = req;
    r->message = *message;
    r->req->cb = cb;

    RAFT__QUEUE_PUSH(&s->requests, &r->queue);
    s->n_send++;

    return 0;
}

int raft_io_stub_init(struct raft_io *io, struct raft_logger *logger)
{
    struct io_stub *s;

    assert(io != NULL);

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        return RAFT_ENOMEM;
    }

    s->logger = logger;
    s->time = 0;
    s->term = 0;
    s->voted_for = 0;
    s->entries = NULL;
    s->snapshot = NULL;
    s->n = 0;

    RAFT__QUEUE_INIT(&s->requests);

    s->n_append = 0;
    s->n_send = 0;
    s->n_snapshot_put = 0;
    s->n_snapshot_get = 0;

    RAFT__QUEUE_INIT(&s->transmit);
    s->n_transmit = 0;

    s->n_peers = 0;

    s->min_latency = 0;
    s->max_latency = 0;

    s->randint = NULL;

    s->fault.countdown = -1;
    s->fault.n = -1;

    io->impl = s;
    io->init = io_stub__init;
    io->start = io_stub__start;
    io->close = io_stub__close;
    io->load = io_stub__load;
    io->bootstrap = io_stub__bootstrap;
    io->set_term = io_stub__set_term;
    io->set_vote = io_stub__set_vote;
    io->append = io_stub__append;
    io->truncate = io_stub__truncate;
    io->send = io_stub__send;
    io->snapshot_put = io_stub__snapshot_put;
    io->snapshot_get = io_stub__snapshot_get;
    io->time = io_stub__time;
    io->randint = io_stub__randint;

    return 0;
}

void raft_io_stub_close(struct raft_io *io)
{
    raft_free(io->impl);
}

/**
 * Reset data about last appended or sent entries.
 */
static void io_stub__reset_flushed(struct io_stub *s)
{
    while (!RAFT__QUEUE_IS_EMPTY(&s->transmit)) {
        raft__queue *head;
        struct transmit *transmit;
        struct raft_message *message;

        head = RAFT__QUEUE_HEAD(&s->transmit);
        RAFT__QUEUE_REMOVE(head);
        transmit = RAFT__QUEUE_DATA(head, struct transmit, queue);

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
        s->n_transmit--;
    }
    assert(s->n_transmit == 0);
}

void raft_io_stub_advance(struct raft_io *io, unsigned msecs)
{
    struct io_stub *s;
    s = io->impl;
    s->time += msecs;
    s->tick_cb(io);
}

void raft_io_stub_set_time(struct raft_io *io, unsigned time)
{
    struct io_stub *s;
    s = io->impl;
    s->time = time;
}

void raft_io_stub_set_randint(struct raft_io *io, int (*randint)(int, int))
{
    struct io_stub *s;
    s = io->impl;
    s->randint = randint;
}

void raft_io_stub_set_latency(struct raft_io *io, unsigned min, unsigned max)
{
    struct io_stub *s;
    s = io->impl;
    assert(min != 0);
    assert(max != 0);
    assert(min <= max);
    s->min_latency = min;
    s->max_latency = max;
}

/**
 * Copy all entries in @src into @dst.
 */
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

static void io_stub__flush_append(struct io_stub *s, struct append *append)
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

    if (append->cb != NULL) {
        append->cb(append->data, 0);
    }
    free(append);

    s->n_append--;
}

static void io_stub__flush_send(struct io_stub *s, struct send *send)
{
    struct transmit *transmit;
    struct raft_message *src;
    struct raft_message *dst;
    int rv;

    transmit = raft_malloc(sizeof *transmit);
    assert(transmit != NULL);

    if (s->min_latency != 0) {
        // transmit->timer
    }
    src = &send->message;
    dst = &transmit->message;

    RAFT__QUEUE_PUSH(&s->transmit, &transmit->queue);
    s->n_transmit++;

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
            rv = configuration__copy(&src->install_snapshot.conf,
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

    tracef(s, "io: flush to server %u: %s", src->server_id,
           message_names[dst->type]);

    if (send->req->cb != NULL) {
        send->req->cb(send->req, 0);
    }
    raft_free(send);
    s->n_send--;
}

static void io_stub__flush_snapshot_put(struct io_stub *s,
                                        struct snapshot_put *r)
{
    if (s->snapshot == NULL) {
        s->snapshot = raft_malloc(sizeof *s->snapshot);
        assert(s->snapshot != NULL);
    } else {
        raft_snapshot__close(s->snapshot);
    }

    snapshot_copy(r->snapshot, s->snapshot);

    if (r->req->cb != NULL) {
        r->req->cb(r->req, 0);
    }
    raft_free(r);
    s->n_snapshot_put--;
}

static void io_stub__flush_snapshot_get(struct io_stub *s,
                                        struct snapshot_get *r)
{
    struct raft_snapshot *snapshot = raft_malloc(sizeof *snapshot);
    assert(snapshot != NULL);
    snapshot_copy(s->snapshot, snapshot);
    r->req->cb(r->req, snapshot, 0);
    raft_free(r);
    s->n_snapshot_get--;
}

bool raft_io_stub_flush(struct raft_io *io)
{
    struct io_stub *s;
    raft__queue *head;
    struct request *r;

    s = io->impl;

    head = RAFT__QUEUE_HEAD(&s->requests);
    RAFT__QUEUE_REMOVE(head);
    r = RAFT__QUEUE_DATA(head, struct request, queue);

    switch (r->type) {
        case APPEND:
            io_stub__flush_append(s, (struct append *)r);
            break;
        case SEND:
            io_stub__flush_send(s, (struct send *)r);
            break;
        case SNAPSHOT_PUT:
            io_stub__flush_snapshot_put(s, (struct snapshot_put *)r);
            break;
        case SNAPSHOT_GET:
            io_stub__flush_snapshot_get(s, (struct snapshot_get *)r);
            break;
    }

    return !RAFT__QUEUE_IS_EMPTY(&s->requests);
}

void raft_io_stub_flush_all(struct raft_io *io)
{
    struct io_stub *s;
    bool has_more_requests;
    s = io->impl;

    io_stub__reset_flushed(s);

    do {
        has_more_requests = raft_io_stub_flush(io);
    } while (has_more_requests);

    assert(s->n_append == 0);
    assert(s->n_send == 0);
    assert(s->n_snapshot_put == 0);
    assert(s->n_snapshot_get == 0);
}

unsigned raft_io_stub_n_appending(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;
    return s->n_append;
}

/* Return the i-th pending request of the given type, or NULL. */
static struct request *raft_io_stub__pending(struct io_stub *s,
                                             unsigned i,
                                             int type)
{
    raft__queue *head;
    unsigned count = 0;

    RAFT__QUEUE_FOREACH(head, &s->requests)
    {
        struct request *r = RAFT__QUEUE_DATA(head, struct request, queue);
        if (r->type != type) {
            continue;
        }
        if (count == i) {
            return r;
        }
        count++;
    }
    return NULL;
}

void raft_io_stub_appending(struct raft_io *io,
                            unsigned i,
                            const struct raft_entry **entries,
                            unsigned *n)
{
    struct io_stub *s;
    struct append *r;

    s = io->impl;

    *entries = NULL;
    *n = 0;

    r = (struct append *)raft_io_stub__pending(s, i, APPEND);
    if (r != NULL) {
        *entries = ((struct append *)r)->entries;
        *n = ((struct append *)r)->n;
    }
}

unsigned raft_io_stub_n_sending(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;
    return s->n_send;
}

void raft_io_stub_sending(struct raft_io *io,
                          unsigned i,
                          struct raft_message **message)
{
    struct io_stub *s;
    struct send *r;

    s = io->impl;

    *message = NULL;

    r = (struct send *)raft_io_stub__pending(s, i, SEND);
    if (r != NULL) {
        *message = &((struct send *)r)->message;
    }
}

void raft_io_stub_dispatch(struct raft_io *io, struct raft_message *message)
{
    struct io_stub *s;
    s = io->impl;
    s->recv_cb(io, message);
}

void raft_io_stub_fault(struct raft_io *io, int delay, int repeat)
{
    struct io_stub *s;
    s = io->impl;
    s->fault.countdown = delay;
    s->fault.n = repeat;
}

unsigned raft_io_stub_term(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;
    return s->term;
}

unsigned raft_io_stub_vote(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;
    return s->voted_for;
}

void raft_io_stub_connect(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    s = io->impl;
    s_other = other->impl;
    assert(s->n_peers < MAX_PEERS);
    s->peers[s->n_peers].s = s_other;
    s->peers[s->n_peers].connected = true;
    s->n_peers++;
}

void raft_io_stub_disconnect(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    unsigned i;
    s = io->impl;
    s_other = other->impl;
    for (i = 0; i < s->n_peers; i++) {
        if (s->peers[i].s == s_other) {
            s->peers[i].connected = false;
            break;
        }
    }
    assert(i < s->n_peers);
}

void raft_io_stub_reconnect(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    unsigned i;
    s = io->impl;
    s_other = other->impl;
    for (i = 0; i < s->n_peers; i++) {
        if (s->peers[i].s == s_other) {
            s->peers[i].connected = true;
            break;
        }
    }
    assert(i < s->n_peers);
}
