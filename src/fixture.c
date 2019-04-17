#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft/fixture.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "queue.h"
#include "snapshot.h"
#include "logging.h"

/* Maximum number of cluster steps to perform when waiting for a certain state
 * to be reached. */
#define MAX_STEPS 100

#define HEARTBEAT_TIMEOUT 100
#define ELECTION_TIMEOUT 1000
#define MIN_LATENCY 5
#define MAX_LATENCY 50

/* Set to 1 to enable tracing. */
#if 0
#define tracef(S, MSG, ...) debugf(S->io, MSG, __VA_ARGS__)
static const char *message_names[6] = {
    NULL,           "append entries",      "append entries result",
    "request vote", "request vote result", "install snapshot"};

#else
#define tracef(S, MSG, ...)
#endif

/**
 * Configure the given @raft_io instance to use a stub in-memory I/O
 * implementation.
 */
int raft_io_stub_init(struct raft_io *io);

/**
 * Release all memory held by the given stub I/O implementation.
 */
void raft_io_stub_close(struct raft_io *io);

/**
 * Set the current time, without invoking the tick callback.
 */
void raft_io_stub_set_time(struct raft_io *io, unsigned time);

/**
 * Set the random integer generator.
 */
void raft_io_stub_set_random(struct raft_io *io, int (*f)(int, int));

/**
 * Set the minimum and maximum values of the random latency that is assigned to
 * a message once the callback of its send request has been fired and it has
 * been enqueued for delivery. By default there is no latency and messages are
 * delivered instantaneously.
 */
void raft_io_stub_set_latency(struct raft_io *io, unsigned min, unsigned max);

void raft_io_stub_set_disk_latency(struct raft_io *io, unsigned msecs);

/**
 * Set the initial term stored in this instance.
 */
void raft_io_stub_set_term(struct raft_io *io, raft_term term);

/**
 * Set the initial snapshot stored in this instance.
 */
void raft_io_stub_set_snapshot(struct raft_io *io,
                               struct raft_snapshot *snapshot);

/**
 * Set the initial entries stored in this instance.
 */
void raft_io_stub_set_entries(struct raft_io *io,
                              struct raft_entry *entry,
                              unsigned n);

/**
 * Add an entry to the initial entries stored in this instance.
 */
void raft_io_stub_add_entry(struct raft_io *io, struct raft_entry *entry);

/**
 * Advance the stub time by the given number of milliseconds, and invoke the
 * tick callback accordingly. Also, update the timers of messages in the
 * transmit queue, and if any of them expires deliver it to the destination peer
 * (if connected).
 */
void raft_io_stub_advance(struct raft_io *io, unsigned msecs);

/**
 * Flush the oldest request in the pending I/O queue, invoking the associated
 * callback as appropriate.
 *
 * Return true if there are more pending requests in the queue.
 */
bool raft_io_stub_flush(struct raft_io *io);

/**
 * Flush all pending I/O requests.
 */
void raft_io_stub_flush_all(struct raft_io *io);

/**
 * Return the amount of milliseconds left before the next message gets
 * delivered. If no delivery is pending, return -1.
 */
int raft_io_stub_next_deliver_timeout(struct raft_io *io);

/**
 * Manually trigger the delivery of a message, invoking the recv callback.
 */
void raft_io_stub_deliver(struct raft_io *io, struct raft_message *message);

/**
 * Return the number of pending append requests (i.e. requests successfully
 * submitted with raft_io->append(), but whose callbacks haven't been fired
 * yet).
 */
unsigned raft_io_stub_n_appending(struct raft_io *io);

/**
 * Return the entries if the i'th pending append request, or NULL.
 */
void raft_io_stub_appending(struct raft_io *io,
                            unsigned i,
                            const struct raft_entry **entries,
                            unsigned *n);

/**
 * Return the number of pending raft_io_send requests (i.e. requests
 * successfully submitted with raft_io->send(), but whose callbacks haven't been
 * fired yet).
 */
unsigned raft_io_stub_n_sending(struct raft_io *io);

/**
 * Return a pointer to the message associated with the i'th pending raft_io_send
 * request, or NULL.
 */
void raft_io_stub_sending(struct raft_io *io,
                          unsigned i,
                          struct raft_message **message);

/**
 * Inject a failure that will be triggered after @delay I/O requests and occur
 * @repeat times.
 */
void raft_io_stub_fault(struct raft_io *io, int delay, int repeat);

/**
 * Convenience for getting the current term stored in the stub.
 */
unsigned raft_io_stub_term(struct raft_io *io);

/**
 * Convenience for getting the current vote stored in the stub.
 */
unsigned raft_io_stub_vote(struct raft_io *io);

/**
 * Connect @io to @other, enabling delivery of messages sent from @io to @other.
 */
void raft_io_stub_connect(struct raft_io *io, struct raft_io *other);

/**
 * Return #true if @io is connected to @other.
 */
bool raft_io_stub_connected(struct raft_io *io, struct raft_io *other);

/**
 * Diconnect @io from @other, disabling delivery of messages sent from @io to
 * @other.
 */
void raft_io_stub_disconnect(struct raft_io *io, struct raft_io *other);

/**
 * Reconnect @io to @other, re-enabling delivery of messages sent from @io to
 * @other.
 */
void raft_io_stub_reconnect(struct raft_io *io, struct raft_io *other);

/**
 * Enable or disable silently dropping all outgoing messages of type @type.
 */
void raft_io_stub_drop(struct raft_io *io, int type, bool flag);

/* Maximum number of messages inflight on the network. This should be enough for
 * testing purposes. */
#define MAX_TRANSMIT 64

/* Maximum number of peer stub instances connected to a certain stub
 * instance. This should be enough for testing purposes. */
#define MAX_PEERS 8

#define REQUEST \
    int type;   \
    queue queue

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
    unsigned start; /* Request timestamp. */
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
    queue queue;
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
    struct raft_io *io; /* I/O object we're implementing */
    raft_time time;     /* Elapsed time since the backend was started. */

    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    struct raft_snapshot *snapshot; /* Latest snapshot */
    struct raft_entry *entries;     /* Array or persisted entries */
    size_t n;                       /* Size of the persisted entries array */

    /* Parameters passed via raft_io->init */
    unsigned id;
    const char *address;
    raft_io_tick_cb tick_cb;
    raft_io_recv_cb recv_cb;

    /* Queue of pending asynchronous requests, whose callbacks still haven't
     * been fired. */
    queue requests;

    unsigned n_append;       /* Number of pending append entries requests */
    unsigned n_send;         /* Number of pending send message requests */
    unsigned n_snapshot_put; /* Number of pending snapshot put requests */
    unsigned n_snapshot_get; /* Number of pending snapshot get requests */

    /* Queue of messages that have been written to the network, i.e. the
     * callback of the associated raft_io->send() request has been fired. */
    queue transmit;
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

    /* Fixed latency for disk I/p. The default is zero. */
    unsigned disk_latency;

    int (*random)(int, int); /* Random integer generator */

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
    } fault;

    bool drop[5];
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
        return RAFT_IOERR;
    }

    s->tick_cb = tick_cb;
    s->recv_cb = recv_cb;

    return 0;
}

static void drop_transmit(struct io_stub *s, struct transmit *transmit)
{
    struct raft_message *message;
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

/* Drop all messages in the transmit queue. */
static void drop_all_transmit(struct io_stub *s)
{
    while (!QUEUE_IS_EMPTY(&s->transmit)) {
        queue *head;
        struct transmit *transmit;

        head = QUEUE_HEAD(&s->transmit);
        QUEUE_REMOVE(head);
        transmit = QUEUE_DATA(head, struct transmit, queue);
        drop_transmit(s, transmit);
    }
    assert(s->n_transmit == 0);
}

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

    drop_all_transmit(s);

    if (s->snapshot != NULL) {
        snapshot__close(s->snapshot);
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
                         raft_index *start_index,
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

static int io_stub__bootstrap(struct raft_io *io,
                              const struct raft_configuration *conf)
{
    struct io_stub *s;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
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
    rv = configuration__encode(conf, &buf);
    if (rv != 0) {
        return rv;
    }

    entries = raft_calloc(1, sizeof *s->entries);
    if (entries == NULL) {
        return RAFT_NOMEM;
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
        return RAFT_IOERR;
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
        return RAFT_IOERR;
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
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = APPEND;
    r->entries = entries;
    r->n = n;
    r->data = data;
    r->cb = cb;
    r->start = s->time;

    QUEUE_PUSH(&s->requests, &r->queue);

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

    QUEUE_PUSH(&s->requests, &r->queue);
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

    QUEUE_PUSH(&s->requests, &r->queue);
    s->n_snapshot_get++;

    return 0;
}

static raft_time io_stub__time(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;

    return s->time;
}

static int io_stub__random(struct raft_io *io, int min, int max)
{
    struct io_stub *s;
    s = io->impl;

    return s->random(min, max);
}

static void io_stub__emit(struct raft_io *io,
                          int level,
                          const char *format,
                          ...)
{
    struct io_stub *s;
    va_list args;
    va_start(args, format);
    s = io->impl;
    emit_to_stream(stderr, s->id, s->time, level, format, args);
    va_end(args);
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
        return RAFT_IOERR;
    }

    r = raft_malloc(sizeof *r);
    assert(r != NULL);

    r->type = SEND;
    r->req = req;
    r->message = *message;
    r->req->cb = cb;

    QUEUE_PUSH(&s->requests, &r->queue);
    s->n_send++;

    return 0;
}

static int default_random(int min, int max)
{
    assert(min < max);
    return min + (rand() % (max + 1 - min));
}

int raft_io_stub_init(struct raft_io *io)
{
    struct io_stub *s;

    assert(io != NULL);

    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        return RAFT_NOMEM;
    }

    s->io = io;
    s->time = 0;
    s->term = 0;
    s->voted_for = 0;
    s->snapshot = NULL;
    s->entries = NULL;
    s->n = 0;

    QUEUE_INIT(&s->requests);

    s->n_append = 0;
    s->n_send = 0;
    s->n_snapshot_put = 0;
    s->n_snapshot_get = 0;

    QUEUE_INIT(&s->transmit);
    s->n_transmit = 0;

    s->n_peers = 0;

    s->min_latency = 0;
    s->max_latency = 0;
    s->disk_latency = 0;

    s->random = default_random;

    s->fault.countdown = -1;
    s->fault.n = -1;

    memset(s->drop, 0, sizeof s->drop);

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
    io->random = io_stub__random;
    io->emit = io_stub__emit;

    return 0;
}

void raft_io_stub_close(struct raft_io *io)
{
    raft_free(io->impl);
}

static void deliver_transmit(struct io_stub *s, struct transmit *transmit)
{
    struct raft_message *message = &transmit->message;
    unsigned i;

    /* If this message type is in the drop list, let's discard it */
    if (s->drop[message->type - 1]) {
        drop_transmit(s, transmit);
        return;
    }

    /* Search for the destination server. */
    for (i = 0; i < s->n_peers; i++) {
        if (s->peers[i].s->id == message->server_id) {
            break;
        }
    }

    /* We don't have any peer with this ID or if the peers is disconnect, let's
     * drop the message */
    if (i == s->n_peers || !s->peers[i].connected) {
        drop_transmit(s, transmit);
        return;
    }

    /* Update the message object with our details. */
    message->server_id = s->id;
    message->server_address = s->address;

    raft_io_stub_deliver(s->peers[i].s->io, message);
    raft_free(transmit);
    s->n_transmit--;
}

void raft_io_stub_advance(struct raft_io *io, unsigned msecs)
{
    struct io_stub *s;
    queue q;

    s = io->impl;
    s->time += msecs;
    s->tick_cb(io);

    /* Deliver or messages whose timer expires */
    QUEUE_INIT(&q);
    while (!QUEUE_IS_EMPTY(&s->transmit)) {
        queue *head;
        struct transmit *transmit;

        head = QUEUE_HEAD(&s->transmit);
        QUEUE_REMOVE(head);
        transmit = QUEUE_DATA(head, struct transmit, queue);

        transmit->timer -= msecs;

        if (transmit->timer <= 0) {
            deliver_transmit(s, transmit);
        } else {
            QUEUE_PUSH(&q, &transmit->queue);
        }
    }

    while (!QUEUE_IS_EMPTY(&q)) {
        queue *head;
        head = QUEUE_HEAD(&q);
        QUEUE_REMOVE(head);
        QUEUE_PUSH(&s->transmit, head);
    }
}

void raft_io_stub_set_time(struct raft_io *io, unsigned time)
{
    struct io_stub *s;
    s = io->impl;
    s->time = time;
}

void raft_io_stub_set_random(struct raft_io *io, int (*f)(int, int))
{
    struct io_stub *s;
    s = io->impl;
    s->random = f;
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

void raft_io_stub_set_disk_latency(struct raft_io *io, unsigned msecs)
{
    struct io_stub *s;
    s = io->impl;
    s->disk_latency = msecs;
}

void raft_io_stub_set_term(struct raft_io *io, raft_term term)
{
    struct io_stub *s;
    s = io->impl;
    s->term = term;
}

void raft_io_stub_set_snapshot(struct raft_io *io,
                               struct raft_snapshot *snapshot)
{
    struct io_stub *s;
    s = io->impl;
    s->snapshot = snapshot;
}

void raft_io_stub_set_entries(struct raft_io *io,
                              struct raft_entry *entries,
                              unsigned n)
{
    struct io_stub *s;
    s = io->impl;
    s->entries = entries;
    s->n = n;
}

void raft_io_stub_add_entry(struct raft_io *io, struct raft_entry *entry)
{
    struct io_stub *s;
    struct raft_entry *entries;
    s = io->impl;
    entries = raft_realloc(s->entries, (s->n + 1) * sizeof *entries);
    assert(entries != NULL);
    entries[s->n] = *entry;
    s->entries = entries;
    s->n++;
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
        transmit->timer = s->random(s->min_latency, s->max_latency);
    } else {
        transmit->timer = 0;
    }

    src = &send->message;
    dst = &transmit->message;

    QUEUE_PUSH(&s->transmit, &transmit->queue);
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
        snapshot__close(s->snapshot);
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

static struct request *stubFlush(struct io_stub *s)
{
    queue *head;
    struct request *r;
    struct append *append;

    head = QUEUE_HEAD(&s->requests);
    r = QUEUE_DATA(head, struct request, queue);
    QUEUE_REMOVE(head);

    switch (r->type) {
        case APPEND:
            append = (struct append *)r;
            if (s->time - append->start < s->disk_latency) {
                return r;
            }
            io_stub__flush_append(s, append);
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

    return NULL;
}

bool raft_io_stub_flush(struct raft_io *io)
{
    struct io_stub *s;
    s = io->impl;
    stubFlush(s);
    return !QUEUE_IS_EMPTY(&s->requests);
}

void raft_io_stub_flush_all(struct raft_io *io)
{
    struct io_stub *s;
    struct request *r;
    queue q;
    s = io->impl;

    QUEUE_INIT(&q);

    while (!QUEUE_IS_EMPTY(&s->requests)) {
        r = stubFlush(s);
        if (r != NULL) {
            QUEUE_PUSH(&q, &r->queue);
        }
    };

    while (!QUEUE_IS_EMPTY(&q)) {
        queue *head;
        head = QUEUE_HEAD(&q);
	QUEUE_REMOVE(head);
        QUEUE_PUSH(&s->requests, head);
    }

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
    queue *head;
    unsigned count = 0;

    QUEUE_FOREACH(head, &s->requests)
    {
        struct request *r = QUEUE_DATA(head, struct request, queue);
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

int raft_io_stub_next_deliver_timeout(struct raft_io *io)
{
    struct io_stub *s;
    int lowest_timer = -1;
    queue *head;
    s = io->impl;
    QUEUE_FOREACH(head, &s->transmit)
    {
        struct transmit *t = QUEUE_DATA(head, struct transmit, queue);
        if (lowest_timer == -1) {
            lowest_timer = t->timer;
        } else if (t->timer < lowest_timer) {
            lowest_timer = t->timer;
        }
    }
    assert(lowest_timer == -1 || lowest_timer >= 0);
    return lowest_timer;
}

void raft_io_stub_deliver(struct raft_io *io, struct raft_message *message)
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

static struct peer *get_peer(struct io_stub *s, unsigned id)
{
    unsigned i;
    for (i = 0; i < s->n_peers; i++) {
        struct peer *peer = &s->peers[i];
        if (peer->s->id == id) {
            return peer;
        }
    }
    return NULL;
}

bool raft_io_stub_connected(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = get_peer(s, s_other->id);
    return peer != NULL && peer->connected;
}

void raft_io_stub_disconnect(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = get_peer(s, s_other->id);
    peer->connected = false;
}

void raft_io_stub_reconnect(struct raft_io *io, struct raft_io *other)
{
    struct io_stub *s;
    struct io_stub *s_other;
    struct peer *peer;
    s = io->impl;
    s_other = other->impl;
    peer = get_peer(s, s_other->id);
    peer->connected = true;
}

void raft_io_stub_drop(struct raft_io *io, int type, bool flag)
{
    struct io_stub *s;
    s = io->impl;
    s->drop[type - 1] = flag;
}

static int init_server(unsigned i,
                       struct raft_fixture_server *s,
                       struct raft_fsm *fsm)
{
    int rc;
    s->alive = true;
    s->id = i + 1;
    sprintf(s->address, "%u", s->id);
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    raft_io_stub_set_latency(&s->io, MIN_LATENCY, MAX_LATENCY);
    rc = raft_init(&s->raft, &s->io, fsm, s->id, s->address);
    if (rc != 0) {
        return rc;
    }
    raft_set_election_timeout(&s->raft, ELECTION_TIMEOUT);
    raft_set_heartbeat_timeout(&s->raft, HEARTBEAT_TIMEOUT);
    return 0;
}

static void close_server(struct raft_fixture_server *s)
{
    raft_close(&s->raft, NULL);
    raft_io_stub_close(&s->io);
}

/* Connect the server with the given index to all others */
static void connect_to_all(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        if (i == j) {
            continue;
        }
        raft_io_stub_connect(io1, io2);
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
        struct raft_fixture_server *s = &f->servers[i];
        rc = init_server(i, s, &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        connect_to_all(f, i);
    }

    log__init(&f->log);
    f->commit_index = 0;

    return 0;
}

void raft_fixture_close(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        close_server(&f->servers[i]);
    }
    log__close(&f->log);
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
    return raft_io_stub_vote(&f->servers[i].io);
}

/* Flush any pending write to the disk and any pending message into the network
 * buffers (this will assign them a latency timer). */
static void flush_io(struct raft_fixture *f)
{
    size_t i;
    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        raft_io_stub_flush_all(io);
    }
}

/* Figure what's the lowest delivery timer across all stub I/O instances,
 * i.e. the time at which the next message should be delivered (if any is
 * pending). */
static int lowest_deliver_timeout(struct raft_fixture *f)
{
    int min_timeout = -1;
    size_t i;

    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        int timeout;
        timeout = raft_io_stub_next_deliver_timeout(io);
        if (timeout == -1) {
            continue;
        }
        if (min_timeout == -1 || timeout < min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/* Check what's the raft instance whose timer is closest to expiration. */
static unsigned lowest_raft_timeout(struct raft_fixture *f)
{
    size_t i;
    unsigned min_timeout = 0; /* Lowest remaining time before expiration */

    for (i = 0; i < f->n; i++) {
        struct raft *r = &f->servers[i].raft;
        unsigned timeout; /* Milliseconds remaining before expiration. */

        timeout = raft_next_timeout(r);
        if (i == 0 || timeout <= min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

void raft_fixture_advance(struct raft_fixture *f, unsigned msecs)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        if (!s->alive) {
            continue;
        }
        raft_io_stub_advance(&s->io, msecs);
    }
    f->time += msecs;
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
static bool update_leader(struct raft_fixture *f)
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
                !raft_fixture_connected(f, leader_i, i)) {
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
static void check_leader_append_only(struct raft_fixture *f)
{
    struct raft *raft;
    raft_index index;
    raft_index last = log__last_index(&f->log);

    /* If the cached log is empty it means there was no leader before. */
    if (last == 0) {
        return;
    }

    /* If there's no new leader, just return. */
    if (f->leader_id == 0) {
        return;
    }

    raft = raft_fixture_get(f, f->leader_id - 1);
    last = log__last_index(&f->log);

    for (index = 1; index <= last; index++) {
        const struct raft_entry *entry1;
        const struct raft_entry *entry2;

        entry1 = log__get(&f->log, index);
        entry2 = log__get(&raft->log, index);

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
static void copy_leader_log(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    struct raft_entry *entries;
    unsigned n;
    size_t i;
    int rc;

    log__close(&f->log);
    log__init(&f->log);

    rc = log__acquire(&raft->log, 1, &entries, &n);
    assert(rc == 0);

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        struct raft_buffer buf;
        buf.len = entry->buf.len;
        buf.base = raft_malloc(buf.len);
        memcpy(buf.base, entry->buf.base, buf.len);
        rc = log__append(&f->log, entry->term, entry->type, &buf, NULL);
        assert(rc == 0);
    }

    log__release(&raft->log, 1, entries, n);
}

/* Update the commit index to match the one from the current leader. */
static void update_commit_index(struct raft_fixture *f)
{
    struct raft *raft = raft_fixture_get(f, f->leader_id - 1);
    if (raft->commit_index > f->commit_index) {
        f->commit_index = raft->commit_index;
    }
}

void raft_fixture_step(struct raft_fixture *f)
{
    int deliver_timeout;
    unsigned raft_timeout;
    unsigned timeout;

    /* First flush I/O operations. */
    flush_io(f);

    /* Second, figure what's the message with the lowest timer (i.e. the
     * message that should be delivered first) */
    deliver_timeout = lowest_deliver_timeout(f);

    /* Now check what's the raft instance whose timer is closest to
     * expiration. */
    raft_timeout = lowest_raft_timeout(f);

    /* Fire either a raft tick or a message delivery. */
    if (deliver_timeout != -1 && (unsigned)deliver_timeout < raft_timeout) {
        timeout = deliver_timeout;
    } else {
        timeout = raft_timeout;
    }

    raft_fixture_advance(f, timeout + 1);

    /* If the leader has not changed check the Leader Append-Only
     * guarantee. */
    if (!update_leader(f)) {
        check_leader_append_only(f);
    }

    /* If we have a leader, update leader-related state . */
    if (f->leader_id != 0) {
        copy_leader_log(f);
        update_commit_index(f);
    }
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

static bool has_leader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id != 0;
}

bool raft_fixture_step_until_has_leader(struct raft_fixture *f,
                                        unsigned max_msecs)
{
    return raft_fixture_step_until(f, has_leader, NULL, max_msecs);
}

static bool has_no_leader(struct raft_fixture *f, void *arg)
{
    (void)arg;
    return f->leader_id == 0;
}

bool raft_fixture_step_until_has_no_leader(struct raft_fixture *f,
                                           unsigned max_msecs)
{
    return raft_fixture_step_until(f, has_no_leader, NULL, max_msecs);
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void drop_all_except(struct raft_fixture *f,
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
        raft_io_stub_drop(&s->io, type, flag);
    }
}

/* Reset the election timeout on all servers except the given one. */
static void set_all_election_timeouts_except(struct raft_fixture *f,
                                             unsigned msecs,
                                             unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        struct raft *raft = &f->servers[j].raft;
        if (j == i) {
            continue;
        }
        raft_set_election_timeout(raft, msecs);
    }
}

void raft_fixture_elect(struct raft_fixture *f, unsigned i)
{
    struct raft *raft = raft_fixture_get(f, i);
    unsigned j;

    /* Make sure there's currently no leader. */
    assert(f->leader_id == 0);

    /* Make sure that the given server is voting. */
    assert(configuration__get(&raft->configuration, raft->id)->voting);

    /* Make sure all servers are currently followers */
    for (j = 0; j < f->n; j++) {
        assert(raft_state(&f->servers[j].raft) == RAFT_FOLLOWER);
    }

    /* Set a very large election timeout on all servers, except the one being
     * elected, effectively preventing competition. */
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT * 10, i);

    raft_fixture_step_until_has_leader(f, ELECTION_TIMEOUT * 20);
    assert(f->leader_id == raft->id);

    set_all_election_timeouts_except(f, ELECTION_TIMEOUT, i);
}

void raft_fixture_depose(struct raft_fixture *f)
{
    unsigned leader_i;

    assert(f->leader_id != 0);
    leader_i = f->leader_id - 1;

    /* Set a very large election timeout on all followers, to prevent them from
     * starting an election. */
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT * 10, leader_i);

    /* Prevent all servers from sending append entries results, so the leader
     * will eventually step down. */
    drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, true, leader_i);

    raft_fixture_step_until_has_no_leader(f, ELECTION_TIMEOUT * 3);
    assert(f->leader_id == 0);

    drop_all_except(f, RAFT_IO_APPEND_ENTRIES_RESULT, false, leader_i);
    set_all_election_timeouts_except(f, ELECTION_TIMEOUT, leader_i);
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

static bool has_become(struct raft_fixture *f, void *arg)
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
    return raft_fixture_step_until(f, has_become, &target, max_msecs);
}

void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    raft_io_stub_disconnect(io1, io2);
    raft_io_stub_disconnect(io2, io1);
}

static void disconnect_from_all(struct raft_fixture *f, unsigned i)
{
    unsigned j;
    for (j = 0; j < f->n; j++) {
        if (j == i) {
            continue;
        }
        raft_fixture_disconnect(f, i, j);
    }
}

bool raft_fixture_connected(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    return raft_io_stub_connected(io1, io2) && raft_io_stub_connected(io2, io1);
}

void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j)
{
    struct raft_io *io1 = &f->servers[i].io;
    struct raft_io *io2 = &f->servers[j].io;
    raft_io_stub_reconnect(io1, io2);
    raft_io_stub_reconnect(io2, io1);
}

void raft_fixture_kill(struct raft_fixture *f, unsigned i)
{
    disconnect_from_all(f, i);
    f->servers[i].alive = false;
}

int raft_fixture_grow(struct raft_fixture *f, struct raft_fsm *fsm)
{
    struct raft_fixture_server *s;
    unsigned i;
    unsigned j;
    int rc;
    i = f->n;
    f->n++;
    s = &f->servers[i];

    rc = init_server(i, s, fsm);
    if (rc != 0) {
        return rc;
    }

    connect_to_all(f, i);
    for (j = 0; j < f->n; j++) {
        struct raft_io *io1 = &f->servers[i].io;
        struct raft_io *io2 = &f->servers[j].io;
        raft_io_stub_connect(io2, io1);
    }

    return 0;
}

void raft_fixture_set_random(struct raft_fixture *f,
                             unsigned i,
                             int (*random)(int, int))
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_random(&s->io, random);
}

void raft_fixture_set_latency(struct raft_fixture *f,
                              unsigned i,
                              unsigned min,
                              unsigned max)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_latency(&s->io, min, max);
}

void raft_fixture_set_disk_latency(struct raft_fixture *f,
                                   unsigned i,
                                   unsigned msecs)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_disk_latency(&s->io, msecs);
}

void raft_fixture_set_term(struct raft_fixture *f, unsigned i, raft_term term)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_term(&s->io, term);
}

void raft_fixture_set_snapshot(struct raft_fixture *f,
                               unsigned i,
                               struct raft_snapshot *snapshot)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_snapshot(&s->io, snapshot);
}

void raft_fixture_set_entries(struct raft_fixture *f,
                              unsigned i,
                              struct raft_entry *entries,
                              unsigned n)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_set_entries(&s->io, entries, n);
}

void raft_fixture_add_entry(struct raft_fixture *f,
                            unsigned i,
                            struct raft_entry *entry)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_add_entry(&s->io, entry);
}

void raft_fixture_io_fault(struct raft_fixture *f,
                           unsigned i,
                           int delay,
                           int repeat)
{
    struct raft_fixture_server *s = &f->servers[i];
    raft_io_stub_fault(&s->io, delay, repeat);
}
