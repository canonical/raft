#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"
#include "../include/raft/io_stub.h"

#include "assert.h"
#include "configuration.h"
#include "snapshot.h"

/* Set to 1 to enable logging. */
#if 1
#define __debugf(S, MSG, ...) raft_debugf(S->logger, MSG, __VA_ARGS__)
#else
#define __debugf(S, MSG, ...)
#endif

/**
 * Maximum number of pending send message requests. This should be enough for
 * testing purposes.
 */
#define IO_STUB_MAX_PENDING 64

/**
 * Information about a pending request to send a message.
 */
struct send
{
    struct raft_io_send *req;
    raft_io_send_cb cb;
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

    /* Append requests. */
    struct
    {
        /* Pending */
        struct
        {
            struct raft_entry *entries;
            size_t n_entries;
            struct
            {
                void *data;
                void (*f)(void *data, int status);
            } cbs[64];
            size_t n_cbs;
        } pending;
        /* Copy of the last entries that where appended upon flush. */
        struct
        {
            struct raft_entry *entries;
            unsigned n_entries;
        } flushed;
    } append;

    /* Outgoing messages. */
    struct
    {
        /* Pending */
        struct
        {
            unsigned n_messages;
            struct raft_message messages[IO_STUB_MAX_PENDING];
            struct send requests[IO_STUB_MAX_PENDING];
        } pending;
        /* Copy of the last message that where appended upon flush. */
        struct
        {
            unsigned n_messages;
            struct raft_message messages[IO_STUB_MAX_PENDING];
        } flushed;
    } send;

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
    } fault;

    struct raft_io_snapshot_put *snapshot_put;
    struct raft_io_snapshot_get *snapshot_get;
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

    for (i = 0; i < s->n; i++) {
        struct raft_entry *entry = &s->entries[i];
        raft_free(entry->buf.base);
    }

    if (s->entries != NULL) {
        raft_free(s->entries);
    }

    if (s->append.pending.entries != NULL) {
        raft_free(s->append.pending.entries);
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
    entries[0].type = RAFT_LOG_CONFIGURATION;
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

/**
 * Append to @entries2 all entries in @entries1.
 */
static int io_stub__extend_entries(const struct raft_entry *entries1,
                                   const size_t n_entries1,
                                   struct raft_entry **entries2,
                                   size_t *n_entries2)
{
    struct raft_entry *entries; /* To re-allocate the given entries */
    size_t i;

    entries =
        raft_realloc(*entries2, (*n_entries2 + n_entries1) * sizeof *entries);
    if (entries == NULL) {
        return RAFT_ENOMEM;
    }

    for (i = 0; i < n_entries1; i++) {
        entries[*n_entries2 + i] = entries1[i];
    }

    *entries2 = entries;
    *n_entries2 += n_entries1;

    return 0;
}

static int io_stub__append(struct raft_io *io,
                           const struct raft_entry entries[],
                           unsigned n,
                           void *data,
                           void (*cb)(void *data, int status))
{
    struct io_stub *s;
    int rv;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    rv = io_stub__extend_entries(entries, n, &s->append.pending.entries,
                                 &s->append.pending.n_entries);
    if (rv != 0) {
        return rv;
    }

    s->append.pending.cbs[s->append.pending.n_cbs].f = cb;
    s->append.pending.cbs[s->append.pending.n_cbs].data = data;

    s->append.pending.n_cbs++;

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
        free(s->entries);
        s->entries = NULL;
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
    s = io->impl;

    assert(s->snapshot_put == NULL);

    if (s->snapshot == NULL) {
        s->snapshot = raft_malloc(sizeof *s->snapshot);
        assert(s->snapshot != NULL);
    } else {
        unsigned i;
        raft_configuration_close(&s->snapshot->configuration);
        for (i = 0; i < s->snapshot->n_bufs; i++) {
            raft_free(s->snapshot->bufs[0].base);
        }
        raft_free(s->snapshot->bufs);
    }

    snapshot_copy(snapshot, s->snapshot);

    req->cb = cb;
    s->snapshot_put = req;

    return 0;
}

static int io_stub__snapshot_get(struct raft_io *io,
                                 struct raft_io_snapshot_get *req,
                                 raft_io_snapshot_get_cb cb)
{
    struct io_stub *s;
    s = io->impl;

    req->cb = cb;
    s->snapshot_get = req;

    return 0;
}

static raft_time io_stub__time(struct raft_io *io) {
    struct io_stub *s;
    s = io->impl;

    return s->time;
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
    size_t i;

    s = io->impl;

    if (io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    i = s->send.pending.n_messages;

    s->send.pending.n_messages++;
    s->send.pending.messages[i] = *message;
    s->send.pending.requests[i].req = req;
    s->send.pending.requests[i].cb = cb;

    return 0;
}

int raft_io_stub_init(struct raft_io *io, struct raft_logger *logger)
{
    struct io_stub *stub;

    assert(io != NULL);

    stub = raft_malloc(sizeof *stub);
    if (stub == NULL) {
        return RAFT_ENOMEM;
    }

    stub->logger = logger;
    stub->time = 0;
    stub->term = 0;
    stub->voted_for = 0;
    stub->entries = NULL;
    stub->snapshot = NULL;
    stub->n = 0;

    memset(&stub->append, 0, sizeof stub->append);
    memset(&stub->send, 0, sizeof stub->send);

    stub->snapshot_put = NULL;
    stub->snapshot_get = NULL;

    stub->fault.countdown = -1;
    stub->fault.n = -1;

    io->impl = stub;
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
    unsigned i;

    if (s->append.flushed.entries != NULL) {
        raft_free(s->append.flushed.entries[0].batch);
        raft_free(s->append.flushed.entries);

        s->append.flushed.entries = NULL;
        s->append.flushed.n_entries = 0;
    }

    for (i = 0; i < s->send.flushed.n_messages; i++) {
        struct raft_message *message = &s->send.flushed.messages[i];

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
    }

    s->send.flushed.n_messages = 0;
}

void raft_io_stub_advance(struct raft_io *io, unsigned msecs)
{
    struct io_stub *s;

    assert(io != NULL);

    s = io->impl;

    s->time += msecs;

    s->tick_cb(io, msecs);
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

static void io_stub__append_cb(struct io_stub *s)
{
    int status = 0;
    size_t n = s->append.pending.n_entries;
    struct raft_entry *all_entries;
    const struct raft_entry *new_entries = s->append.pending.entries;
    size_t i;

    assert(new_entries != NULL);
    assert(n > 0);

    /* Allocate an array for the old entries plus ne the new ons. */
    all_entries = raft_malloc((s->n + n) * sizeof *all_entries);
    assert(all_entries != NULL);

    /* If it's not the very first write, copy the existing entries into the
     * new array. */
    if (s->n > 0) {
        assert(s->entries != NULL);
        memcpy(all_entries, s->entries, s->n * sizeof *s->entries);
    }

    /* Copy the new entries into the new array. */
    memcpy(all_entries + s->n, new_entries, n * sizeof *new_entries);
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &all_entries[s->n + i];

        /* Make a copy of the actual entry data. */
        entry->buf.base = raft_malloc(entry->buf.len);
        assert(entry->buf.base != NULL);
        memcpy(entry->buf.base, new_entries[i].buf.base, entry->buf.len);
    }

    if (s->entries != NULL) {
        raft_free(s->entries);
    }

    s->entries = all_entries;
    s->n += n;

    io_stub__copy_entries(s->append.pending.entries, &s->append.flushed.entries,
                          s->append.pending.n_entries);
    s->append.flushed.n_entries = s->append.pending.n_entries;

    for (i = 0; i < s->append.pending.n_cbs; i++) {
        void *data = s->append.pending.cbs[i].data;
        void (*f)(void *data, int status) = s->append.pending.cbs[i].f;

        if (f != NULL) {
            f(data, status);
        }
    }

    s->append.pending.n_entries = 0;
    s->append.pending.n_cbs = 0;
}

void raft_io_stub_flush(struct raft_io *io)
{
    struct io_stub *s;
    size_t i;
    int rv;

    assert(io != NULL);

    s = io->impl;

    io_stub__reset_flushed(s);

    if (s->append.pending.n_cbs > 0) {
        io_stub__append_cb(s);
    }

    for (i = 0; i < s->send.pending.n_messages; i++) {
        struct raft_message *src = &s->send.pending.messages[i];
        struct raft_message *dst = &s->send.flushed.messages[i];
        struct send *request = &s->send.pending.requests[i];
        char desc[256];

        s->send.flushed.n_messages++;

        *dst = *src;

        switch (dst->type) {
            case RAFT_IO_APPEND_ENTRIES:
                sprintf(desc, "append entries");
                /* Make a copy of the entries being sent */
                io_stub__copy_entries(src->append_entries.entries,
                                      &dst->append_entries.entries,
                                      src->append_entries.n_entries);
                dst->append_entries.n_entries = src->append_entries.n_entries;
                break;
            case RAFT_IO_APPEND_ENTRIES_RESULT:
                sprintf(desc, "append entries result");
                break;
            case RAFT_IO_REQUEST_VOTE:
                sprintf(desc, "request vote");
                break;
            case RAFT_IO_REQUEST_VOTE_RESULT:
                sprintf(desc, "request vote result");
                break;
            case RAFT_IO_INSTALL_SNAPSHOT:
                sprintf(desc, "install snapshot");
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

        __debugf(s, "io: flush to server %u: %s", src->server_id, desc);

        if (request->cb != NULL) {
            request->cb(request->req, 0);
        }
    }

    s->send.pending.n_messages = 0;

    if (s->snapshot_put != NULL) {
        if (s->snapshot_put->cb != NULL) {
            s->snapshot_put->cb(s->snapshot_put, 0);
        }
        s->snapshot_put = NULL;
    }

    if (s->snapshot_get != NULL) {
        struct raft_snapshot *snapshot = raft_malloc(sizeof *snapshot);
        assert(snapshot != NULL);
        snapshot_copy(s->snapshot, snapshot);
        s->snapshot_get->cb(s->snapshot_get, snapshot, 0);
        s->snapshot_get = NULL;
    }
}

void raft_io_stub_sent(struct raft_io *io,
                       struct raft_message **messages,
                       unsigned *n)
{
    struct io_stub *s;

    s = io->impl;

    *messages = s->send.flushed.messages;
    *n = s->send.flushed.n_messages;
}

void raft_io_stub_appended(struct raft_io *io,
                           struct raft_entry **entries,
                           unsigned *n)
{
    struct io_stub *s;

    s = io->impl;

    *entries = s->append.flushed.entries;
    *n = s->append.flushed.n_entries;
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
