#include <string.h>

#include "../include/raft.h"

#include "assert.h"

/* Set to 1 to enable logging. */
#if 0
#define __debugf(S, MSG, ...) raft_debugf(S->logger, MSG, __VA_ARGS__)
#else
#define __debugf(S, MSG, ...)
#endif

/**
 * Maximum number of pending send message requests. This should be enough for
 * testing purposes.
 */
#define RAFT_IO_STUB_MAX_PENDING 64

/**
 * Information about a pending request to send a message.
 */
struct raft_io_stub_send
{
    void *data;
    void (*cb)(void *data, int status);
};

/**
 * Stub I/O implementation implementing all operations in-memory.
 */
struct raft_io_stub
{
    /* Elapsed time since the backend was started. */
    unsigned time;

    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    raft_index start_index;     /* Index of the first persisted entry */
    struct raft_entry *entries; /* Array or persisted entries */
    size_t n;                   /* Size of the persisted entries array */

    /* Parameters passed via raft_io->init */
    struct raft_logger *logger;
    unsigned id;
    const char *address;

    struct
    {
        void *data;
        void (*cb)(void *data, unsigned msecs);
    } tick;

    struct
    {
        void *data;
        void (*cb)(void *data, struct raft_message *message);
    } recv;

    /* Append requests. */
    struct
    {
        /* Pending */
        struct
        {
            const struct raft_entry *entries;
            unsigned n_entries;
            void *data;
            void (*cb)(void *data, int status);
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
            struct raft_message messages[RAFT_IO_STUB_MAX_PENDING];
            struct raft_io_stub_send requests[RAFT_IO_STUB_MAX_PENDING];
        } pending;
        /* Copy of the last message that where appended upon flush. */
        struct
        {
            unsigned n_messages;
            struct raft_message messages[RAFT_IO_STUB_MAX_PENDING];
        } flushed;
    } send;

    struct
    {
        int countdown; /* Trigger the fault when this counter gets to zero. */
        int n;         /* Repeat the fault this many times. Default is -1. */
    } fault;
};

/**
 * Advance the fault counters and return @true if an error should occurr.
 */
static bool raft_io_stub__fault_tick(struct raft_io_stub *s)
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

static int raft_io_stub__start(const struct raft_io *io,
                               unsigned id,
                               const char *address,
                               unsigned msecs,
                               void *data,
                               void (*tick)(void *data, unsigned elapsed),
                               void (*recv)(void *data,
                                            struct raft_message *msg))
{
    struct raft_io_stub *s;

    (void)msecs;

    assert(io != NULL);

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->id = id;
    s->address = address;

    s->tick.data = data;
    s->tick.cb = tick;

    s->recv.data = data;
    s->recv.cb = recv;

    return 0;
}

static int raft_io_stub__stop(const struct raft_io *io,
                              void *data,
                              void (*cb)(void *data))
{
    (void)io;

    cb(data);

    return 0;
}

static int raft_io_stub__load(const struct raft_io *io,
                              raft_term *term,
                              unsigned *voted_for,
                              raft_index *start_index,
                              struct raft_entry **entries,
                              size_t *n_entries)
{
    struct raft_io_stub *s;
    size_t i;
    void *batch;
    void *cursor;
    size_t size = 0; /* Size of the batch */
    int rv;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    *term = s->term;
    *voted_for = s->voted_for;
    *start_index = s->start_index;

    if (s->n == 0) {
        *entries = NULL;
        *n_entries = 0;
        return 0;
    }

    /* Make a copy of the persisted entries, storing their data into a single
     * batch. */
    *n_entries = s->n;
    *entries = raft_calloc(s->n, sizeof **entries);
    if (*entries == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    for (i = 0; i < s->n; i++) {
        size += s->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        rv = RAFT_ERR_NOMEM;
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

    return 0;

err_after_entries_alloc:
    raft_free(*entries);

err:
    assert(rv != 0);
    return rv;
}

static int raft_io_stub__bootstrap(const struct raft_io *io,
                                   const struct raft_configuration *conf)
{
    struct raft_io_stub *s;
    struct raft_buffer buf;
    struct raft_entry *entries;
    int rv;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    if (s->term != 0) {
        return RAFT_ERR_BUSY;
    }

    assert(s->voted_for == 0);
    assert(s->start_index == 1);
    assert(s->entries == NULL);
    assert(s->n == 0);

    /* Encode the given configuration. */
    rv = raft_configuration_encode(conf, &buf);
    if (rv != 0) {
        return rv;
    }

    entries = raft_calloc(1, sizeof *s->entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    entries[0].term = 1;
    entries[0].type = RAFT_LOG_CONFIGURATION;
    entries[0].buf = buf;

    s->term = 1;
    s->voted_for = 0;
    s->start_index = 1;
    s->entries = entries;
    s->n = 1;

    return 0;
}

static int raft_io_stub__set_term(struct raft_io *io, const raft_term term)
{
    struct raft_io_stub *s;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->term = term;
    s->voted_for = 0;

    return 0;
}

static int raft_io_stub__set_vote(struct raft_io *io, const unsigned server_id)
{
    struct raft_io_stub *s;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    s->voted_for = server_id;

    return 0;
}

static int raft_io_stub__append(const struct raft_io *io,
                                const struct raft_entry entries[],
                                unsigned n,
                                void *data,
                                void (*cb)(void *data, int status))
{
    struct raft_io_stub *s;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    if (s->append.pending.data != NULL) {
        return RAFT_ERR_IO_BUSY;
    }

    s->append.pending.entries = entries;
    s->append.pending.n_entries = n;
    s->append.pending.cb = cb;
    s->append.pending.data = data;

    return 0;
}

static int raft_io_stub__truncate(const struct raft_io *io, raft_index index)
{
    struct raft_io_stub *s;
    size_t n;

    s = io->data;

    assert(index >= s->start_index);

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    n = index - 1;

    if (n > 0) {
        struct raft_entry *new_entries;
        new_entries = raft_malloc((index - 1) * sizeof *new_entries);
        if (new_entries == NULL) {
            return RAFT_ERR_NOMEM;
        }
        memcpy(new_entries, s->entries, n * sizeof *s->entries);
        if (s->entries != NULL) {
            size_t i;
            for (i = n; i < s->n; i++) {
                raft_free(s->entries[i].buf.base);
            }
            raft_free(s->entries);
        }
        s->entries = new_entries;
    } else {
        free(s->entries);
        s->entries = NULL;
    }

    s->n = n;

    return 0;
}

/**
 * Queue up a request which will be processed later, when raft_io_stub_flush()
 * is invoked.
 */
static int raft_io_stub__send(const struct raft_io *io,
                              const struct raft_message *message,
                              void *data,
                              void (*cb)(void *data, int status))
{
    struct raft_io_stub *s;
    size_t i;

    s = io->data;

    if (raft_io_stub__fault_tick(s)) {
        return RAFT_ERR_IO;
    }

    /* Check if we have still room available. */
    if (s->send.pending.n_messages == RAFT_IO_STUB_MAX_PENDING) {
        return RAFT_ERR_IO_BUSY;
    }

    i = s->send.pending.n_messages;

    s->send.pending.n_messages++;
    s->send.pending.messages[i] = *message;
    s->send.pending.requests[i].cb = cb;
    s->send.pending.requests[i].data = data;

    return 0;
}

int raft_io_stub_init(struct raft_io *io, struct raft_logger *logger)
{
    struct raft_io_stub *stub;

    assert(io != NULL);

    stub = raft_malloc(sizeof *stub);
    if (stub == NULL) {
        return RAFT_ERR_NOMEM;
    }

    stub->logger = logger;
    stub->time = 0;
    stub->term = 0;
    stub->voted_for = 0;
    stub->entries = NULL;
    stub->start_index = 1;
    stub->n = 0;

    memset(&stub->append, 0, sizeof stub->append);
    memset(&stub->send, 0, sizeof stub->send);

    stub->fault.countdown = -1;
    stub->fault.n = -1;

    io->data = stub;
    io->start = raft_io_stub__start;
    io->stop = raft_io_stub__stop;
    io->load = raft_io_stub__load;
    io->bootstrap = raft_io_stub__bootstrap;
    io->set_term = raft_io_stub__set_term;
    io->set_vote = raft_io_stub__set_vote;
    io->append = raft_io_stub__append;
    io->truncate = raft_io_stub__truncate;
    io->send = raft_io_stub__send;

    return 0;
}

/**
 * Reset data about last appended or sent entries.
 */
static void raft_io_stub__reset_flushed(struct raft_io_stub *s)
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
                    free(message->append_entries.entries[0].batch);
                    free(message->append_entries.entries);
                }
                break;
        }
    }

    s->send.flushed.n_messages = 0;
}

void raft_io_stub_close(struct raft_io *io)
{
    struct raft_io_stub *s;
    size_t i;

    assert(io != NULL);

    s = io->data;

    for (i = 0; i < s->n; i++) {
        struct raft_entry *entry = &s->entries[i];
        raft_free(entry->buf.base);
    }

    if (s->entries != NULL) {
        raft_free(s->entries);
    }

    raft_io_stub__reset_flushed(s);

    raft_free(s);
}

void raft_io_stub_advance(struct raft_io *io, unsigned msecs)
{
    struct raft_io_stub *s;

    assert(io != NULL);

    s = io->data;

    s->time += msecs;

    s->tick.cb(s->tick.data, msecs);
}

/**
 * Copy all entries in @src into @dst.
 */
static void raft_io_stub__copy_entries(const struct raft_entry *src,
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

static void raft_io_stub__append_cb(struct raft_io_stub *s)
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

    /* If it's not the very first write, copy the existing entries into the new
     * array. */
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

    raft_io_stub__copy_entries(s->append.pending.entries,
                               &s->append.flushed.entries,
                               s->append.pending.n_entries);
    s->append.flushed.n_entries = s->append.pending.n_entries;

    if (s->append.pending.cb != NULL) {
        s->append.pending.cb(s->append.pending.data, status);
    }

    s->append.pending.data = NULL;
    s->append.pending.cb = NULL;
}

void raft_io_stub_flush(struct raft_io *io)
{
    struct raft_io_stub *s;
    size_t i;

    assert(io != NULL);

    s = io->data;

    raft_io_stub__reset_flushed(s);

    if (s->append.pending.cb != NULL) {
        raft_io_stub__append_cb(s);
    }

    for (i = 0; i < s->send.pending.n_messages; i++) {
        struct raft_message *src = &s->send.pending.messages[i];
        struct raft_message *dst = &s->send.flushed.messages[i];
        struct raft_io_stub_send *request = &s->send.pending.requests[i];
        char desc[256];

        s->send.flushed.n_messages++;

        *dst = *src;

        switch (dst->type) {
            case RAFT_IO_APPEND_ENTRIES:
                sprintf(desc, "append entries");
                /* Make a copy of the entries being sent */
                raft_io_stub__copy_entries(src->append_entries.entries,
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
        }

        __debugf(s, "io: flush to server %u: %s", src->server_id, desc);

        if (request->cb != NULL) {
            request->cb(request->data, 0);
        }
    }

    s->send.pending.n_messages = 0;
}

void raft_io_stub_sent(struct raft_io *io,
                       struct raft_message **messages,
                       unsigned *n)
{
    struct raft_io_stub *s;

    s = io->data;

    *messages = s->send.flushed.messages;
    *n = s->send.flushed.n_messages;
}

void raft_io_stub_appended(struct raft_io *io,
                           struct raft_entry **entries,
                           unsigned *n)
{
    struct raft_io_stub *s;

    s = io->data;

    *entries = s->append.flushed.entries;
    *n = s->append.flushed.n_entries;
}

void raft_io_stub_dispatch(struct raft_io *io, struct raft_message *message)
{
    struct raft_io_stub *s;

    s = io->data;

    s->recv.cb(s->recv.data, message);
}

void raft_io_stub_fault(struct raft_io *io, int delay, int repeat)
{
    struct raft_io_stub *s;

    s = io->data;

    s->fault.countdown = delay;
    s->fault.n = repeat;
}

unsigned raft_io_stub_term(struct raft_io *io)
{
    struct raft_io_stub *s;

    s = io->data;

    return s->term;
}

unsigned raft_io_stub_vote(struct raft_io *io)
{
    struct raft_io_stub *s;

    s = io->data;

    return s->voted_for;
}
