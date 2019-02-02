#include <assert.h>
#include <string.h>

#include "../include/raft.h"

/**
 * Maximum number of pending I/O requests. This should be enough for testing
 * purposes.
 */
#define RAFT_IO_STUB_MAX_REQUESTS 64

/**
 * Pending request to send a message.
 */
struct raft_io_stub_request
{
    bool pending;
    void *data;
    void (*cb)(void *data, int status);
    struct raft_message message;
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

    /* In-flight append request. */
    struct
    {
        const struct raft_entry *entries;
        unsigned n_entries;
        void *data;
        void (*cb)(void *data, int status);
    } append;

    /* In-flight send requests. */
    struct
    {
        struct raft_io_stub_request requests[RAFT_IO_STUB_MAX_REQUESTS];
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

    if (s->append.data != NULL) {
        return RAFT_ERR_IO_BUSY;
    }

    s->append.entries = entries;
    s->append.n_entries = n;
    s->append.cb = cb;
    s->append.data = data;

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

    /* Search for an available slot in our internal queue */
    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        if (!s->send.requests[i].pending) {
            s->send.requests[i].message = *message;
            s->send.requests[i].cb = cb;
            s->send.requests[i].data = data;
            s->send.requests[i].pending = true;
            return 0;
        }
    }

    return RAFT_ERR_IO_BUSY;
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

    stub->append.entries = NULL;
    stub->append.n_entries = 0;
    stub->append.data = NULL;
    stub->append.cb = NULL;

    stub->fault.countdown = -1;
    stub->fault.n = -1;

    memset(stub->send.requests, 0, sizeof stub->send.requests);

    io->data = stub;
    io->start = raft_io_stub__start;
    io->stop = raft_io_stub__stop;
    io->load = raft_io_stub__load;
    io->bootstrap = raft_io_stub__bootstrap;
    io->set_term = raft_io_stub__set_term;
    io->set_vote = raft_io_stub__set_vote;
    io->append = raft_io_stub__append;
    io->send = raft_io_stub__send;

    return 0;
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

static void raft_io_stub__append_cb(struct raft_io_stub *s)
{
    int status = 0;
    size_t n = s->append.n_entries;
    struct raft_entry *all_entries;
    const struct raft_entry *new_entries = s->append.entries;
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

    if (s->append.cb != NULL) {
        s->append.cb(s->append.data, status);
    }

    s->append.data = NULL;
    s->append.cb = NULL;
}

void raft_io_stub_flush(struct raft_io *io)
{
    struct raft_io_stub *s;
    size_t i;

    assert(io != NULL);

    s = io->data;

    if (s->append.cb != NULL) {
        raft_io_stub__append_cb(s);
    }

    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        struct raft_io_stub_request *request = &s->send.requests[i];

        if (!request->pending) {
            continue;
        }

        if (request->cb != NULL) {
            request->cb(request->data, 0);
        }
        request->pending = false;
    }
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

bool raft_io_stub_writing(struct raft_io *io)
{
    struct raft_io_stub *s;

    s = io->data;

    return (s->append.cb != NULL);
}

unsigned raft_io_stub_sending_n(struct raft_io *io, int type)
{
    struct raft_io_stub *s;
    unsigned n = 0;
    size_t i;

    assert(io != NULL);

    s = io->data;

    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        struct raft_io_stub_request *request = &s->send.requests[i];

        if (!request->pending) {
            continue;
        }

        if (request->message.type == type) {
            n++;
        }
    }

    return n;
}

struct raft_message *raft_io_stub_sending(struct raft_io *io,
                                          int type,
                                          unsigned i)
{
    struct raft_io_stub *s;
    unsigned n = 0;
    size_t j;

    assert(io != NULL);

    s = io->data;

    for (j = 0; j < RAFT_IO_STUB_MAX_REQUESTS; j++) {
        struct raft_io_stub_request *request = &s->send.requests[i];

        if (!request->pending) {
            continue;
        }

        if (request->message.type == type) {
            if (n == i) {
                return &request->message;
            }
	    n++;
        }
    }

    return NULL;
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
