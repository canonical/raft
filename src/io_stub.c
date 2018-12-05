#include <assert.h>
#include <string.h>

#include "../include/raft.h"

/**
 * Maximum number of pending I/O requests. This should be enough for testing
 * purposes.
 */
#define RAFT_IO_STUB_MAX_REQUESTS 64

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
    raft_index first_index;     /* Index of the first persisted entry */
    struct raft_entry *entries; /* Array or persisted entries */
    size_t n;                   /* Size of the persisted entries array */

    /* Queue of in-flight asynchronous I/O requests. */
    unsigned request_ids[RAFT_IO_STUB_MAX_REQUESTS];

    /* Parameters passed via raft_io->init */
    struct raft_io_queue *queue;
    void *p;
    void (*tick)(void *, const unsigned);
    void (*notify)(void *, const unsigned, const int);

};

static void raft_io_stub__init(struct raft_io *io,
                               struct raft_io_queue *queue,
                               void *p,
                               void (*tick)(void *, const unsigned),
                               void (*notify)(void *,
                                              const unsigned,
                                              const int))
{
    struct raft_io_stub *s;

    s = io->data;

    s->queue = queue;
    s->p = p;
    s->tick = tick;
    s->notify = notify;
}

static int raft_io_stub__start(struct raft_io *io, const unsigned msecs)
{
    (void)msecs;

    assert(io != NULL);

    return 0;
}

static int raft_io_stub__stop(struct raft_io *io)
{
    assert(io != NULL);

    return 0;
}

static void raft_io_stub__close(struct raft_io *io)
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

static int raft_io_stub__bootstrap(struct raft_io_stub *s,
                                   struct raft_io_request *request)
{
    struct raft_entry *entries;

    if (s->term != 0) {
        return RAFT_ERR_BUSY;
    }

    assert(s->voted_for == 0);
    assert(s->first_index == 0);
    assert(s->entries == NULL);
    assert(s->n == 0);

    entries = raft_calloc(1, sizeof *s->entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    entries[0].term = 1;
    entries[0].type = RAFT_LOG_CONFIGURATION;
    entries[0].buf.len = request->args.bootstrap.conf.len;
    entries[0].buf.base = raft_malloc(entries[0].buf.len);

    if (entries[0].buf.base == NULL) {
        raft_free(entries);
        return RAFT_ERR_NOMEM;
    }

    s->term = 1;
    s->voted_for = 0;
    s->first_index = 1;
    s->entries = entries;
    s->n = 1;

    return 0;
}

static int raft_io_stub__read_state(struct raft_io_stub *s,
                                    struct raft_io_request *request)
{
    struct raft_entry *entries;
    size_t n;
    size_t i;
    void *batch;
    void *cursor;
    size_t size = 0; /* Size of the batch */

    request->result.read_state.term = s->term;
    request->result.read_state.voted_for = s->voted_for;
    request->result.read_state.first_index = s->first_index;

    if (s->n == 0) {
        request->result.read_state.entries = NULL;
        request->result.read_state.n = 0;
        return 0;
    }

    /* Make a copy of the persisted entries, storing their data into a single
     * batch. */
    n = s->n;
    entries = raft_calloc(n, sizeof *entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = 0; i < n; i++) {
        size += s->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        raft_free(entries);
        return RAFT_ERR_NOMEM;
    }

    cursor = batch;

    for (i = 0; i < n; i++) {
        memcpy(cursor, s->entries[i].buf.base, s->entries[i].buf.len);

        entries[i].term = s->entries[i].term;
        entries[i].type = s->entries[i].type;
        entries[i].buf.base = cursor;
        entries[i].buf.len = s->entries[i].buf.len;
        entries[i].batch = batch;

        cursor += entries[i].buf.len;
    }

    request->result.read_state.entries = entries;
    request->result.read_state.n = n;

    return 0;
}

static int raft_io_stub__write_term(struct raft_io_stub *s,
                                    struct raft_io_request *request)
{
    s->term = request->args.write_term.term;
    s->voted_for = 0;

    return 0;
}

static int raft_io_stub__write_vote(struct raft_io_stub *s,
                                    struct raft_io_request *request)
{
    s->voted_for = request->args.write_vote.server_id;

    return 0;
}

static int raft_io_stub__write_log(struct raft_io_stub *s,
                                   const unsigned request_id)
{
    size_t i;

    /* Search for an available slot in our internal queue */
    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        if (s->request_ids[i] == 0) {
            s->request_ids[i] = request_id;
            return 0;
        }
    }

    return RAFT_ERR_IO_BUSY;
}

static int raft_io_stub__submit(struct raft_io *io, const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_stub *s;
    int rv;

    assert(io != NULL);

    s = io->data;

    request = raft_io_queue_get(s->queue, request_id);

    assert(request != NULL);

    switch (request->type) {
        case RAFT_IO_BOOTSTRAP:
            rv = raft_io_stub__bootstrap(s, request);
            break;
        case RAFT_IO_READ_STATE:
            rv = raft_io_stub__read_state(s, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_stub__write_term(s, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_stub__write_vote(s, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_stub__write_log(s, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_stub_init(struct raft_io *io)
{
    struct raft_io_stub *stub;

    assert(io != NULL);

    stub = raft_malloc(sizeof *stub);
    if (stub == NULL) {
        return RAFT_ERR_NOMEM;
    }

    stub->time = 0;
    stub->term = 0;
    stub->voted_for = 0;
    stub->entries = NULL;
    stub->first_index = 0;
    stub->n = 0;

    memset(&stub->request_ids, 0, sizeof stub->request_ids);

    io->data = stub;
    io->init = raft_io_stub__init;
    io->start = raft_io_stub__start;
    io->stop = raft_io_stub__stop;
    io->close = raft_io_stub__close;
    io->submit = raft_io_stub__submit;

    return 0;
}

static void raft_io_stub__write_log_cb(struct raft_io_stub *io,
                                       struct raft_io_request *request)
{
    size_t n = request->args.write_log.n;
    struct raft_entry *all_entries;
    const struct raft_entry *new_entries = request->args.write_log.entries;
    size_t i;

    assert(new_entries != NULL);
    assert(n > 0);

    /* If it's the very first write, set the initial index. */
    if (io->first_index == 0) {
        io->first_index = 1;
    }

    /* Allocate an array for the old entries plus ne the new ons. */
    all_entries = raft_malloc((io->n + n) * sizeof *all_entries);
    assert(all_entries != NULL);

    /* If it's not the very first write, copy the existing entries into the new
     * array. */
    if (io->n > 0) {
        assert(io->entries != NULL);
        memcpy(all_entries, io->entries, io->n * sizeof *io->entries);
    }

    /* Copy the new entries into the new array. */
    memcpy(all_entries + io->n, new_entries, n * sizeof *new_entries);
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &all_entries[io->n + i];

        /* Make a copy of the actual entry data. */
        entry->buf.base = raft_malloc(entry->buf.len);
        assert(entry->buf.base != NULL);
        memcpy(entry->buf.base, new_entries[i].buf.base, entry->buf.len);
    }

    if (io->entries != NULL) {
        raft_free(io->entries);
    }

    io->entries = all_entries;
    io->n += n;
}

void raft_io_stub_advance(struct raft_io *io, unsigned msecs)
{
    struct raft_io_stub *s;

    assert(io != NULL);

    s = io->data;

    s->time += msecs;

    s->tick(s->p, msecs);
}

void raft_io_stub_flush(struct raft_io *io)
{
    struct raft_io_stub *s;
    size_t i;

    assert(io != NULL);

    s = io->data;

    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        unsigned request_id = s->request_ids[i];
        struct raft_io_request *request;

        if (request_id == 0) {
            continue;
        }

        request = raft_io_queue_get(s->queue, request_id);

        switch (request->type) {
            case RAFT_IO_WRITE_LOG:
                raft_io_stub__write_log_cb(s, request);
                break;
            default:
                assert(0);
                break;
        }

        s->notify(s->p, request_id, 0);
    }
}
