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
    struct raft *raft;
    int (*tick)(struct raft *, const unsigned);

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
};

static int raft_io_stub__start(struct raft *r,
                               const unsigned msecs,
                               int (*tick)(struct raft *, const unsigned))
{
    struct raft_io_stub *io;

    (void)msecs;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_UNAVAILABLE);
    assert(tick != NULL);

    io = r->io_.data;
    io->tick = tick;

    return 0;
}

static int raft_io_stub__stop(struct raft *r)
{
    assert(r != NULL);

    return 0;
}

static void raft_io_stub__close(struct raft *r)
{
    struct raft_io_stub *io;
    size_t i;

    assert(r != NULL);

    io = r->io_.data;

    for (i = 0; i < io->n; i++) {
        struct raft_entry *entry = &io->entries[i];
        raft_free(entry->buf.base);
    }

    if (io->entries != NULL) {
        raft_free(io->entries);
    }

    raft_free(io);
}

static int raft_io_stub__bootstrap(struct raft_io_stub *io,
                                   struct raft_io_request *request)
{
    struct raft_entry *entries;

    if (io->term != 0) {
        return RAFT_ERR_BUSY;
    }

    assert(io->voted_for == 0);
    assert(io->first_index == 0);
    assert(io->entries == NULL);
    assert(io->n == 0);

    entries = raft_calloc(1, sizeof *io->entries);
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

    io->term = 1;
    io->voted_for = 0;
    io->first_index = 1;
    io->entries = entries;
    io->n = 1;

    return 0;
}

static int raft_io_stub__read_state(struct raft_io_stub *io,
                                    struct raft_io_request *request)
{
    request->result.read_state.term = io->term;
    request->result.read_state.voted_for = io->voted_for;
    request->result.read_state.first_index = io->first_index;
    request->result.read_state.n_entries = io->n;

    return 0;
}

static int raft_io_stub__read_log(struct raft_io_stub *io,
                                  struct raft_io_request *request)
{
    struct raft_entry *entries;
    size_t n;
    size_t i;
    void *batch;
    void *cursor;
    size_t size = 0; /* Size of the batch */

    if (io->n == 0) {
        request->result.read_log.entries = NULL;
        request->result.read_log.n = 0;
        return 0;
    }

    /* Make a copy of the persisted entries, storing their data into a single
     * batch. */
    n = io->n;
    entries = raft_calloc(n, sizeof *entries);
    if (entries == NULL) {
        return RAFT_ERR_NOMEM;
    }

    for (i = 0; i < n; i++) {
        size += io->entries[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        raft_free(entries);
        return RAFT_ERR_NOMEM;
    }

    cursor = batch;

    for (i = 0; i < n; i++) {
        memcpy(cursor, io->entries[i].buf.base, io->entries[i].buf.len);

        entries[i].term = io->entries[i].term;
        entries[i].type = io->entries[i].type;
        entries[i].buf.base = cursor;
        entries[i].buf.len = io->entries[i].buf.len;
        entries[i].batch = batch;

        cursor += entries[i].buf.len;
    }

    request->result.read_log.entries = entries;
    request->result.read_log.n = n;

    return 0;
}

static int raft_io_stub__write_term(struct raft_io_stub *io,
                                    struct raft_io_request *request)
{
    io->term = request->args.write_term.term;
    io->voted_for = 0;

    return 0;
}

static int raft_io_stub__write_vote(struct raft_io_stub *io,
                                    struct raft_io_request *request)
{
    io->voted_for = request->args.write_vote.server_id;

    return 0;
}

static int raft_io_stub__write_log(struct raft_io_stub *io,
                                   const unsigned request_id)
{
    size_t i;

    /* Search for an available slot in our internal queue */
    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        if (io->request_ids[i] == 0) {
            io->request_ids[i] = request_id;
            return 0;
        }
    }

    return RAFT_ERR_IO_BUSY;
}

static int raft_io_stub__submit(struct raft *r, const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_stub *io;
    int rv;

    assert(r != NULL);

    io = r->io_.data;

    request = raft_io_queue_get_(r, request_id);

    assert(request != NULL);

    switch (request->type) {
        case RAFT_IO_BOOTSTRAP:
            rv = raft_io_stub__bootstrap(io, request);
            break;
        case RAFT_IO_READ_STATE:
            rv = raft_io_stub__read_state(io, request);
            break;
        case RAFT_IO_READ_LOG:
            rv = raft_io_stub__read_log(io, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_stub__write_term(io, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_stub__write_vote(io, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_stub__write_log(io, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_stub_init(struct raft *r)
{
    struct raft_io_stub *io;

    assert(r != NULL);

    io = raft_malloc(sizeof *io);
    if (io == NULL) {
        return RAFT_ERR_NOMEM;
    }

    io->time = 0;
    io->raft = r;
    io->term = 0;
    io->voted_for = 0;
    io->entries = NULL;
    io->first_index = 0;
    io->n = 0;

    memset(&io->request_ids, 0, sizeof io->request_ids);

    r->io_.data = io;
    r->io_.start = raft_io_stub__start;
    r->io_.stop = raft_io_stub__stop;
    r->io_.close = raft_io_stub__close;
    r->io_.submit = raft_io_stub__submit;

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

int raft_io_stub_advance(struct raft *r, unsigned msecs)
{
    struct raft_io_stub *io;
    int rv;

    assert(r != NULL);

    io = r->io_.data;

    io->time += msecs;

    rv = io->tick(io->raft, msecs);

    return rv;
}

void raft_io_stub_flush(struct raft *r)
{
    struct raft_io_stub *io;
    size_t i;

    assert(r != NULL);

    io = r->io_.data;

    for (i = 0; i < RAFT_IO_STUB_MAX_REQUESTS; i++) {
        unsigned request_id = io->request_ids[i];
        struct raft_io_request *request;

        if (request_id == 0) {
            continue;
        }

        request = raft_io_queue_get_(r, request_id);

        switch (request->type) {
            case RAFT_IO_WRITE_LOG:
                raft_io_stub__write_log_cb(io, request);
                break;
            default:
                assert(0);
                break;
        }

        if (request->cb != NULL) {
            request->cb(r, request_id, 0);
        }
    }
}
