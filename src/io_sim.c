#include <assert.h>
#include <string.h>

#include "../include/raft.h"

/**
 * Maximum number of pending I/O requests. This should be enough for testing
 * purposes.
 */
#define RAFT_IO_SIM_MAX_REQUESTS 64

/**
 * Raft I/O implementation simulating all operations in-memory.
 */
struct raft_io_sim
{
    struct raft *raft;

    /* Term and vote */
    raft_term term;
    unsigned voted_for;

    /* Log */
    raft_index first_index;     /* Index of the first entry */
    struct raft_entry *entries; /* Entries array */
    size_t n;                   /* Size of the entries array */

    /* Queue of in-flight asynchronous I/O requests. */
    unsigned request_ids[RAFT_IO_SIM_MAX_REQUESTS];
};

static int raft_io_sim__start(struct raft *r, unsigned tick)
{
    assert(r != NULL);
    assert(r->state == RAFT_STATE_UNAVAILABLE);

    assert(tick > 0);

    return 0;
}

static int raft_io_sim__stop(struct raft *r)
{
    assert(r != NULL);

    return 0;
}

static void raft_io_sim__close(struct raft *r)
{
    struct raft_io_sim *io;
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

static int raft_io_sim__read_state(struct raft_io_sim *io,
                                   struct raft_io_request *request)
{
    request->result.read_state.term = io->term;
    request->result.read_state.voted_for = io->voted_for;
    request->result.read_state.first_index = io->first_index;
    request->result.read_state.n_entries = io->n;

    return 0;
}

static int raft_io_sim__write_term(struct raft_io_sim *io,
                                   struct raft_io_request *request)
{
    io->term = request->args.write_term.term;
    io->voted_for = 0;

    return 0;
}

static int raft_io_sim__write_vote(struct raft_io_sim *io,
                                   struct raft_io_request *request)
{
    io->voted_for = request->args.write_vote.server_id;

    return 0;
}

static int raft_io_sim__write_log(struct raft_io_sim *io,
                                  const unsigned request_id)
{
    size_t i;

    for (i = 0; i < RAFT_IO_SIM_MAX_REQUESTS; i++) {
        if (io->request_ids[i] == 0) {
            io->request_ids[i] = request_id;
            return 0;
        }
    }

    return RAFT_ERR_IO_BUSY;
}

static int raft_io_sim__submit(struct raft *r, const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_sim *io;
    int rv;

    assert(r != NULL);

    io = r->io_.data;

    request = raft_io_queue_get(r, request_id);

    assert(request != NULL);

    switch (request->type) {
        case RAFT_IO_READ_STATE:
            rv = raft_io_sim__read_state(io, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_sim__write_term(io, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_sim__write_vote(io, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_sim__write_log(io, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_sim_init(struct raft *r)
{
    struct raft_io_sim *io;

    assert(r != NULL);

    io = raft_malloc(sizeof *io);
    if (io == NULL) {
        return RAFT_ERR_NOMEM;
    }

    io->raft = r;
    io->term = 0;
    io->voted_for = 0;
    io->entries = NULL;
    io->first_index = 0;
    io->n = 0;

    memset(&io->request_ids, 0, sizeof io->request_ids);

    r->io_.data = io;
    r->io_.start = raft_io_sim__start;
    r->io_.stop = raft_io_sim__stop;
    r->io_.close = raft_io_sim__close;
    r->io_.submit = raft_io_sim__submit;

    return 0;
}

static void raft_io_sim__write_log_cb(struct raft_io_sim *io,
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

void raft_io_sim_flush(struct raft *r)
{
    struct raft_io_sim *io;
    size_t i;

    assert(r != NULL);

    io = r->io_.data;

    for (i = 0; i < RAFT_IO_SIM_MAX_REQUESTS; i++) {
        unsigned request_id = io->request_ids[i];
        struct raft_io_request *request;

        if (request_id == 0) {
            continue;
        }

        request = raft_io_queue_get(r, request_id);

        switch (request->type) {
            case RAFT_IO_WRITE_LOG:
                raft_io_sim__write_log_cb(io, request);
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
