#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "logging.h"
#include "rpc_append_entries.h"
#include "rpc_install_snapshot.h"
#include "rpc_request_vote.h"
#include "snapshot.h"
#include "state.h"
#include "tick.h"

#define DEFAULT_ELECTION_TIMEOUT 1000 /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100 /* One tenth of a second */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024

/* Set to 1 to enable debug logging. */
#if 0
#define tracef(MSG, ...) tracef(r->io, MSG, __VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const unsigned id,
              const char *address)
{
    int i;
    int rv;

    assert(r != NULL);

    r->io = io;
    r->io->data = r;
    r->fsm = fsm;
    r->id = id;
    /* Make a copy of the address */
    r->address = raft_malloc(strlen(address) + 1);
    if (r->address == NULL) {
        return RAFT_ENOMEM;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    log__init(&r->log);
    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->election_timeout_rand = 0;
    r->last_tick = 0;
    r->timer = 0;
    r->snapshot.term = 0;
    r->snapshot.index = 0;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.put.data = NULL;
    for (i = 0; i < RAFT_EVENT_N; i++) {
        r->watchers[i] = NULL;
    }
    r->close_cb = NULL;
    rv = r->io->init(r->io, r->id, r->address);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

static void io_close_cb(struct raft_io *io)
{
    struct raft *r = io->data;
    infof(r->io, "stopped");

    raft_free(r->address);
    log__close(&r->log);
    raft_configuration_close(&r->configuration);

    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}

static void tick_cb(struct raft_io *io)
{
    struct raft *r;
    r = io->data;
    tick(r);
}

static const char *message_descs[] = {"append entries", "append entries result",
                                      "request vote", "request vote result",
                                      "install snapshot"};

static void recv_cb(struct raft_io *io, struct raft_message *message)
{
    struct raft *r;
    int rv;
    r = io->data;

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = raft_rpc__recv_append_entries(r, message->server_id,
                                               message->server_address,
                                               &message->append_entries);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            rv = raft_rpc__recv_append_entries_result(
                r, message->server_id, message->server_address,
                &message->append_entries_result);
            break;
        case RAFT_IO_REQUEST_VOTE:
            rv = raft_rpc__recv_request_vote(r, message->server_id,
                                             message->server_address,
                                             &message->request_vote);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            rv = raft_rpc__recv_request_vote_result(
                r, message->server_id, message->server_address,
                &message->request_vote_result);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = raft_rpc__recv_install_snapshot(r, message->server_id,
                                                 message->server_address,
                                                 &message->install_snapshot);
            break;
        default:
            warnf(r->io, "rpc: unknown message type type: %d", message->type);
            return;
    };

    if (rv != 0 && rv != RAFT_ERR_IO_CONNECT) {
        errorf(r->io, "rpc %s: %s", message_descs[message->type],
               raft_strerror(rv));
    }
}

int raft_start(struct raft *r)
{
    int rv;
    struct raft_snapshot *snapshot;
    struct raft_entry *entries;
    size_t n_entries;
    raft_index start_index;
    const struct raft_server *server;
    size_t i;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(log__n_entries(&r->log) == 0);
    assert(r->last_stored == 0);

    infof(r->io, "starting");


    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &entries, &n_entries);
    if (rv != 0) {
        goto err;
    }

    /* If we have a snapshot, let's restore it, updating the start index. */
    start_index = 1;
    if (snapshot != NULL) {
        start_index = snapshot->index + 1;
        rv = snapshot__restore(r, snapshot);
        if (rv != 0) {
            goto err_after_load;
        }
    } else if (n_entries > 0) {
        /* If we don't have a snapshot and the on-disk log is not empty, then
         * the first entry must be a configuration entry. */
        assert(entries[0].type == RAFT_CONFIGURATION);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Index of the last entry we have on disk. */
    r->last_stored += n_entries;

    /* Look for the most recent configuration entry, if any. */
    for (i = n_entries; i > 0; i--) {
        struct raft_entry *entry = &entries[i - 1];

        if (entry->type != RAFT_CONFIGURATION) {
            continue;
        }

        raft_configuration_close(&r->configuration);
        raft_configuration_init(&r->configuration);

        rv = configuration__decode(&entry->buf, &r->configuration);
        if (rv != 0) {
            goto err_after_load;
        }

        r->configuration_index = start_index + i - 1;

        break;
    }

    /* Append the entries to the log. */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];

        rv = log__append(&r->log, entry->term, entry->type, &entry->buf,
                         entry->batch);
        if (rv != 0) {
            goto err_after_load;
        }
    }

    /* Initialize the tick timestamp. */
    r->last_tick = r->io->time(r->io);

    /* Start the I/O backend. The tick callback is expected to fire every
     * r->heartbeat_timeout milliseconds and the recv callback whenever an RPC
     * is received. */
    rv = r->io->start(r->io, r->heartbeat_timeout, tick_cb, recv_cb);
    if (rv != 0) {
        goto err_after_load;
    }

    if (entries != NULL) {
        raft_free(entries);
    }

    raft_state__start_as_follower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, so we stay follower. */
    server = configuration__get(&r->configuration, r->id);
    if (server != NULL && server->voting &&
        configuration__n_voting(&r->configuration) == 1) {
        debugf(r->io, "self elect and convert to leader");
        rv = raft_state__convert_to_candidate(r);
        if (rv != 0) {
            return rv;
        }
        rv = raft_state__convert_to_leader(r);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;

err_after_load:
    if (entries != NULL) {
        raft_free(entries[0].batch);
        raft_free(entries);
    }

err:
    assert(rv != 0);

    return rv;
}

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
    assert(r != NULL);
    assert(r->close_cb == NULL);

    r->close_cb = cb;

    raft_state__clear(r);
    r->state = RAFT_UNAVAILABLE;
    r->io->close(r->io, io_close_cb);
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
    raft_election__reset_timer(r);
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

const char *raft_state_name(struct raft *r)
{
    return raft_state_names[r->state];
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    assert(r->state == RAFT_UNAVAILABLE);

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}
