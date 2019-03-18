#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "logging.h"
#include "rpc.h"
#include "rpc_append_entries.h"
#include "rpc_install_snapshot.h"
#include "rpc_request_vote.h"
#include "state.h"
#include "tick.h"

#define RAFT__DEFAULT_ELECTION_TIMEOUT 1000
#define RAFT__DEFAULT_HEARTBEAT_TIMEOUT 100
#define RAFT__DEFAULT_SNAPSHOT_THRESHOLD 1024

int raft_init(struct raft *r,
              struct raft_logger *logger,
              struct raft_io *io,
              struct raft_fsm *fsm,
              void *data,
              const unsigned id,
              const char *address)
{
    int i;
    int rv;

    assert(r != NULL);

    r->logger = logger;
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

    /* User-defined */
    r->data = data;

    /* Initial persistent server state. */
    r->current_term = 0;
    r->voted_for = 0;
    log__init(&r->log);

    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;

    r->election_timeout = RAFT__DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = RAFT__DEFAULT_HEARTBEAT_TIMEOUT;

    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;

    r->state = RAFT_UNAVAILABLE;

    for (i = 0; i < RAFT_EVENT_N; i++) {
        r->watchers[i] = NULL;
    }

    r->close_cb = NULL;

    r->snapshot.term = 0;
    r->snapshot.index = 0;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = RAFT__DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.put.data = NULL;

    rv = r->io->init(r->io, r->id, r->address);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void raft__close_cb(struct raft_io *io)
{
    struct raft *r = io->data;

    infof(r->io, "stopped");

    raft_free(r->address);
    raft_state__clear(r);
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
    raft__tick(r);
}

static const char *raft__message_names[] = {
    "append entries",
    "append entries result",
    "request vote",
    "request vote result",
};

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
            raft_warnf(r->logger, "rpc: unknown message type type: %d",
                       message->type);
            return;
    };

    if (rv != 0 && rv != RAFT_ERR_IO_CONNECT) {
        raft_errorf(r->logger, "rpc %s: %s", raft__message_names[message->type],
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
    size_t i;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);

    infof(r->io, "starting");

    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);

    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &entries, &n_entries);
    if (rv != 0) {
        goto err;
    }

    if (snapshot == NULL) {
        start_index = 1;
    } else {
        start_index = snapshot->index + 1;
        assert(snapshot->n_bufs == 1);
        rv = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
        if (rv != 0) {
            goto err_after_load;
        }
        r->commit_index = snapshot->index;
        r->last_applied = snapshot->index;
        r->snapshot.index = snapshot->index;
        r->snapshot.term = snapshot->term;
        r->configuration = snapshot->configuration;
        r->configuration_index = snapshot->configuration_index;
        raft_free(snapshot->bufs);
        raft_free(snapshot);
    }

    /* If the start index is 1 and the log is not empty, then the first entry
     * must be a configuration*/
    if (start_index == 1 && n_entries > 0) {
        assert(snapshot == NULL);
        assert(entries[0].type == RAFT_CONFIGURATION);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Index of the last entry we have on disk. */
    r->last_stored = start_index + n_entries - 1;

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
    log__set_offset(&r->log, start_index - 1);
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
    r->io->close(r->io, raft__close_cb);
}

void raft_set_election_timeout(struct raft *r, const unsigned election_timeout)
{
    r->election_timeout = election_timeout;
    raft_election__reset_timer(r);
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
