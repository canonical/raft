#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "entry.h"
#include "log.h"
#include "logging.h"
#include "rpc.h"
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

/* Restore the most recent configuration among the given entries, if any. */
static int restore_most_recent_configuration(struct raft *r,
                                             struct raft_entry *entries,
                                             size_t n)
{
    struct raft_configuration configuration;
    size_t i;
    int rc;
    for (i = n; i > 0; i--) {
        struct raft_entry *entry = &entries[i - 1];
        if (entry->type != RAFT_CONFIGURATION) {
            continue;
        }
        raft_configuration_init(&configuration);
        rc = configuration__decode(&entry->buf, &configuration);
        if (rc != 0) {
            raft_configuration_close(&configuration);
            return rc;
        }
        raft_configuration_close(&r->configuration);
        r->configuration = configuration;
        r->configuration_index = r->last_stored - (n - i);
        break;
    }
    return 0;
}

/* Restore the given entries that were loaded from persistent storage. */
static int restore_entries(struct raft *r, struct raft_entry *entries, size_t n)
{
    size_t i;
    int rc;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        rc = log__append(&r->log, entry->term, entry->type, &entry->buf,
                         entry->batch);
        if (rc != 0) {
            if (log__n_entries(&r->log) > 0) {
                log__discard(&r->log, log__first_index(&r->log));
            }
            return rc;
        }
    }
    raft_free(entries);
    return 0;
}

/* Automatically self-elect ourselves and convert to leader if we're the only
 * voting server in the configuration. */
static int maybe_self_elect(struct raft *r) {
    const struct raft_server *server;
    int rc;
    server = configuration__get(&r->configuration, r->id);
    if (server != NULL && server->voting &&
        configuration__n_voting(&r->configuration) == 1) {
        debugf(r->io, "self elect and convert to leader");
        rc = raft_state__convert_to_candidate(r);
        if (rc != 0) {
            return rc;
        }
        rc = raft_state__convert_to_leader(r);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

int raft_start(struct raft *r)
{
    int rc;
    struct raft_snapshot *snapshot;
    struct raft_entry *entries;
    size_t n_entries;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(log__n_entries(&r->log) == 0);
    assert(r->last_stored == 0);

    infof(r->io, "starting");

    rc = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &entries, &n_entries);
    if (rc != 0) {
        return rc;
    }

    /* If we have a snapshot, let's restore it, updating the start index. */
    if (snapshot != NULL) {
        rc = snapshot__restore(r, snapshot);
        if (rc != 0) {
            snapshot__destroy(snapshot);
            entry_batches__destroy(entries, n_entries);
            return rc;
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
    rc = restore_most_recent_configuration(r, entries, n_entries);
    if (rc != 0) {
        entry_batches__destroy(entries, n_entries);
        return rc;
    }

    /* Append the entries to the log. */
    rc = restore_entries(r, entries, n_entries);
    if (rc != 0) {
        entry_batches__destroy(entries, n_entries);
        return rc;
    }

    /* Initialize the tick timestamp. */
    r->last_tick = r->io->time(r->io);

    /* Start the I/O backend. The tick callback is expected to fire every
     * r->heartbeat_timeout milliseconds and the recv callback whenever an RPC
     * is received. */
    rc = r->io->start(r->io, r->heartbeat_timeout, tick_cb, rpc__recv_cb);
    if (rc != 0) {
        return rc;
    }

    raft_state__start_as_follower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rc = maybe_self_elect(r);
    if (rc != 0) {
      return rc;
    }

    return 0;
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
