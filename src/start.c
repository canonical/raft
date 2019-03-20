#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "entry.h"
#include "log.h"
#include "logging.h"
#include "rpc.h"
#include "snapshot.h"
#include "state.h"
#include "tick.h"

/* Restore the most recent configuration. */
static int restore_most_recent_configuration(struct raft *r,
                                             struct raft_entry *entry,
                                             raft_index index)
{
    struct raft_configuration configuration;
    int rc;
    raft_configuration_init(&configuration);
    rc = configuration__decode(&entry->buf, &configuration);
    if (rc != 0) {
        raft_configuration_close(&configuration);
        return rc;
    }
    raft_configuration_close(&r->configuration);
    r->configuration = configuration;
    r->configuration_index = index;
    return 0;
}

/* Restore the entries that were loaded from persistent storage. The most recent
 * configuration entry will be restored as well, if any. */
static int restore_entries(struct raft *r, struct raft_entry *entries, size_t n)
{
    struct raft_entry *conf = NULL;
    raft_index conf_index;
    size_t i;
    int rc;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        rc = log__append(&r->log, entry->term, entry->type, &entry->buf,
                         entry->batch);
        if (rc != 0) {
            goto err;
        }
        r->last_stored++;
        if (entry->type == RAFT_CONFIGURATION) {
            conf = entry;
            conf_index = r->last_stored;
        }
    }
    if (conf != NULL) {
        rc = restore_most_recent_configuration(r, conf, conf_index);
        if (rc != 0) {
            goto err;
        }
    }
    raft_free(entries);
    return 0;

 err:
    if (log__n_entries(&r->log) > 0) {
        log__discard(&r->log, log__first_index(&r->log));
    }
    return rc;
}

/* Automatically self-elect ourselves and convert to leader if we're the only
 * voting server in the configuration. */
static int maybe_self_elect(struct raft *r)
{
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

    /* Append the entries to the log, possibly restoring the last
     * configuration. */
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
