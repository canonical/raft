#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "log.h"
#include "logging.h"
#include "recv.h"
#include "snapshot.h"
#include "tick.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r->io, "start: " MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* Restore the most recent configuration. */
static int restoreMostRecentConfiguration(struct raft *r,
                                          struct raft_entry *entry,
                                          raft_index index)
{
    struct raft_configuration configuration;
    int rc;
    raft_configuration_init(&configuration);
    rc = configurationDecode(&entry->buf, &configuration);
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
static int restoreEntries(struct raft *r,
                          raft_index start_index,
                          struct raft_entry *entries,
                          size_t n)
{
    struct raft_entry *conf = NULL;
    raft_index conf_index;
    size_t i;
    int rc;
    logSeek(&r->log, start_index);
    r->last_stored = start_index - 1;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        rc = logAppend(&r->log, entry->term, entry->type, &entry->buf,
                       entry->batch);
        if (rc != 0) {
            goto err;
        }
        r->last_stored++;
        if (entry->type == RAFT_CHANGE) {
            conf = entry;
            conf_index = r->last_stored;
        }
    }
    if (conf != NULL) {
        rc = restoreMostRecentConfiguration(r, conf, conf_index);
        if (rc != 0) {
            goto err;
        }
    }
    raft_free(entries);
    return 0;

err:
    if (logNumOutstanding(&r->log) > 0) {
        logDiscard(&r->log, r->log.offset + 1);
    }
    return rc;
}

/* Automatically self-elect ourselves and convert to leader if we're the only
 * voting server in the configuration. */
static int maybe_self_elect(struct raft *r)
{
    const struct raft_server *server;
    int rc;
    server = configurationGet(&r->configuration, r->id);
    if (server == NULL || !server->voting ||
        configurationNumVoting(&r->configuration) > 1) {
        return 0;
    }
    debugf(r->io, "self elect and convert to leader");
    rc = convertToCandidate(r);
    if (rc != 0) {
        return rc;
    }
    rc = convertToLeader(r);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

int raft_start(struct raft *r)
{
    int rc;
    struct raft_snapshot *snapshot;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(logNumOutstanding(&r->log) == 0);
    assert(r->last_stored == 0);

    infof(r->io, "starting");
    rc = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &start_index, &entries, &n_entries);
    if (rc != 0) {
        return rc;
    }
    assert(start_index >= 1);

    /* If we have a snapshot, let's restore it, updating the start index. */
    if (snapshot != NULL) {
        tracef("restore snapshot with last index %llu and last term %llu",
               snapshot->index, snapshot->term);
        rc = snapshotRestore(r, snapshot);
        if (rc != 0) {
            snapshotDestroy(snapshot);
            entryBatchesDestroy(entries, n_entries);
            return rc;
        }
        logRestore(&r->log, snapshot->index, snapshot->term);
        raft_free(snapshot);
    } else if (n_entries > 0) {
        /* If we don't have a snapshot and the on-disk log is not empty, then
         * the first entry must be a configuration entry. */
        assert(start_index == 1);
        assert(entries[0].type == RAFT_CHANGE);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Append the entries to the log, possibly restoring the last
     * configuration. */
    tracef("restore %lu entries starting at %llu", n_entries, start_index);
    rc = restoreEntries(r, start_index, entries, n_entries);
    if (rc != 0) {
        entryBatchesDestroy(entries, n_entries);
        return rc;
    }

    /* Start the I/O backend. The tick callback is expected to fire every
     * r->heartbeat_timeout milliseconds and the recv callback whenever an RPC
     * is received. */
    rc = r->io->start(r->io, r->heartbeat_timeout, tick_cb, recvCb);
    if (rc != 0) {
        return rc;
    }

    convertToFollower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rc = maybe_self_elect(r);
    if (rc != 0) {
        return rc;
    }

    return 0;
}
