#include "../include/raft.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "entry.h"
#include "err.h"
#include "log.h"
#include "recv.h"
#include "snapshot.h"
#include "tick.h"
#include "tracing.h"

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

/* Restore the most recent configurations. */
static int restoreConfigurations(struct raft *r, raft_index prev_index,
                                 raft_index last_index, struct raft_entry *last)
{
    struct raft_configuration last_conf;
    int rv;

    /* No configuration entry loaded, nothing to do */
    if (last == NULL) {
        assert(prev_index == 0);
        return 0;
    } else {
        /* There is a latest configuration, we can't know if it's
         * committed or not. Backup the configuration restored from the snapshot
         * or noop in case there was no snapshot. */
        configurationBackup(r, &r->configuration);
        raft_configuration_init(&last_conf);
        rv = configurationDecode(&last->buf, &last_conf);
        if (rv != 0) {
            raft_configuration_close(&last_conf);
            return rv;
        }
        configurationClose(&r->configuration);
        r->configuration = last_conf;
        r->configuration_uncommitted_index = last_index;

        /* If the last configuration is the first entry in the log, we know it's
         * the bootstrap configuration and it's committed by default. */
        if (last_index == 1) {
            assert(prev_index == 0);
            r->configuration_index = 1;
            r->configuration_uncommitted_index = 0;
        }

        /* If there is a previous configuration it must have been committed as
         * we don't allow multiple uncommitted configurations. */
        if (prev_index != 0) {
            r->configuration_index = prev_index;
        }
    }

    configurationTrace(r, &r->configuration, "restore most recent configuration");
    return 0;
}

/* Restore the entries that were loaded from persistent storage. The most recent
 * configuration entry will be restored as well, if any.
 *
 * Note that we cannot know if the last configuration in the log was committed
 * or not, therefore we also need to track the second-to-last configuration
 * entry. This second-to-last entry is committed by default as raft doesn't
 * allow multipled uncommitted configuration entries and is used in case of
 * configuration rollback scenarios. */
static int restoreEntries(struct raft *r,
                          raft_index snapshot_index,
                          raft_term snapshot_term,
                          raft_index start_index,
                          struct raft_entry *entries,
                          size_t n)
{
    struct raft_entry *last_conf = NULL;
    raft_index last_conf_index = 0;
    raft_index prev_conf_index = 0;
    size_t i;
    int rv;
    logStart(r->log, snapshot_index, snapshot_term, start_index);
    r->last_stored = start_index - 1;
    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        rv = logAppend(r->log, entry->term, entry->type, &entry->buf,
                       entry->batch);
        if (rv != 0) {
            goto err;
        }
        r->last_stored++;
        /* Only take configurations into account that are newer than the
         * configuration restored from the snapshot. */
        if (entry->type == RAFT_CHANGE && r->last_stored > r->configuration_index) {
            prev_conf_index = last_conf_index;
            last_conf = entry;
            last_conf_index = r->last_stored;
        }
    }

    rv = restoreConfigurations(r, prev_conf_index, last_conf_index, last_conf);
    if (rv != 0) {
        goto err;
    }

    raft_free(entries);
    return 0;

err:
    if (logNumEntries(r->log) > 0) {
        logDiscard(r->log, r->log->offset + 1);
    }
    return rv;
}

/* If we're the only voting server in the configuration, automatically
 * self-elect ourselves and convert to leader without waiting for the election
 * timeout. */
static int maybeSelfElect(struct raft *r)
{
    const struct raft_server *server;
    int rv;
    server = configurationGet(&r->configuration, r->id);
    if (server == NULL || server->role != RAFT_VOTER ||
        configurationVoterCount(&r->configuration) > 1) {
        return 0;
    }
    /* Converting to candidate will notice that we're the only voter and
     * automatically convert to leader. */
    rv = convertToCandidate(r, false /* disrupt leader */);
    if (rv != 0) {
        return rv;
    }
    assert(r->state == RAFT_LEADER);
    return 0;
}

int raft_start(struct raft *r)
{
    struct raft_snapshot *snapshot;
    raft_index snapshot_index = 0;
    raft_term snapshot_term = 0;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE);
    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);
    assert(r->install_snapshot_timeout != 0);
    assert(logNumEntries(r->log) == 0);
    assert(logSnapshotIndex(r->log) == 0);
    assert(r->last_stored == 0);

    tracef("starting");
    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &snapshot,
                     &start_index, &entries, &n_entries);
    if (rv != 0) {
        ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
        return rv;
    }
    assert(start_index >= 1);
    tracef("current_term:%llu voted_for:%llu start_index:%llu n_entries:%zu",
           r->current_term, r->voted_for, start_index, n_entries);

    /* If we have a snapshot, let's restore it. */
    if (snapshot != NULL) {
        tracef("restore snapshot with last index %llu and last term %llu",
               snapshot->index, snapshot->term);
        rv = snapshotRestore(r, snapshot);
        if (rv != 0) {
            snapshotDestroy(snapshot);
            entryBatchesDestroy(entries, n_entries);
            return rv;
        }
        snapshot_index = snapshot->index;
        snapshot_term = snapshot->term;
        raft_free(snapshot);

        /* Enable configuration rollback if the next configuration after installing
         * this snapshot needs to be rolled back. */
        rv = configurationBackup(r, &r->configuration);
        if (rv != 0) {
            tracef("failed to backup current configuration.");
            return rv;
        }
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
    tracef("restore %zu entries starting at %llu", n_entries, start_index);
    rv = restoreEntries(r, snapshot_index, snapshot_term, start_index, entries,
                        n_entries);
    if (rv != 0) {
        entryBatchesDestroy(entries, n_entries);
        return rv;
    }

    /* Start the I/O backend. The tickCb function is expected to fire every
     * r->heartbeat_timeout milliseconds and recvCb whenever an RPC is
     * received. */
    rv = r->io->start(r->io, r->heartbeat_timeout, tickCb, recvCb);
    if (rv != 0) {
        tracef("io start failed %d", rv);
        return rv;
    }

    /* By default we start as followers. */
    convertToFollower(r);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader right away. If that is not us, we're either joining the cluster
     * or we're simply configured as non-voter, and we'll stay follower. */
    rv = maybeSelfElect(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

#undef tracef
