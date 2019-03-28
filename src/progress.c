#include "progress.h"
#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "logging.h"

#ifndef max
#define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef min
#define min(a, b) ((a) < (b) ? (a) : (b))
#endif

static void init_progress(struct raft_progress *p, raft_index last_index)
{
    p->next_index = last_index + 1;
    p->match_index = 0;
    p->snapshot_index = 0;
    p->recent_send = false;
    p->recent_recv = false;
    p->state = PROGRESS__PROBE;
}

int progress__build_array(struct raft *r)
{
    struct raft_progress *p;
    unsigned i;
    raft_index last_index = log__last_index(&r->log);
    p = raft_malloc(r->configuration.n * sizeof *p);
    if (p == NULL) {
        return RAFT_ENOMEM;
    }
    for (i = 0; i < r->configuration.n; i++) {
        init_progress(&p[i], last_index);
    }
    r->leader_state.progress = p;
    return 0;
}

int progress__rebuild_array(struct raft *r,
                            const struct raft_configuration *configuration)
{
    raft_index last_index = log__last_index(&r->log);
    struct raft_progress *p;
    unsigned i;

    p = raft_malloc(configuration->n * sizeof *p);
    if (p == NULL) {
        return RAFT_ENOMEM;
    }

    /* First copy the progress information for the servers that exists both in
     * the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        unsigned id = r->configuration.servers[i].id;
        size_t j = configuration__index_of(configuration, id);
        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }
        p[j] = r->leader_state.progress[i];
    }

    /* Then reset the replication state for servers that are present in the new
     * configuration, but not in the current one. */
    for (i = 0; i < configuration->n; i++) {
        unsigned id = configuration->servers[i].id;
        size_t j = configuration__index_of(&r->configuration, id);
        if (j < r->configuration.n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }
        assert(j == r->configuration.n);
        init_progress(&p[i], last_index);
    }

    raft_free(r->leader_state.progress);
    r->leader_state.progress = p;

    return 0;
}

bool progress__check_quorum(struct raft *r)
{
    unsigned i;
    unsigned contacts = 0;

    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        struct raft_progress *progress = &r->leader_state.progress[i];
        bool recent_recv = progress->recent_recv;
        progress->recent_recv = false;
        if (!server->voting) {
            continue;
        }
        if (server->id == r->id) {
            contacts++;
            continue;
        }
        if (recent_recv) {
            contacts++;
        }
    }

    return contacts > configuration__n_voting(&r->configuration) / 2;
}

static struct raft_progress *get(struct raft *r,
                                 const struct raft_server *server)
{
    unsigned server_index;
    server_index = configuration__index_of(&r->configuration, server->id);
    assert(server_index < r->configuration.n);
    return &r->leader_state.progress[server_index];
}

int progress__state(struct raft *r, const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    return p->state;
}

raft_index progress__next_index(struct raft *r,
                                const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    /* If we have sent a snapshot, let's send only entries after the
     * snapshot. */
    if (p->state == PROGRESS__SNAPSHOT) {
        return p->snapshot_index + 1;
    }
    return p->next_index;
}

/* Return false if the given rejected index comes from an out of order
 * message. Otherwise it decreases the progress next index to min(rejected,
 * last) and returns true. */
bool progress__maybe_decrement(struct raft *r,
                               const struct raft_server *server,
                               raft_index rejected,
                               raft_index last_index)
{
    struct raft_progress *p = get(r, server);
    if (p->state == PROGRESS__PIPELINE) {
        /* The rejection must be stale if the rejected index is smaller than the
         * matched one. */
        if (rejected <= p->match_index) {
            debugf(r->io, "match index is up to date -> ignore ");
            return false;
        }
        /* Directly decrease next to match + 1 */
        p->next_index = p->match_index + 1;
        return true;
    }

    /* The rejection must be stale or spurious (e.g. when the follower rejects
     * heartbeats sent concurrently with a snapshot) if the rejected index does
     * not match the next index minus one. */
    if (rejected != p->next_index - 1) {
        debugf(r->io,
               "rejected index %llu different from next index %lld -> ignore ",
               rejected, p->next_index);
        return false;
    }

    p->next_index = min(rejected, last_index + 1);
    p->next_index = max(p->next_index, 1);

    return true;
}

bool progress__maybe_update(struct raft *r,
                            const struct raft_server *server,
                            const raft_index last_index)
{
    struct raft_progress *p = get(r, server);
    bool updated = false;
    if (p->match_index < last_index) {
        p->match_index = last_index;
        updated = true;
    }
    if (p->next_index < last_index + 1) {
        p->next_index = last_index + 1;
    }
    if (updated) {
        debugf(r->io, "new match/next idx for server %ld: %ld/%ld", server->id,
               p->match_index, p->next_index);
    }
    return updated;
}

void progress__mark_recent_recv(struct raft *r,
                                const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    p->recent_recv = true;
}

void progress__mark_recent_send(struct raft *r,
                                const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    p->recent_send = true;
}

void progress__to_probe(struct raft *r, const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);

    /* If the current state is snapshot, we know that the pending snapshot has
     * been sent to this peer successfully, so we probe from snapshot_index +
     * 1.*/
    if (p->state == PROGRESS__SNAPSHOT) {
        assert(p->snapshot_index > 0);
        p->next_index = max(p->match_index + 1, p->snapshot_index);
        p->snapshot_index = 0;
    } else {
        p->next_index = p->match_index + 1;
    }
    p->state = PROGRESS__PROBE;
}

void progress__to_snapshot(struct raft *r,
                           const struct raft_server *server,
                           raft_index last_index)
{
    struct raft_progress *p = get(r, server);
    p->state = PROGRESS__SNAPSHOT;
    p->snapshot_index = last_index;
}

void progress__abort_snapshot(struct raft *r, const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    p->snapshot_index = 0;
    p->state = PROGRESS__PROBE;
}

bool progress__snapshot_done(struct raft *r, const struct raft_server *server)
{
    struct raft_progress *p = get(r, server);
    assert(p->state == PROGRESS__SNAPSHOT);
    return p->match_index >= p->snapshot_index;
}
