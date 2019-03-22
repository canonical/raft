#include "state.h"
#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "queue.h"
#include "watch.h"

const char *raft_state_names[] = {"unavailable", "follower", "candidate",
                                  "leader"};

int raft_state(struct raft *r)
{
    return r->state;
}

void raft_leader(struct raft *r, unsigned *id, const char **address)
{
    switch (r->state) {
        case RAFT_UNAVAILABLE:
        case RAFT_CANDIDATE:
            *id = 0;
            *address = NULL;
            return;
        case RAFT_FOLLOWER:
            *id = r->follower_state.current_leader.id;
            *address = r->follower_state.current_leader.address;
            return;
        case RAFT_LEADER:
            *id = r->id;
            *address = r->address;
            break;
    }
}

raft_index raft_last_applied(struct raft *r)
{
    return r->last_applied;
}

int raft_state__bump_current_term(struct raft *r, raft_term term)
{
    int rv;

    assert(r != NULL);
    assert(term >= r->current_term);

    /* Save the new term to persistent store, resetting the vote. */
    rv = r->io->set_term(r->io, term);
    if (rv != 0) {
        return rv;
    }

    /* Update our cache too. */
    r->current_term = term;
    r->voted_for = 0;

    return 0;
}

/**
 * Allocate the replication state for n_servers.
 */
static int alloc_replication(size_t n_servers,
                             struct raft_replication **replication)
{
    int rv;

    assert(n_servers > 0);
    assert(replication != NULL);

    *replication = raft_calloc(n_servers, sizeof **replication);
    if (*replication == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    return 0;

err:
    assert(rv != 0);
    return rv;
}

int raft_state__rebuild_next_and_match_indexes(
    struct raft *r,
    const struct raft_configuration *configuration)
{
    struct raft_replication *replication; /* New replication array */
    size_t i;
    int rv;

    assert(r != NULL);
    assert(configuration != NULL);
    assert(r->state == RAFT_LEADER);

    /* Allocate the new replication states array. */
    rv = alloc_replication(configuration->n, &replication);
    if (rv != 0) {
        goto err;
    }

    /* First copy the current replication state for the servers that exists both
     * in the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        unsigned id = r->configuration.servers[i].id;
        size_t j = configuration__index_of(configuration, id);

        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }

        replication[j] = r->leader_state.replication[i];
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

        replication[i].next_index = log__last_index(&r->log) + 1;
        replication[i].match_index = 0;
        replication[i].last_contact = r->io->time(r->io);
    }

    raft_free(r->leader_state.replication);

    r->leader_state.replication = replication;

    return 0;

err:
    assert(rv != 0);
    return rv;
}
