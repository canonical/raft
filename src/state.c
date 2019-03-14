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

/**
 * Clear follower state.
 */
static void raft_state__clear_follower(struct raft *r)
{
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

/**
 * Clear candidate state.
 */
static void raft_state__clear_candidate(struct raft *r)
{
    if (r->candidate_state.votes != NULL) {
        raft_free(r->candidate_state.votes);
        r->candidate_state.votes = NULL;
    }
}

/**
 * Clear leader state.
 */
static void raft_state__clear_leader(struct raft *r)
{
    if (r->leader_state.replication != NULL) {
        raft_free(r->leader_state.replication);
        r->leader_state.replication = NULL;
    }

    /* If a promotion request is in progress and we are waiting for the server
     * to be promoted to catch up with logs, then we need to abort the
     * promotion, because having lost leadership we're not in the position to
     * submit any raft entry.
     *
     * Note that if a promotion request is in progress but we're not waiting for
     * the server to be promoted to catch up with logs, tthen it means that the
     * server was up to date at some point and we submitted the configuration
     * change to turn it into a voting server. Even if we lost leadership, it
     * could be that the entry still gets committed, so we don't abort the
     * promotion just yet. */
    if (r->leader_state.promotee_id > 0) {
        raft_watch__promotion_aborted(r, r->leader_state.promotee_id);
    }

    /* Fail all outstanding apply requests */
    while (!RAFT__QUEUE_IS_EMPTY(&r->leader_state.apply_reqs)) {
        struct raft_apply *req;
        raft__queue *head;
        head = RAFT__QUEUE_HEAD(&r->leader_state.apply_reqs);
        RAFT__QUEUE_REMOVE(head);
        req = RAFT__QUEUE_DATA(head, struct raft_apply, queue);
        if (req->cb != NULL) {
            req->cb(req, RAFT_ERR_LEADERSHIP_LOST);
        }
    }
}

void raft_state__clear(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

    switch (r->state) {
        case RAFT_FOLLOWER:
            raft_state__clear_follower(r);
            break;
        case RAFT_CANDIDATE:
            raft_state__clear_candidate(r);
            break;
        case RAFT_LEADER:
            raft_state__clear_leader(r);
            break;
    }
}

/**
 * Convenience for changing state and asserting that the transition is valid.
 */
static void raft_state__change(struct raft *r, int state)
{
    assert(r != NULL);
    assert(state == RAFT_FOLLOWER || state == RAFT_CANDIDATE ||
           state == RAFT_LEADER);

    /* Check that the transition is legal, see Figure 3.3. */
    assert((r->state == RAFT_FOLLOWER && state == RAFT_CANDIDATE) ||
           (r->state == RAFT_CANDIDATE && state == RAFT_FOLLOWER) ||
           (r->state == RAFT_CANDIDATE && state == RAFT_LEADER) ||
           (r->state == RAFT_LEADER && state == RAFT_FOLLOWER));

    r->state = state;
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
 * Reset follower state.
 */
static void raft_state__reset_follower(struct raft *r)
{
    /* Reset election timer. */
    raft_election__reset_timer(r);

    /* The current leader will be set next time that we receive an AppendEntries
     * RPC. */
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

void raft_state__start_as_follower(struct raft *r)
{
    assert(r->state == RAFT_UNAVAILABLE);

    r->state = RAFT_FOLLOWER;

    raft_state__reset_follower(r);
}

int raft_state__convert_to_follower(struct raft *r, raft_term term)
{
    int rv;
    unsigned short prev_state = r->state;

    assert(r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

    switch (r->state) {
        case RAFT_CANDIDATE:
            raft_state__clear_candidate(r);
            break;
        case RAFT_LEADER:
            raft_state__clear_leader(r);
            break;
    }

    raft_state__change(r, RAFT_FOLLOWER);

    /* We only need to write the new term if it differs form the current
     * one. The only case were this is not the case is if the leader decides to
     * step down after it has been removed from the cluster configuration. */
    if (term > r->current_term) {
        rv = raft_state__bump_current_term(r, term);
        if (rv != 0) {
            return rv;
        }
    }

    raft_state__reset_follower(r);

    /* Notify watchers */
    raft_watch__state_change(r, prev_state);

    return 0;
}

int raft_state__convert_to_candidate(struct raft *r)
{
    size_t n_voting = configuration__n_voting(&r->configuration);
    int rv;

    assert(r->state == RAFT_FOLLOWER);

    raft_state__clear_follower(r);

    /* Change state */
    raft_state__change(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voting * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }

    /* Start a new election round */
    rv = raft_election__start(r);
    if (rv != 0) {
        r->state = RAFT_FOLLOWER;
        raft_free(r->candidate_state.votes);
        return rv;
    }

    /* Notify watchers */
    raft_watch__state_change(r, RAFT_FOLLOWER);

    return 0;

err:
    assert(rv != 0);
    return rv;
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

int raft_state__convert_to_leader(struct raft *r)
{
    size_t i;
    int rv;

    assert(r != NULL);

    assert(r->state == RAFT_CANDIDATE);

    raft_state__clear_candidate(r);

    raft_state__change(r, RAFT_LEADER);

    /* Reset apply requests queue */
    RAFT__QUEUE_INIT(&r->leader_state.apply_reqs);

    /* Allocate the next_index and match_index arrays. */
    rv = alloc_replication(r->configuration.n, &r->leader_state.replication);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the replication state for each server. We optimistically
     * assume that servers are up-to-date and back track if turns out not to be
     * so (TODO: include reference to raft paper). */
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_replication *replication = &r->leader_state.replication[i];
        replication->next_index = log__last_index(&r->log) + 1;
        replication->match_index = 0;
        /* TODO: we should keep a last_contact array which is independent from
         * the replication array, and keep it up-to-date.  */
        replication->last_contact = 0;
        replication->state = REPLICATION__PROBE;
    }

    /* Notify watchers */
    raft_watch__state_change(r, RAFT_CANDIDATE);

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_duration = 0;

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
