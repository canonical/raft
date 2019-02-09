#include "state.h"
#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "watch.h"

const char *raft_state_names[] = {"unavailable", "follower", "candidate",
                                  "leader"};

/**
 * Clear follower state.
 */
static void raft_state__clear_follower(struct raft *r)
{
    r->follower_state.current_leader_id = 0;
    if (r->follower_state.pending != NULL) {
        raft_free(r->follower_state.pending);
        r->follower_state.pending = NULL;
    }
    r->follower_state.n_pending = 0;
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
    if (r->leader_state.next_index != NULL) {
        raft_free(r->leader_state.next_index);
        r->leader_state.next_index = NULL;
    }

    if (r->leader_state.match_index != NULL) {
        raft_free(r->leader_state.match_index);
        r->leader_state.match_index = NULL;
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
}

void raft_state__clear(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_STATE_UNAVAILABLE ||
           r->state == RAFT_STATE_FOLLOWER ||
           r->state == RAFT_STATE_CANDIDATE || r->state == RAFT_STATE_LEADER);

    switch (r->state) {
        case RAFT_STATE_FOLLOWER:
            raft_state__clear_follower(r);
            break;
        case RAFT_STATE_CANDIDATE:
            raft_state__clear_candidate(r);
            break;
        case RAFT_STATE_LEADER:
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
    assert(state == RAFT_STATE_FOLLOWER || state == RAFT_STATE_CANDIDATE ||
           state == RAFT_STATE_LEADER);

    /* Check that the transition is legal, see Figure 3.3. */
    assert((r->state == RAFT_STATE_FOLLOWER && state == RAFT_STATE_CANDIDATE) ||
           (r->state == RAFT_STATE_CANDIDATE && state == RAFT_STATE_FOLLOWER) ||
           (r->state == RAFT_STATE_CANDIDATE && state == RAFT_STATE_LEADER) ||
           (r->state == RAFT_STATE_LEADER && state == RAFT_STATE_FOLLOWER));

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
    r->follower_state.current_leader_id = 0;

    r->follower_state.pending = NULL;
    r->follower_state.n_pending = 0;
}

void raft_state__start_as_follower(struct raft *r)
{
    assert(r->state == RAFT_STATE_UNAVAILABLE);

    r->state = RAFT_STATE_FOLLOWER;

    raft_state__reset_follower(r);
}

int raft_state__convert_to_follower(struct raft *r, raft_term term)
{
    int rv;
    unsigned short prev_state = r->state;

    assert(r->state == RAFT_STATE_CANDIDATE || r->state == RAFT_STATE_LEADER);

    switch (r->state) {
        case RAFT_STATE_CANDIDATE:
            raft_state__clear_candidate(r);
            break;
        case RAFT_STATE_LEADER:
            raft_state__clear_leader(r);
            break;
    }

    raft_state__change(r, RAFT_STATE_FOLLOWER);

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
    size_t n_voting = raft_configuration__n_voting(&r->configuration);
    int rv;

    assert(r->state == RAFT_STATE_FOLLOWER);

    raft_state__clear_follower(r);

    /* Change state */
    raft_state__change(r, RAFT_STATE_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voting * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    /* Start a new election round */
    rv = raft_election__start(r);
    if (rv != 0) {
        r->state = RAFT_STATE_FOLLOWER;
        raft_free(r->candidate_state.votes);
        return rv;
    }

    /* Notify watchers */
    raft_watch__state_change(r, RAFT_STATE_FOLLOWER);

    return 0;

err:
    assert(rv != 0);
    return rv;
}

/**
 * Allocate the given next/match indexes.
 */
static int raft_state__alloc_next_and_match_indexes(struct raft *r,
                                                    size_t n_servers,
                                                    raft_index **next_index,
                                                    raft_index **match_index)
{
    int rv;

    assert(n_servers > 0);
    assert(next_index != NULL);
    assert(match_index != NULL);

    *next_index = raft_calloc(n_servers, sizeof **next_index);
    if (*next_index == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    *match_index = raft_calloc(n_servers, sizeof **match_index);
    if (*match_index == NULL) {
        rv = RAFT_ERR_NOMEM;
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

    assert(r->state == RAFT_STATE_CANDIDATE);

    raft_state__clear_candidate(r);

    raft_state__change(r, RAFT_STATE_LEADER);

    /* Allocate the next_index and match_index arrays. */
    rv = raft_state__alloc_next_and_match_indexes(r, r->configuration.n,
                                                  &r->leader_state.next_index,
                                                  &r->leader_state.match_index);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the next_index and match_index arrays. */
    for (i = 0; i < r->configuration.n; i++) {
        r->leader_state.next_index[i] = raft_log__last_index(&r->log) + 1;
        r->leader_state.match_index[i] = 0;
    }

    /* Notify watchers */
    raft_watch__state_change(r, RAFT_STATE_CANDIDATE);

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
    raft_index *next_index;  /* New next index */
    raft_index *match_index; /* New match index */
    size_t i;
    int rv;

    assert(r != NULL);
    assert(configuration != NULL);
    assert(r->state == RAFT_STATE_LEADER);

    /* Allocate the new next_index and match_index arrays. */
    rv = raft_state__alloc_next_and_match_indexes(r, configuration->n,
                                                  &next_index, &match_index);
    if (rv != 0) {
        goto err;
    }

    /* First copy the current next/match index value for the servers that exists
     * both in the current and in the new configuration. */
    for (i = 0; i < r->configuration.n; i++) {
        unsigned id = r->configuration.servers[i].id;
        size_t j = raft_configuration__index(configuration, id);

        if (j == configuration->n) {
            /* This server is not present in the new configuration, so we just
             * skip it. */
            continue;
        }

        next_index[j] = r->leader_state.next_index[i];
        match_index[j] = r->leader_state.match_index[j];
    }

    /* The reset the next/match index value for servers that are present in the
     * new configuration, but not in the current one. */
    for (i = 0; i < configuration->n; i++) {
        unsigned id = configuration->servers[i].id;
        size_t j = raft_configuration__index(&r->configuration, id);

        if (j < r->configuration.n) {
            /* This server is present both in the new and in the current
             * configuration, so we have already copied its next/match index
             * value in the loop above. */
            continue;
        }

        assert(j == r->configuration.n);

        next_index[i] = raft_log__last_index(&r->log) + 1;
        match_index[i] = 0;
    }

    raft_free(r->leader_state.next_index);
    raft_free(r->leader_state.match_index);

    r->leader_state.next_index = next_index;
    r->leader_state.match_index = match_index;

    return 0;

err:
    assert(rv != 0);
    return rv;
}
