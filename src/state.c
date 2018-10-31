#include <assert.h>

#include "configuration.h"
#include "election.h"
#include "error.h"
#include "log.h"
#include "logger.h"
#include "replication.h"
#include "state.h"

const char *raft_state_names[] = {
    "follower",
    "candidate",
    "leader",
};

/**
 * Clear follower state.
 */
static void raft_state__clear_follower(struct raft *r)
{
    r->follower_state.current_leader = NULL;
}

/**
 * Clear candidate state.
 */
static void raft_state__clear_candidate(struct raft *r)
{
    raft_free(r->candidate_state.votes);
    r->candidate_state.votes = NULL;
}

/**
 * Clear leader state.
 */
static void raft_state__clear_leader(struct raft *r)
{
    raft_free(r->leader_state.next_index);
    raft_free(r->leader_state.match_index);

    r->leader_state.next_index = NULL;
    r->leader_state.match_index = NULL;
}

void raft_state__clear(struct raft *r)
{
    switch (r->state) {
        case RAFT_STATE_FOLLOWER:
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
 * Notify the watcher for the given event ID, if one is registered.
 */
static void raft_state__notify_watcher(struct raft *r, int event_id)
{
    void (*watcher)(void *, int);

    assert(r != NULL);
    assert(event_id == RAFT_EVENT_STATE_CHANGE);

    watcher = r->watchers[RAFT_EVENT_STATE_CHANGE];

    if (watcher != NULL) {
        watcher(r->data, event_id);
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

/**
 * Update the current term to the given value and reset our vote.
 */
int raft_state__update_current_term(struct raft *r, raft_term term)
{
    int rv;

    assert(r != NULL);
    assert(term >= r->current_term);

    /* Save the new term to persistent store, resetting the vote. */
    rv = r->io->write_term(r->io, term);
    if (rv != 0) {
        raft__errorf(r, "failed to write term: %s (%d)", raft_strerror(rv), rv);
        return rv;
    }

    /* Update our cache too. */
    r->current_term = term;
    r->voted_for = 0;

    return 0;
}

int raft_state__convert_to_follower(struct raft *r, raft_term term)
{
    int rv;

    assert(r->state == RAFT_STATE_CANDIDATE || r->state == RAFT_STATE_LEADER);
    switch (r->state) {
        case RAFT_STATE_CANDIDATE:
            raft_state__clear_candidate(r);
            break;
        case RAFT_STATE_LEADER:
            raft_state__clear_leader(r);
            break;
    }

    rv = raft_state__update_current_term(r, term);
    if (rv != 0) {
        return rv;
    }

    /* Reset election timer. */
    raft_election__reset_timer(r);

    raft_state__change(r, RAFT_STATE_FOLLOWER);

    /* The current leader will be set next time that we receive an AppendEntries
     * RPC. */
    r->follower_state.current_leader = NULL;

    return 0;
}

int raft_state__convert_to_candidate(struct raft *r)
{
    size_t n_voting = raft_configuration__n_voting(&r->configuration);
    int rv;

    assert(r->state == RAFT_STATE_FOLLOWER);
    raft_state__clear_follower(r);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voting * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        rv = RAFT_ERR_NOMEM;
        raft_error__printf(r, rv, "alloc votes array");
        goto err;
    }

    /* Change state */
    raft_state__change(r, RAFT_STATE_CANDIDATE);

    /* Start a new election round */
    rv = raft_election__start(r);
    if (rv != 0) {
        r->state = RAFT_STATE_FOLLOWER;
        raft_free(r->candidate_state.votes);
        return rv;
    }

    /* Notify watchers */
    raft_state__notify_watcher(r, RAFT_EVENT_STATE_CHANGE);

    return 0;

 err:
    assert(rv != 0);
    return rv;
}

int raft_state__convert_to_leader(struct raft *r)
{
    size_t i;
    size_t n_servers;
    int rv;

    assert(r != NULL);

    assert(r->state == RAFT_STATE_CANDIDATE);
    raft_state__clear_candidate(r);

    /* Allocate the next_index and match_index arrays. */
    n_servers = r->configuration.n;
    r->leader_state.next_index = raft_malloc(n_servers * sizeof(uint64_t));
    if (r->leader_state.next_index == NULL) {
        rv = RAFT_ERR_NOMEM;
        raft_error__printf(r, rv, "alloc next_index array");
        goto err;
    }
    r->leader_state.match_index = raft_malloc(n_servers * sizeof(uint64_t));
    if (r->leader_state.match_index == NULL) {
        rv = RAFT_ERR_NOMEM;
        raft_error__printf(r, rv, "alloc match_index array");
        goto err;
    }

    /* Initialize the next_index and match_index arrays.
     */
    for (i = 0; i < r->configuration.n; i++) {
        r->leader_state.next_index[i] = raft_log__last_index(&r->log) + 1;
        r->leader_state.match_index[i] = 0;
    }

    raft_state__change(r, RAFT_STATE_LEADER);

    /* Send heartbeat messages.
     *
     * Note that since we have just set the next_index to the latest index in
     * our log, the AppendEntries RPC that we send here will carry 0 entries,
     * and indeed act as initial heartbeat. */
    raft_replication__send_heartbeat(r);

    return 0;

err:
    assert(rv != 0);
    return rv;
}
