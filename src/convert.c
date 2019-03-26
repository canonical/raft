#include "convert.h"
#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "election.h"
#include "queue.h"
#include "state.h"

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void set_state(struct raft *r, int state)
{
    /* Check that the transition is legal, see Figure 3.3. Note that with
     * respect to the paper we have an additional "unavailable" state, which is
     * the initial or final state. */
    assert((r->state == RAFT_UNAVAILABLE && state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && state == RAFT_CANDIDATE) ||
           (r->state == RAFT_CANDIDATE && state == RAFT_FOLLOWER) ||
           (r->state == RAFT_CANDIDATE && state == RAFT_LEADER) ||
           (r->state == RAFT_LEADER && state == RAFT_FOLLOWER) ||
           (r->state == RAFT_FOLLOWER && state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_CANDIDATE && state == RAFT_UNAVAILABLE) ||
           (r->state == RAFT_LEADER && state == RAFT_UNAVAILABLE));
    r->state = state;
}

/* Clear follower state. */
static void clear_follower(struct raft *r)
{
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

/* Clear candidate state. */
static void clear_candidate(struct raft *r)
{
    if (r->candidate_state.votes != NULL) {
        raft_free(r->candidate_state.votes);
        r->candidate_state.votes = NULL;
    }
}

/*Clear leader state. */
static void clear_leader(struct raft *r)
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
     * the server to be promoted to catch up with logs, then it means that the
     * server was up to date at some point and we submitted the configuration
     * change to turn it into a voting server. Even if we lost leadership, it
     * could be that the entry still gets committed, so we don't abort the
     * promotion just yet.
     *
     * TODO: create a request object for promotion requests.
     */

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

/* Clear the current state */
static void clear(struct raft *r)
{
    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
    switch (r->state) {
        case RAFT_FOLLOWER:
            clear_follower(r);
            break;
        case RAFT_CANDIDATE:
            clear_candidate(r);
            break;
        case RAFT_LEADER:
            clear_leader(r);
            break;
    }
}

void convert__to_follower(struct raft *r)
{
    clear(r);
    set_state(r, RAFT_FOLLOWER);

    /* Reset election timer. */
    election__reset_timer(r);

    /* The current leader will be set next time that we receive an AppendEntries
     * RPC. */
    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

int convert__to_candidate(struct raft *r)
{
    size_t n_voting = configuration__n_voting(&r->configuration);
    int rv;

    clear(r);
    set_state(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voting * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        return RAFT_ENOMEM;
    }

    /* Start a new election round */
    rv = election__start(r);
    if (rv != 0) {
        r->state = RAFT_FOLLOWER;
        raft_free(r->candidate_state.votes);
        return rv;
    }
    return 0;
}

/* Allocate the replication state for n_servers. */
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

int convert__to_leader(struct raft *r)
{
    size_t i;
    raft_index last_index;
    int rv;

    clear(r);
    set_state(r, RAFT_LEADER);

    /* Reset apply requests queue */
    RAFT__QUEUE_INIT(&r->leader_state.apply_reqs);

    /* Allocate the next_index and match_index arrays. */
    rv = alloc_replication(r->configuration.n, &r->leader_state.replication);
    if (rv != 0) {
        return rv;
    }

    last_index = log__last_index(&r->log);

    /* Initialize the replication state for each server. We optimistically
     * assume that servers are up-to-date and back track if turns out not to be
     * so (TODO: include reference to raft paper). */
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_replication *replication = &r->leader_state.replication[i];
        replication->next_index = last_index + 1;
        replication->match_index = 0;
        /* TODO: we should keep a last_contact array which is independent from
         * the replication array, and keep it up-to-date.  */
        replication->last_contact = 0;
        replication->state = REPLICATION__PROBE;
    }

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_duration = 0;

    return 0;
}

void convert__to_unavailable(struct raft *r) {
    clear(r);
    set_state(r, RAFT_UNAVAILABLE);
}
