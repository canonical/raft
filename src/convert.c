#include "convert.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "progress.h"
#include "queue.h"
#include "request.h"

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, int new_state)
{
    int old_state = r->state;

    /* Check that the transition is legal, see Figure 3.3. Note that with
     * respect to the paper we have an additional "unavailable" state, which is
     * the initial or final state. */
    assert((old_state == RAFT_UNAVAILABLE && new_state == RAFT_FOLLOWER) ||
           (old_state == RAFT_FOLLOWER && new_state == RAFT_CANDIDATE) ||
           (old_state == RAFT_CANDIDATE && new_state == RAFT_FOLLOWER) ||
           (old_state == RAFT_CANDIDATE && new_state == RAFT_LEADER) ||
           (old_state == RAFT_LEADER && new_state == RAFT_FOLLOWER) ||
           (old_state == RAFT_FOLLOWER && new_state == RAFT_UNAVAILABLE) ||
           (old_state == RAFT_CANDIDATE && new_state == RAFT_UNAVAILABLE) ||
           (old_state == RAFT_LEADER && new_state == RAFT_UNAVAILABLE));
    r->state = new_state;
}

/* Clear follower state. */
static void convertClearFollower(struct raft *r)
{
    r->follower_state.current_leader.id = 0;
    if (r->follower_state.current_leader.address != NULL) {
        raft_free(r->follower_state.current_leader.address);
    }
    r->follower_state.current_leader.address = NULL;
}

/* Clear candidate state. */
static void convertClearCandidate(struct raft *r)
{
    if (r->candidate_state.votes != NULL) {
        raft_free(r->candidate_state.votes);
        r->candidate_state.votes = NULL;
    }
}

static void convertFailApply(struct raft_apply *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST, NULL);
    }
}

static void convertFailBarrier(struct raft_barrier *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST);
    }
}

static void convertFailChange(struct raft_change *req)
{
    if (req != NULL && req->cb != NULL) {
        req->cb(req, RAFT_LEADERSHIPLOST);
    }
}

/* Clear leader state. */
static void convertClearLeader(struct raft *r)
{
    if (r->leader_state.progress != NULL) {
        raft_free(r->leader_state.progress);
        r->leader_state.progress = NULL;
    }

    /* Fail all outstanding requests */
    while (!QUEUE_IS_EMPTY(&r->leader_state.requests)) {
        struct request *req;
        queue *head;
        head = QUEUE_HEAD(&r->leader_state.requests);
        QUEUE_REMOVE(head);
        req = QUEUE_DATA(head, struct request, queue);
        switch (req->type) {
            case RAFT_COMMAND:
                convertFailApply((struct raft_apply *)req);
                break;
            case RAFT_BARRIER:
                convertFailBarrier((struct raft_barrier *)req);
                break;
            case RAFT_CHANGE:
                convertFailChange((struct raft_change *)req);
                assert(r->leader_state.change == (struct raft_change *)req);
                r->leader_state.change = NULL;
                break;
        };
    }

    /* Fail any promote request that is still outstanding because the server is
     * still catching up and no entry was submitted. */
    if (r->leader_state.change != NULL) {
        convertFailChange(r->leader_state.change);
        r->leader_state.change = NULL;
    }
}

/* Clear the current state */
static void convertClear(struct raft *r)
{
    assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
           r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
    switch (r->state) {
        case RAFT_FOLLOWER:
            convertClearFollower(r);
            break;
        case RAFT_CANDIDATE:
            convertClearCandidate(r);
            break;
        case RAFT_LEADER:
            convertClearLeader(r);
            break;
    }
}

void convertToFollower(struct raft *r)
{
    convertClear(r);
    convertSetState(r, RAFT_FOLLOWER);

    /* Reset election timer. */
    electionResetTimer(r);

    r->follower_state.current_leader.id = 0;
    r->follower_state.current_leader.address = NULL;
}

int convertToCandidate(struct raft *r)
{
    size_t n_voters = configurationVoterCount(&r->configuration);
    int rv;

    convertClear(r);
    convertSetState(r, RAFT_CANDIDATE);

    /* Allocate the votes array. */
    r->candidate_state.votes = raft_malloc(n_voters * sizeof(bool));
    if (r->candidate_state.votes == NULL) {
        return RAFT_NOMEM;
    }

    /* Start a new election round */
    rv = electionStart(r);
    if (rv != 0) {
        r->state = RAFT_FOLLOWER;
        raft_free(r->candidate_state.votes);
        return rv;
    }

    return 0;
}

int convertToLeader(struct raft *r)
{
    int rv;

    convertClear(r);
    convertSetState(r, RAFT_LEADER);

    /* Reset timers */
    r->election_timer_start = r->io->time(r->io);

    /* Reset apply requests queue */
    QUEUE_INIT(&r->leader_state.requests);

    /* Allocate and initialize the progress array. */
    rv = progressBuildArray(r);
    if (rv != 0) {
        return rv;
    }

    r->leader_state.change = NULL;

    /* Reset promotion state. */
    r->leader_state.promotee_id = 0;
    r->leader_state.round_number = 0;
    r->leader_state.round_index = 0;
    r->leader_state.round_start = 0;

    return 0;
}

void convertToUnavailable(struct raft *r)
{
    convertClear(r);
    convertSetState(r, RAFT_UNAVAILABLE);
}
