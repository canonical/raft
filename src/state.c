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
