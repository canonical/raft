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
