#include <assert.h>

#include "../include/raft.h"

#include "configuration.h"
#include "error.h"
#include "election.h"
#include "logger.h"
#include "replication.h"
#include "state.h"

/**
 * Apply time-dependent rules for followers (Figure 3.1).
 */
static int raft_tick__follower(struct raft *r)
{
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_FOLLOWER);

    server = raft_configuration__get(&r->configuration, r->id);

    /* If there's only one voting server, and that is us, it's safe to convert
     * to leader. If that is not us, we're either joining the cluster or we're
     * simply configured as non-voter, do nothing and wait for RPCs. */
    if (raft_configuration__n_voting(&r->configuration) == 1) {
        if (server->voting) {
            raft__debugf(r, "tick: self elect and convert to leader");
            rv = raft_state__convert_to_candidate(r);
            if (rv != 0) {
                raft_error__wrapf(r, "convert to candidate");
                return rv;
            }
            rv = raft_state__convert_to_leader(r);
            if (rv != 0) {
                raft_error__wrapf(r, "convert to leader");
                return rv;
            }
            return 0;
        }

        return 0;
    }

    /* Check if we need to start an election.
     *
     * From Section §3.3:
     *
     *   If a follower receives no communication over a period of time called
     *   the election timeout, then it assumes there is no viable leader and
     *   begins an election to choose a new leader.
     *
     * Figure 3.1:
     *
     *   If election timeout elapses without receiving AppendEntries RPC from
     *   current leader or granting vote to candidate, convert to candidate.
     */
    if (r->timer > r->election_timeout_rand && server->voting) {
        raft__infof(r, "tick: convert to candidate and start new election");
        return raft_state__convert_to_candidate(r);
    }

    return 0;
}

/**
 * Apply time-dependent rules for candidates (Figure 3.1).
 */
static int raft_tick__candidate(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_STATE_CANDIDATE);

    /* Check if we need to start an election.
     *
     * From Section §3.4:
     *
     *   The third possible outcome is that a candidate neither wins nor loses
     *   the election: if many followers become candidates at the same time,
     *   votes could be split so that no candidate obtains a majority. When this
     *   happens, each candidate will time out and start a new election by
     *   incrementing its term and initiating another round of RequestVote RPCs
     */
    if (r->timer > r->election_timeout_rand) {
        raft__infof(r, "tick: start new election");
        return raft_election__start(r);
    }

    return 0;
}

/**
 * Apply time-dependent rules for leaders (Figure 3.1).
 */
static int raft_tick__leader(struct raft *r)
{
    assert(r != NULL);
    assert(r->state == RAFT_STATE_LEADER);

    /* Check if we need to send heartbeats.
     *
     * From Figure 3.1:
     *
     *   Send empty AppendEntries RPC during idle periods to prevent election
     *   timeouts.
     */
    if (r->timer > r->heartbeat_timeout) {
        raft_replication__send_heartbeat(r);
        r->timer = 0;
    }

    return 0;
}

int raft_tick(struct raft *r, const unsigned msec_since_last_tick)
{
    int rv;

    assert(r != NULL);

    assert(r->state == RAFT_STATE_FOLLOWER ||
           r->state == RAFT_STATE_CANDIDATE || r->state == RAFT_STATE_LEADER);

    r->timer += msec_since_last_tick;

    switch (r->state) {
        case RAFT_STATE_FOLLOWER:
            rv = raft_tick__follower(r);
            break;
        case RAFT_STATE_CANDIDATE:
            rv = raft_tick__candidate(r);
            break;
        case RAFT_STATE_LEADER:
            rv = raft_tick__leader(r);
            break;
        default:
            rv = RAFT_ERR_INTERNAL;
            break;
    }

    return rv;
}
