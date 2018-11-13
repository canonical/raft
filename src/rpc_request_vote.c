#include <assert.h>

#include "../include/raft.h"

#include "configuration.h"
#include "election.h"
#include "logger.h"
#include "rpc.h"
#include "state.h"

int raft_handle_request_vote(struct raft *r,
                             const unsigned id,
                             const char *address,
                             const struct raft_request_vote_args *args)
{
    struct raft_request_vote_result result;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    result.vote_granted = false;

    raft__debugf(r, "received vote request from server %ld", id);

    /* Reject the request if we have a leader.
     *
     * From Section §4.2.3:
     *
     *   [Removed] servers should not be able to disrupt a leader whose cluster
     *   is receiving heartbeats. [...] If a server receives a RequestVote
     *   request within the minimum election timeout of hearing from a current
     *   leader, it does not update its term or grant its vote
     */
    if (r->state == RAFT_STATE_FOLLOWER &&
        r->follower_state.current_leader_id != 0) {
        raft__debugf(r, "local server has a leader -> reject ");
        goto reply;
    }

    rv = raft_rpc__ensure_matching_terms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }

    /* From Figure 3.1:
     *
     *   RequestVote RPC: Receiver implementation: Reply false if
     *   term < currentTerm.
     *
     */
    if (match < 0) {
        raft__debugf(r, "local term is higher -> reject ");
        goto reply;
    }

    /* At this point our term must be the same or lower than the request term
     * (otherwise we would have reject the request */
    assert(r->current_term <= args->term);

    rv = raft_election__vote(r, args, &result.vote_granted);
    if (rv != 0) {
        return rv;
    }

reply:
    result.term = r->current_term;

    rv = r->io->send_request_vote_response(r->io, id, address, &result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_handle_request_vote_response(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_request_vote_result *result)
{
    size_t votes_index;
    int match;
    int rv;

    (void)address;

    assert(r != NULL);
    assert(id > 0);
    assert(result != NULL);

    raft__debugf(r, "received vote request result from server %ld", id);

    votes_index =
        raft_configuration__voting_index(&r->configuration, id);
    if (votes_index == r->configuration.n) {
        raft__infof(r, "non-voting or unknown server -> reject");
        return 0;
    }

    /* Ignore responses if we are not candidate anymore */
    if (r->state != RAFT_STATE_CANDIDATE) {
        raft__debugf(r, "local server is not candidate -> ignore");
        return 0;
    }

    rv = raft_rpc__ensure_matching_terms(r, result->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        /* If the term in the result is older than ours, this is an old message
         * we should ignore, because the node who voted for us would have
         * obtained our term.  This happens if the network is pretty choppy. */
        raft__debugf(r, "local term is higher -> ignore");
        return 0;
    }

    assert(result->term == r->current_term);

    /* If the vote was granted and we reached quorum, convert to leader.
     *
     * From Figure 3.1:
     *
     *   If votes received from majority of severs: become leader.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: receives votes from majority of servers -> [leader]
     *
     * From Section §3.4:
     *
     *   A candidate wins an election if it receives votes from a majority of
     *   the servers in the full cluster for the same term. Each server will
     *   vote for at most one candidate in a given term, on a
     *   firstcome-first-served basis [...]. Once a candidate wins an election,
     *   it becomes leader.
     */
    if (result->vote_granted) {
        if (raft_election__tally(r, votes_index)) {
            raft__debugf(r, "votes quorum reached -> convert to leader");
            rv = raft_state__convert_to_leader(r);
            if (rv != 0) {
                return rv;
            }
        }
    }

    return 0;
}
