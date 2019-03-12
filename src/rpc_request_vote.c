#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "replication.h"
#include "rpc.h"
#include "state.h"

static void raft_rpc__recv_request_vote_send_cb(struct raft_io_send *req,
                                                int status)
{
    (void)status;
    raft_free(req);
}

int raft_rpc__recv_request_vote(struct raft *r,
                                const unsigned id,
                                const char *address,
                                const struct raft_request_vote *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_request_vote_result *result = &message.request_vote_result;
    int match;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);

    result->vote_granted = false;

    raft_debugf(r->logger, "received vote request from server %ld", id);

    /* Reject the request if we have a leader.
     *
     * From Section §4.2.3:
     *
     *   [Removed] servers should not be able to disrupt a leader whose cluster
     *   is receiving heartbeats. [...] If a server receives a RequestVote
     *   request within the minimum election timeout of hearing from a current
     *   leader, it does not update its term or grant its vote
     */
    if (r->state == RAFT_FOLLOWER && r->follower_state.current_leader.id != 0) {
        raft_debugf(r->logger, "local server has a leader -> reject ");
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
        raft_debugf(r->logger, "local term is higher -> reject ");
        goto reply;
    }

    /* At this point our term must be the same or lower than the request term
     * (otherwise we would have reject the request */
    assert(r->current_term <= args->term);

    rv = raft_election__vote(r, args, &result->vote_granted);
    if (rv != 0) {
        return rv;
    }

reply:
    result->term = r->current_term;

    message.type = RAFT_IO_REQUEST_VOTE_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_ENOMEM;
    }

    rv = r->io->send(r->io, req, &message, raft_rpc__recv_request_vote_send_cb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

int raft_rpc__recv_request_vote_result(
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

    raft_debugf(r->logger, "received vote request result from server %ld", id);

    votes_index = configuration__index_of_voting(&r->configuration, id);
    if (votes_index == r->configuration.n) {
        raft_infof(r->logger, "non-voting or unknown server -> reject");
        return 0;
    }

    /* Ignore responses if we are not candidate anymore */
    if (r->state != RAFT_CANDIDATE) {
        raft_debugf(r->logger, "local server is not candidate -> ignore");
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
        raft_debugf(r->logger, "local term is higher -> ignore");
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
            raft_infof(r->logger, "votes quorum reached -> convert to leader");
            rv = raft_state__convert_to_leader(r);
            if (rv != 0) {
                return rv;
            }
            /* Send heartbeat messages.
             *
             * Note that since we have just set the next_index to the latest
             * index in our log, the AppendEntries RPC that we send here will
             * carry 0 entries, and indeed act as initial heartbeat. */
            raft_replication__trigger(r, 0);
        }
    }

    return 0;
}
