#include <assert.h>

#include "../include/raft.h"

#include "binary.h"
#include "configuration.h"
#include "error.h"
#include "election.h"
#include "log.h"
#include "logger.h"
#include "replication.h"
#include "state.h"

/**
 * Common logic for RPC handlers, comparing the request's term with the server's
 * current term and possibly deciding to reject the request or step down from
 * candidate or leader.
 *
 * The @match output parameter will be set to 0 if the local term matches the
 * request's term, to -1 if the request's term is lower, and to 1 if the
 * request's term was higher but we have successfully bumped the local one to
 * match it (and stepped down to follower in that case, if we were not already).
 */
static int raft__rpc_ensure_matching_terms(struct raft *r,
                                           raft_term term,
                                           int *match)
{
    int rv;

    assert(r != NULL);
    assert(match != NULL);

    if (term < r->current_term) {
        *match = -1;
        return 0;
    }

    /* From Figure 3.1:
     *
     *   Rules for Servers: All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     *
     * From state diagram in Figure 3.3:
     *
     *   [leader]: discovers server with higher term -> [follower]
     *
     * From Section §3.3:
     *
     *   If a candidate or leader discovers that its term is out of date, it
     *   immediately reverts to follower state.
     */
    if (term > r->current_term) {
        if (r->state == RAFT_STATE_FOLLOWER) {
            /* Just bump the current term */
            raft__infof(r, "remote server term is higher -> bump local term");
            rv = raft_state__update_current_term(r, term);
        } else {
            /* Bump current state and also convert to follower. */
            raft__infof(r, "remote server term is higher -> step down");
            rv = raft_state__convert_to_follower(r, term);
        }
        if (rv != 0) {
            return rv;
        }
        *match = 1;
    } else {
        *match = 0;
    }

    return 0;
}

int raft_handle_request_vote(struct raft *r,
                             const struct raft_server *server,
                             const struct raft_request_vote_args *args)
{
    struct raft_request_vote_result result;
    int match;
    int rv;

    assert(r != NULL);
    assert(server != NULL);
    assert(args != NULL);

    result.vote_granted = false;

    raft__debugf(r, "received vote request from server %ld", server->id);

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
        r->follower_state.current_leader != NULL) {
        raft__debugf(r, "local server has a leader -> reject ");
        goto reply;
    }

    rv = raft__rpc_ensure_matching_terms(r, args->term, &match);
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

    rv = raft_election__maybe_grant_vote(r, args, &result.vote_granted);
    if (rv != 0) {
        return rv;
    }

reply:
    result.term = r->current_term;

    rv = r->io->send_request_vote_response(r->io, server, &result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_handle_request_vote_response(
    struct raft *r,
    const struct raft_server *server,
    const struct raft_request_vote_result *result)
{
    size_t votes_index;
    int match;
    int rv;

    assert(r != NULL);
    assert(server != NULL);
    assert(result != NULL);

    raft__debugf(r, "received vote request result from server %ld", server->id);

    votes_index =
        raft_configuration__voting_index(&r->configuration, server->id);
    if (votes_index == r->configuration.n) {
        raft__infof(r, "non-voting or unknown server -> reject");
        return 0;
    }

    /* Ignore responses if we are not candidate anymore */
    if (r->state != RAFT_STATE_CANDIDATE) {
        raft__debugf(r, "local server is not candidate -> ignore");
        return 0;
    }

    rv = raft__rpc_ensure_matching_terms(r, result->term, &match);
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
        if (raft_election__maybe_win(r, votes_index)) {
            raft__debugf(r, "votes quorum reached -> convert to leader");
            return raft_state__convert_to_leader(r);
        }
    }

    return 0;
}

int raft_handle_append_entries(struct raft *r,
                               const struct raft_server *server,
                               const struct raft_append_entries_args *args)
{
    struct raft_append_entries_result result;
    int match;
    int rv;

    assert(r != NULL);
    assert(server != NULL);
    assert(args != NULL);
    assert(server->id != 0);

    raft__debugf(r, "received %d entries from server %ld", args->n, server->id);

    result.success = false;
    result.last_log_index = raft_log__last_index(&r->log);

    rv = raft__rpc_ensure_matching_terms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
        raft__debugf(r, "local term is higher -> reject ");
        goto reply;
    }

    /* If we get here it means that the term in the request matches our current
     * term. If we're candidate, we want to step down to follower, because we
     * discovered the current leader.
     *
     * From Figure 3.1:
     *
     *   Rules for Servers: Candidates: if AppendEntries RPC is received from
     *   new leader: convert to follower.
     *
     * From Section §3.4:
     *
     *   While waiting for votes, a candidate may receive an AppendEntries RPC
     *   from another server claiming to be leader. If the leader’s term
     *   (included in its RPC) is at least as large as the candidate’s current
     *   term, then the candidate recognizes the leader as legitimate and
     *   returns to follower state. If the term in the RPC is smaller than the
     *   candidate’s current term, then the candidate rejects the RPC and
     *   continues in candidate state.
     *
     * From state diagram in Figure 3.3:
     *
     *   [candidate]: discovers current leader -> [follower]
     */
    assert(r->state == RAFT_STATE_FOLLOWER || r->state == RAFT_STATE_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_STATE_CANDIDATE) {
        raft__debugf(r, "discovered leader -> step down ");
        rv = raft_state__convert_to_follower(r, args->term);
        if (rv != 0) {
            return rv;
        }
    }

    assert(r->state == RAFT_STATE_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    r->follower_state.current_leader = server;

    /* Reset the election timer. */
    r->timer = 0;

    bool async;
    rv = raft_replication__maybe_append(r, args, &result.success, &async);
    if (rv != 0) {
        return rv;
    }

    if (async) {
        return 0;
    }

    if (result.success) {
        /* Echo back to the leader the point that we reached. */
        result.last_log_index = args->prev_log_index + args->n;
    }

reply:
    result.term = r->current_term;

    /* Free the entries batch, if any. */
    if (args->n > 0 && args->entries[0].batch != NULL) {
        raft_free(args->entries[0].batch);
    }

    if (args->entries != NULL) {
        raft_free(args->entries);
    }

    rv = r->io->send_append_entries_response(r->io, server, &result);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_handle_append_entries_response(
    struct raft *r,
    const struct raft_server *server,
    const struct raft_append_entries_result *result)
{
    int match;
    size_t server_index;
    int rv;

    assert(r != NULL);
    assert(server != NULL);
    assert(result != NULL);

    raft__debugf(r, "received append entries result from server %ld",
                 server->id);

    if (result->success != 1) {
      /* TODO: handle failures? */
      //assert(0);
    }

    if (r->state != RAFT_STATE_LEADER) {
        raft__debugf(r, "local server is not leader -> ignore");
        return 0;
    }

    rv = raft__rpc_ensure_matching_terms(r, result->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        raft__debugf(r, "local term is higher -> ignore ");
        return 0;
    }

    /* If we have stepped down, abort here.
     *
     * From Figure 3.1:
     *
     *   [Rules for Servers] All Servers: If RPC request or response contains
     *   term T > currentTerm: set currentTerm = T, convert to follower.
     */
    if (match > 0) {
        assert(r->state == RAFT_STATE_FOLLOWER);
        return 0;
    }

    assert(result->term == r->current_term);

    /* Ignore responses from servers that have been removed */
    server_index = raft_configuration__index(&r->configuration, server->id);
    if (server_index == r->configuration.n) {
        raft__errorf(r, "unknown server -> ignore");
        return 0;
    }

    /* Update the match/next indexes and possibly send further entries. */
    raft_replication__update_server(r, server_index, result);

    /* Commit entries if possible */
    raft_replication__maybe_commit(r, result->last_log_index);

    return 0;
}
