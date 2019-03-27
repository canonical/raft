#include "recv_request_vote_result.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "logging.h"
#include "recv.h"
#include "replication.h"

int recv__request_vote_result(struct raft *r,
                              unsigned id,
                              const char *address,
                              const struct raft_request_vote_result *result)
{
    size_t votes_index;
    int match;
    int rv;

    (void)address;

    assert(r != NULL);
    assert(id > 0);

    votes_index = configuration__index_of_voting(&r->configuration, id);
    if (votes_index == r->configuration.n) {
        infof(r->io, "non-voting or unknown server -> reject");
        return 0;
    }

    /* Ignore responses if we are not candidate anymore */
    if (r->state != RAFT_CANDIDATE) {
        debugf(r->io, "local server is not candidate -> ignore");
        return 0;
    }

    rv = recv__ensure_matching_terms(r, result->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        /* If the term in the result is older than ours, this is an old message
         * we should ignore, because the node who voted for us would have
         * obtained our term.  This happens if the network is pretty choppy. */
        debugf(r->io, "local term is higher -> ignore");
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
        if (election__tally(r, votes_index)) {
            infof(r->io, "votes quorum reached -> convert to leader");
            rv = convert__to_leader(r);
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
