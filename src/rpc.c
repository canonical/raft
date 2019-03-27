#include "../include/raft.h"

#include "assert.h"
#include "convert.h"
#include "log.h"
#include "logging.h"
#include "rpc_append_entries.h"
#include "rpc_install_snapshot.h"
#include "rpc_request_vote.h"

/* Bump the current term to the given value and reset our vote, persiting the
 * change to disk. */
static int bump_current_term(struct raft *r, raft_term term)
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

int raft_rpc__ensure_matching_terms(struct raft *r, raft_term term, int *match)
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
        infof(r->io, "remote server term is higher -> bump local term");
        rv = bump_current_term(r, term);
        if (rv != 0) {
            return rv;
        }
        if (r->state != RAFT_FOLLOWER) {
            /* Also convert to follower. */
            infof(r->io, "remote server term is higher -> step down");
            convert__to_follower(r);
        }
        *match = 1;
    } else {
        *match = 0;
    }

    return 0;
}
