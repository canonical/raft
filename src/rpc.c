#include <assert.h>

#include "../include/raft.h"

#include "error.h"
#include "log.h"
#include "state.h"

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
        const char *errmsg;

        if (r->state == RAFT_STATE_FOLLOWER) {
            /* Just bump the current term */
            raft_infof(r->logger, "remote server term is higher -> bump local term");
            rv = raft_state__bump_current_term(r, term);
            errmsg = "bump local term";
        } else {
            /* Bump current state and also convert to follower. */
            raft_infof(r->logger, "remote server term is higher -> step down");
            rv = raft_state__convert_to_follower(r, term);
            errmsg = "convert to follower";
        }

        if (rv != 0) {
            raft_error__wrapf(r, errmsg);
            return rv;
        }

        *match = 1;
    } else {
        *match = 0;
    }

    return 0;
}
