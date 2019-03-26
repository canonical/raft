#include "../include/raft.h"

#include "assert.h"
#include "convert.h"
#include "log.h"
#include "logging.h"
#include "rpc_append_entries.h"
#include "rpc_install_snapshot.h"
#include "rpc_request_vote.h"
#include "state.h"

static const char *message_descs[] = {"append entries", "append entries result",
                                      "request vote", "request vote result",
                                      "install snapshot"};

/* Dispatch a single RPC message to the appropriate handler. */
static int dispatch(struct raft *r, struct raft_message *message)
{
    int rv;
    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = raft_rpc__recv_append_entries(r, message->server_id,
                                               message->server_address,
                                               &message->append_entries);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            rv = raft_rpc__recv_append_entries_result(
                r, message->server_id, message->server_address,
                &message->append_entries_result);
            break;
        case RAFT_IO_REQUEST_VOTE:
            rv = raft_rpc__recv_request_vote(r, message->server_id,
                                             message->server_address,
                                             &message->request_vote);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            rv = raft_rpc__recv_request_vote_result(
                r, message->server_id, message->server_address,
                &message->request_vote_result);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = raft_rpc__recv_install_snapshot(r, message->server_id,
                                                 message->server_address,
                                                 &message->install_snapshot);
            break;
        default:
            warnf(r->io, "rpc: unknown message type type: %d", message->type);
            return 0;
    };

    if (rv != 0 && rv != RAFT_ERR_IO_CONNECT) {
        errorf(r->io, "rpc %s: %s", message_descs[message->type],
               raft_strerror(rv));
        return rv;
    }
    return 0;
}

void rpc__recv_cb(struct raft_io *io, struct raft_message *message)
{
    struct raft *r;
    int rv;
    r = io->data;
    rv = dispatch(r, message);
    if (rv != 0) {
        convert__to_unavailable(r);
    }
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
        if (r->state == RAFT_FOLLOWER) {
            /* Just bump the current term */
            infof(r->io, "remote server term is higher -> bump local term");
            rv = raft_state__bump_current_term(r, term);
        } else {
            /* Bump current state and also convert to follower. */
            infof(r->io, "remote server term is higher -> step down");
            rv = raft_state__bump_current_term(r, term);
            convert__to_follower(r);
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
