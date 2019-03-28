#include "recv.h"
#include "assert.h"
#include "convert.h"
#include "log.h"
#include "logging.h"
#include "recv_append_entries.h"
#include "recv_append_entries_result.h"
#include "recv_install_snapshot.h"
#include "recv_request_vote.h"
#include "recv_request_vote_result.h"
#include "string.h"

static const char *message_descs[] = {"append entries", "append entries result",
                                      "request vote", "request vote result",
                                      "install snapshot"};

/* Dispatch a single RPC message to the appropriate handler. */
static int recv(struct raft *r, struct raft_message *message)
{
    int rv;

    if (message->type < RAFT_IO_APPEND_ENTRIES ||
        message->type > RAFT_IO_INSTALL_SNAPSHOT) {
        warnf(r->io, "received unknown message type type: %d", message->type);
        return 0;
    }

    debugf(r->io, "received %s from server %ld",
           message_descs[message->type - 1], message->server_id);

    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = recv__append_entries(r, message->server_id,
                                      message->server_address,
                                      &message->append_entries);
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            rv = recv__append_entries_result(r, message->server_id,
                                             message->server_address,
                                             &message->append_entries_result);
            break;
        case RAFT_IO_REQUEST_VOTE:
            rv = recv__request_vote(r, message->server_id,
                                    message->server_address,
                                    &message->request_vote);
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            rv = recv__request_vote_result(r, message->server_id,
                                           message->server_address,
                                           &message->request_vote_result);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = raft_rpc__recv_install_snapshot(r, message->server_id,
                                                 message->server_address,
                                                 &message->install_snapshot);
            break;
    };

    if (rv != 0 && rv != RAFT_ERR_IO_CONNECT) {
        errorf(r->io, "recv: %s: %s", message_descs[message->type - 1],
               raft_strerror(rv));
        return rv;
    }
    return 0;
}

void recv_cb(struct raft_io *io, struct raft_message *message)
{
    struct raft *r;
    int rv;
    r = io->data;
    rv = recv(r, message);
    if (rv != 0) {
        convert__to_unavailable(r);
    }
}

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

int recv__ensure_matching_terms(struct raft *r, raft_term term, int *match)
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
        char msg[1204];
        sprintf(msg, "remote term %lld is higher than %lld -> bump local term",
                term, r->current_term);
        if (r->state != RAFT_FOLLOWER) {
            strcat(msg, " and step down");
        }
        infof(r->io, msg);
        rv = bump_current_term(r, term);
        if (rv != 0) {
            return rv;
        }
        if (r->state != RAFT_FOLLOWER) {
            /* Also convert to follower. */
            convert__to_follower(r);
        }
        *match = 1;
    } else {
        *match = 0;
    }

    return 0;
}
