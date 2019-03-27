#include "recv.h"
#include "assert.h"
#include "convert.h"
#include "rpc_append_entries.h"
#include "rpc_install_snapshot.h"
#include "rpc_request_vote.h"
#include "logging.h"

static const char *message_descs[] = {"append entries", "append entries result",
                                      "request vote", "request vote result",
                                      "install snapshot"};


/* Dispatch a single RPC message to the appropriate handler. */
static int recv(struct raft *r, struct raft_message *message)
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
