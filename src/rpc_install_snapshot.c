#include "rpc_install_snapshot.h"
#include "assert.h"
#include "log.h"
#include "replication.h"
#include "rpc.h"
#include "state.h"

static void send_append_entries_result_cb(struct raft_io_send *req, int status)
{
    (void)status;
    raft_free(req);
}

int raft_rpc__recv_install_snapshot(struct raft *r,
                                    const unsigned id,
                                    const char *address,
                                    struct raft_install_snapshot *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int rv;
    int match;
    bool async;

    assert(address != NULL);

    raft_infof(r->logger, "received snapshot %d from server %ld",
               args->last_index, id);

    result->success = false;
    result->last_log_index = log__last_index(&r->log);

    rv = raft_rpc__ensure_matching_terms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        raft_debugf(r->logger, "local term is higher -> reject ");
        goto reply;
    }

    /* TODO: this logic duplicates the one in the AppendEntries handler */
    assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_CANDIDATE) {
        raft_debugf(r->logger, "discovered leader -> step down ");
        rv = raft_state__convert_to_follower(r, args->term);
        if (rv != 0) {
            return rv;
        }
    }

    r->follower_state.current_leader.id = id;
    r->follower_state.current_leader.address = address;
    r->timer = 0;

    rv = raft_replication__install_snapshot(r, args, &result->success, &async);
    if (rv != 0) {
        return rv;
    }

    if (async) {
        return 0;
    }

    if (result->success) {
        /* Echo back to the leader the point that we reached. */
        result->last_log_index = args->last_index;
    }

reply:
    result->term = r->current_term;

    /* Free the snapshot data. */
    raft_configuration_close(&args->conf);
    raft_free(args->data.base);

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_ENOMEM;
    }

    rv = r->io->send(r->io, req, &message, send_append_entries_result_cb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}
