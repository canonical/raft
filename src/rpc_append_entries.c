#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "replication.h"
#include "rpc.h"
#include "state.h"

static void raft_rpc__recv_append_entries_send_cb(struct raft_io_send *req,
                                                  int status)
{
    (void)status;
    raft_free(req);
}

int raft_rpc__recv_append_entries(struct raft *r,
                                  const unsigned id,
                                  const char *address,
                                  const struct raft_append_entries *args)
{
    struct raft_io_send *req;
    struct raft_message message;
    struct raft_append_entries_result *result = &message.append_entries_result;
    int match;
    bool async;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(args != NULL);
    assert(address != NULL);

    raft_debugf(r->logger, "received %d entries from server %ld",
                args->n_entries, id);

    result->success = false;
    result->last_log_index = raft_log__last_index(&r->log);

    rv = raft_rpc__ensure_matching_terms(r, args->term, &match);
    if (rv != 0) {
        return rv;
    }

    /* From Figure 3.1:
     *
     *   AppendEntries RPC: Receiver implementation: Reply false if term <
     *   currentTerm.
     */
    if (match < 0) {
        raft_debugf(r->logger, "local term is higher -> reject ");
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
     *
     * Note that it should not be possible for us to be in leader state, because
     * the leader that is sending us the request should have either a lower term
     * (and in that case we reject the request above), or a higher term (and in
     * that case we step down).
     */
    assert(r->state == RAFT_STATE_FOLLOWER || r->state == RAFT_STATE_CANDIDATE);
    assert(r->current_term == args->term);

    if (r->state == RAFT_STATE_CANDIDATE) {
        raft_debugf(r->logger, "discovered leader -> step down ");
        rv = raft_state__convert_to_follower(r, args->term);
        if (rv != 0) {
            return rv;
        }
    }

    assert(r->state == RAFT_STATE_FOLLOWER);

    /* Update current leader because the term in this AppendEntries RPC is up to
     * date. */
    r->follower_state.current_leader_id = id;

    /* Reset the election timer. */
    r->timer = 0;

    rv = raft_replication__append(r, args, &result->success, &async);
    if (rv != 0) {
        return rv;
    }

    if (async) {
        return 0;
    }

    if (result->success) {
        /* Echo back to the leader the point that we reached. */
        result->last_log_index = args->prev_log_index + args->n_entries;
    }

reply:
    result->term = r->current_term;

    /* Free the entries batch, if any. */
    if (args->n_entries > 0 && args->entries[0].batch != NULL) {
        raft_free(args->entries[0].batch);
    }

    if (args->entries != NULL) {
        raft_free(args->entries);
    }

    message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
    message.server_id = id;
    message.server_address = address;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_ENOMEM;
    }

    rv = r->io->send(r->io, req, &message,
                     raft_rpc__recv_append_entries_send_cb);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

int raft_rpc__recv_append_entries_result(
    struct raft *r,
    const unsigned id,
    const char *address,
    const struct raft_append_entries_result *result)
{
    int match;
    const struct raft_server *server;
    int rv;

    assert(r != NULL);
    assert(id > 0);
    assert(address != NULL);
    assert(result != NULL);

    raft_debugf(r->logger, "received append entries result from server %ld",
                id);

    if (result->success != 1) {
        /* TODO: handle failures? */
        // assert(0);
    }

    if (r->state != RAFT_STATE_LEADER) {
        raft_debugf(r->logger, "local server is not leader -> ignore");
        return 0;
    }

    rv = raft_rpc__ensure_matching_terms(r, result->term, &match);
    if (rv != 0) {
        return rv;
    }

    if (match < 0) {
        raft_debugf(r->logger, "local term is higher -> ignore ");
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
    server = raft_configuration__get(&r->configuration, id);
    if (server == NULL) {
        raft_errorf(r->logger, "unknown server -> ignore");
        return 0;
    }

    /* Update the match/next indexes and possibly send further entries. */
    rv = raft_replication__update(r, server, result);
    if (rv != 0) {
        return rv;
    }

    /* Commit entries if possible */
    raft_replication__quorum(r, result->last_log_index);

    rv = raft_replication__apply(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
}
