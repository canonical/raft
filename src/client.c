#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "log.h"
#include "logging.h"
#include "membership.h"
#include "queue.h"
#include "replication.h"
#include "state.h"

int raft_apply(struct raft *r,
               struct raft_apply *req,
               const struct raft_buffer bufs[],
               const unsigned n,
               raft_apply_cb cb)
{
    raft_index index;
    int rv;

    assert(r != NULL);
    assert(bufs != NULL);
    assert(n > 0);

    if (r->state != RAFT_LEADER) {
        rv = RAFT_ERR_NOT_LEADER;
        goto err;
    }

    debugf(r->io, "client request: %d entries", n);

    /* Index of the first entry being appended. */
    index = log__last_index(&r->log) + 1;

    req->index = index;
    req->cb = cb;

    /* Append the new entries to the log. */
    rv = log__append_commands(&r->log, r->current_term, bufs, n);
    if (rv != 0) {
        goto err;
    }

    RAFT__QUEUE_PUSH(&r->leader_state.apply_reqs, &req->queue);

    rv = raft_replication__trigger(r, index);
    if (rv != 0) {
        goto err_after_log_append;
    }

    return 0;

err_after_log_append:
    log__discard(&r->log, index);
    RAFT__QUEUE_REMOVE(&req->queue);
err:
    assert(rv != 0);
    return rv;
}

static int raft_client__change_configuration(
    struct raft *r,
    const struct raft_configuration *configuration)
{
    raft_index index;
    raft_term term = r->current_term;
    int rv;

    /* Index of the entry being appended. */
    index = log__last_index(&r->log) + 1;

    /* Encode the new configuration and append it to the log. */
    rv = log__append_configuration(&r->log, term, configuration);
    if (rv != 0) {
        goto err;
    }

    if (configuration->n != r->configuration.n) {
        rv = raft_state__rebuild_next_and_match_indexes(r, configuration);
        if (rv != 0) {
            goto err;
        }
    }

    /* Update the current configuration if we've created a new object. */
    if (configuration != &r->configuration) {
        raft_configuration_close(&r->configuration);
        r->configuration = *configuration;
    }

    /* Start writing the new log entry to disk and send it to the followers. */
    rv = raft_replication__trigger(r, index);
    if (rv != 0) {
        /* TODO: restore the old next/match indexes and configuration. */
        goto err_after_log_append;
    }

    r->configuration_uncommitted_index = index;

    return 0;

err_after_log_append:
    log__truncate(&r->log, index);

err:
    assert(rv != 0);
    return rv;
}

int raft_add_server(struct raft *r, const unsigned id, const char *address)
{
    struct raft_configuration configuration;
    int rv;

    rv = raft_membership__can_change_configuration(r);
    if (rv != 0) {
        return rv;
    }

    debugf(r->io, "add server: id %d, address %s", id, address);

    /* Make a copy of the current configuration, and add the new server to
     * it. */
    raft_configuration_init(&configuration);

    rv = configuration__copy(&r->configuration, &configuration);
    if (rv != 0) {
        goto err;
    }

    rv = raft_configuration_add(&configuration, id, address, false);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    rv = raft_client__change_configuration(r, &configuration);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);

err:
    assert(rv != 0);
    return rv;
}

int raft_promote(struct raft *r, const unsigned id)
{
    const struct raft_server *server;
    size_t server_index;
    raft_index last_index;
    int rv;

    rv = raft_membership__can_change_configuration(r);
    if (rv != 0) {
        return rv;
    }

    debugf(r->io, "promote server: id %d", id);

    server = configuration__get(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_EBADID;
        goto err;
    }

    if (server->voting) {
        rv = RAFT_EALREADYVOTING;
        goto err;
    }

    server_index = configuration__index_of(&r->configuration, id);
    assert(server_index < r->configuration.n);

    last_index = log__last_index(&r->log);

    if (r->leader_state.replication[server_index].match_index == last_index) {
        /* The log of this non-voting server is already up-to-date, so we can
         * ask its promotion immediately. */
        r->configuration.servers[server_index].voting = true;

        rv = raft_client__change_configuration(r, &r->configuration);
        if (rv != 0) {
            r->configuration.servers[server_index].voting = false;
            return rv;
        }

        return 0;
    }

    r->leader_state.promotee_id = server->id;

    /* Initialize the first catch-up round. */
    r->leader_state.round_number = 1;
    r->leader_state.round_index = last_index;
    r->leader_state.round_duration = 0;

    /* Immediately initiate an AppendEntries request. */
    rv = raft_replication__send_append_entries(r, server_index);
    if (rv != 0 && rv != RAFT_ERR_IO_CONNECT) {
        /* This error is not fatal. */
        warnf(r->io, "failed to send append entries to server %ld: %s (%d)",
              server->id, raft_strerror(rv), rv);
    }

    return 0;

err:
    assert(rv != 0);

    return rv;
}

int raft_remove_server(struct raft *r, const unsigned id)
{
    const struct raft_server *server;
    struct raft_configuration configuration;
    int rv;

    rv = raft_membership__can_change_configuration(r);
    if (rv != 0) {
        return rv;
    }

    server = configuration__get(&r->configuration, id);
    if (server == NULL) {
        rv = RAFT_EBADID;
        goto err;
    }

    debugf(r->io, "remove server: id %d", id);

    /* Make a copy of the current configuration, and remove the given server
     * from it. */
    raft_configuration_init(&configuration);

    rv = configuration__copy(&r->configuration, &configuration);
    if (rv != 0) {
        goto err;
    }

    rv = configuration__remove(&configuration, id);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    rv = raft_client__change_configuration(r, &configuration);
    if (rv != 0) {
        goto err_after_configuration_copy;
    }

    return 0;

err_after_configuration_copy:
    raft_configuration_close(&configuration);

err:
    assert(rv != 0);
    return rv;
}
