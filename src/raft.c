#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "rpc.h"
#include "rpc_append_entries.h"
#include "rpc_request_vote.h"
#include "state.h"
#include "tick.h"

#define RAFT__DEFAULT_ELECTION_TIMEOUT 1000
#define RAFT__DEFAULT_HEARTBEAT_TIMEOUT 100

int raft_init(struct raft *r,
              struct raft_logger *logger,
              struct raft_io *io,
              struct raft_fsm *fsm,
              void *data,
              const unsigned id,
              const char *address)
{
    int i;

    assert(r != NULL);

    r->logger = logger;
    r->io = io;
    r->fsm = fsm;

    r->id = id;

    /* Make a copy of the address */
    r->address = raft_malloc(strlen(address) + 1);
    if (r->address == NULL) {
        return RAFT_ERR_NOMEM;
    }
    strcpy(r->address, address);

    /* User-defined */
    r->data = data;

    /* Initial persistent server state. */
    r->current_term = 0;
    r->voted_for = 0;
    raft_log__init(&r->log);

    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;

    r->election_timeout = RAFT__DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = RAFT__DEFAULT_HEARTBEAT_TIMEOUT;

    r->commit_index = 0;
    r->last_applied = 0;

    r->state = RAFT_STATE_UNAVAILABLE;

    r->rand = rand;
    raft_election__reset_timer(r);

    for (i = 0; i < RAFT_EVENT_N; i++) {
        r->watchers[i] = NULL;
    }

    /* Context. */
    r->ctx.state = &r->state;
    r->ctx.current_term = &r->current_term;

    r->stop.data = NULL;
    r->stop.cb = NULL;

    return 0;
}

void raft_close(struct raft *r)
{
    assert(r != NULL);

    raft_free(r->address);
    raft_state__clear(r);
    raft_log__close(&r->log);
    raft_configuration_close(&r->configuration);
}

static void raft__stop_cb(void *data)
{
    struct raft *r;

    r = data;

    if (r->stop.cb != NULL) {
        r->stop.cb(r->stop.data);
    }
}

static void raft__tick_cb(void *data, unsigned msecs)
{
    struct raft *r;

    r = data;

    raft__tick(r, msecs);
}

static void raft__recv_cb(void *data, struct raft_message *message)
{
    struct raft *r;
    int rv;

    r = data;

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
        default:
            rv = RAFT_ERR_MALFORMED;
            break;
    };

    if (rv != 0) {
        raft_warnf(r->logger, "handle message with type %d: %s", message->type,
                   raft_strerror(rv));
    }
}

int raft_start(struct raft *r)
{
    int rv;
    raft_index start_index;
    struct raft_entry *entries;
    size_t n_entries;
    size_t i;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_UNAVAILABLE);

    raft_debugf(r->logger, "start");

    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);

    rv = r->io->start(r->io, r->id, r->address, r->heartbeat_timeout, r,
                      raft__tick_cb, raft__recv_cb);
    if (rv != 0) {
        goto err;
    }

    rv = r->io->load(r->io, &r->current_term, &r->voted_for, &start_index,
                     &entries, &n_entries);
    if (rv != 0) {
        goto err_after_io_start;
    }

    /* If the start index is 1 and the log is not empty, then the first entry
     * must be a configuration*/
    if (start_index == 1 && n_entries > 0) {
        assert(entries[0].type == RAFT_LOG_CONFIGURATION);

        /* As a small optimization, bump the commit index to 1 since we require
         * the first entry to be the same on all servers. */
        r->commit_index = 1;
        r->last_applied = 1;
    }

    /* Look for the most recent configuration entry, if any. */
    for (i = n_entries; i > 0; i--) {
        struct raft_entry *entry = &entries[i - 1];

        if (entry->type != RAFT_LOG_CONFIGURATION) {
            continue;
        }

        rv = raft_configuration_decode(&entry->buf, &r->configuration);
        if (rv != 0) {
            goto err_after_load;
        }

        r->configuration_index = start_index + i - 1;

        break;
    }

    /* Append the entries to the log. */
    for (i = 0; i < n_entries; i++) {
        struct raft_entry *entry = &entries[i];

        rv = raft_log__append(&r->log, entry->term, entry->type, &entry->buf,
                              entry->batch);
        if (rv != 0) {
            goto err_after_load;
        }
    }

    if (entries != NULL) {
        raft_free(entries);
    }

    r->state = RAFT_STATE_FOLLOWER;

    return 0;

err_after_load:
    if (entries != NULL) {
        raft_free(entries[0].batch);
        raft_free(entries);
    }

err_after_io_start:
    r->io->stop(r->io, r, raft__stop_cb);

err:
    assert(rv != 0);

    return rv;
}

int raft_stop(struct raft *r, void *data, void (*cb)(void *data))
{
    int rv;

    assert(r != NULL);
    assert(r->stop.data == NULL);
    assert(r->stop.cb == NULL);

    r->stop.data = data;
    r->stop.cb = cb;

    raft_debugf(r->logger, "stop");

    rv = r->io->stop(r->io, r, raft__stop_cb);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

void raft_set_rand(struct raft *r, int (*rand)())
{
    r->rand = rand;
    raft_election__reset_timer(r);
}

void raft_set_election_timeout(struct raft *r, const unsigned election_timeout)
{
    r->election_timeout = election_timeout;
    raft_election__reset_timer(r);
}

const char *raft_state_name(struct raft *r)
{
    return raft_state_names[r->state];
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    assert(r->state == RAFT_STATE_UNAVAILABLE);

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}
