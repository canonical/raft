#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "election.h"
#include "log.h"
#include "logger.h"
#include "queue.h"
#include "state.h"

void raft_init(struct raft *r,
               struct raft_io *io,
               struct raft_fsm *fsm,
               void *data,
               const unsigned id)
{
    int i;

    assert(r != NULL);

    r->backend.data = NULL;
    r->backend.start = NULL;
    r->backend.stop = NULL;
    r->backend.close = NULL;

    /* User-defined */
    r->io = io;
    r->fsm = fsm;
    r->id = id;
    r->data = data;

    /* Initial persistent server state. */
    r->current_term = 0;
    r->voted_for = 0;
    raft_log__init(&r->log);

    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;

    r->election_timeout = 1000;
    r->heartbeat_timeout = 100;

    raft_set_logger(r, &raft_default_logger);

    r->commit_index = 0;
    r->last_applied = 0;

    r->state = RAFT_STATE_NONE;

    r->rand = rand;
    raft_election__reset_timer(r);

    for (i = 0; i < RAFT_EVENT_N; i++) {
        r->watchers[i] = NULL;
    }

    /* Context. */
    r->ctx.state = &r->state;
    r->ctx.current_term = &r->current_term;
    strcpy(r->errmsg, "");

    r->io_queue.requests = NULL;
    r->io_queue.size = 0;
}

void raft_close(struct raft *r)
{
    assert(r != NULL);

    if (r->backend.close != NULL) {
        r->backend.close(r);
    }

    raft_queue__close(r);

    raft_state__clear(r);
    raft_log__close(&r->log);
    raft_configuration_close(&r->configuration);
}

void raft_set_logger(struct raft *r, const struct raft_logger *logger)
{
    assert(r != NULL);
    assert(logger != NULL);

    r->logger = *logger;
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

int raft_start(struct raft *r)
{
    int rv;

    assert(r != NULL);

    raft__debugf(r, "start");

    if (r->backend.start != NULL) {
        assert(r->heartbeat_timeout != 0);
        assert(r->heartbeat_timeout < r->election_timeout);

        rv = r->backend.start(r, r->heartbeat_timeout);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

int raft_stop(struct raft *r)
{
    int rv;

    assert(r != NULL);

    raft__debugf(r, "stop");

    if (r->backend.stop != NULL) {
        rv = r->backend.stop(r);
        if (rv != 0) {
            return rv;
        }
    }

    return 0;
}

const char *raft_state_name(struct raft *r)
{
    return raft_state_names[r->state];
}

const struct raft_entry *raft_get_entry(struct raft *r, const raft_index index)
{
    assert(r != NULL);
    assert(index > 0);

    return raft_log__get(&r->log, index);
}
