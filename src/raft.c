#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "election.h"
#include "error.h"
#include "log.h"
#include "logger.h"
#include "io_queue.h"
#include "queue.h"
#include "state.h"

#define RAFT__DEFAULT_ELECTION_TIMEOUT 1000
#define RAFT__DEFAULT_HEARTBEAT_TIMEOUT 100

void raft_init(struct raft *r,
               struct raft_io *io,
               struct raft_fsm *fsm,
               void *data,
               const unsigned id)
{
    int i;

    assert(r != NULL);

    r->id = id;
    r->fsm = fsm;

    r->io_.data = NULL;
    r->io_.start = NULL;
    r->io_.stop = NULL;
    r->io_.close = NULL;

    /* TODO: move out of io_ */
    r->io_.queue.requests = NULL;
    r->io_.queue.size = 0;

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

    raft_set_logger(r, &raft_default_logger);

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

    /* Last error */
    strcpy(r->errmsg, "");

    r->io_queue.requests = NULL;
    r->io_queue.size = 0;

    r->io = io;
}

void raft_close(struct raft *r)
{
    assert(r != NULL);

    if (r->io_.close != NULL) {
        r->io_.close(r);
    }

    raft_queue__close(r);

    raft_state__clear(r);
    raft_log__close(&r->log);
    raft_configuration_close(&r->configuration);
}

int raft_start(struct raft *r)
{
    int rv;

    assert(r != NULL);
    assert(r->state == RAFT_STATE_UNAVAILABLE);

    raft__debugf(r, "start");

    //assert(r->io_.start != NULL);

    assert(r->heartbeat_timeout != 0);
    assert(r->heartbeat_timeout < r->election_timeout);

    return 0;

    rv = r->io_.start(r, r->heartbeat_timeout, raft_tick);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_stop(struct raft *r)
{
    int rv;

    assert(r != NULL);

    raft__debugf(r, "stop");

    assert(r->io_.stop != NULL);
    rv = r->io_.stop(r);
    if (rv != 0) {
        return rv;
    }

    return 0;
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
