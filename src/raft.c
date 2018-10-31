#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"

#include "configuration.h"
#include "election.h"
#include "io.h"
#include "log.h"
#include "state.h"

void raft_init(struct raft *r,
               struct raft_io *io,
               void *data,
               const unsigned id)
{
    int i;

    assert(r != NULL);
    assert(io != NULL);

    /* User-defined */
    r->io = io;
    r->id = id;
    r->data = data;

    /* Initial persistent server state. */
    r->current_term = 0;
    r->voted_for = 0;
    raft_log__init(&r->log);

    raft_configuration_init(&r->configuration);

    r->election_timeout = 1000;
    r->heartbeat_timeout = 100;

    raft_set_logger(r, &raft_default_logger);

    r->commit_index = 0;
    r->last_applied = 0;

    /* From Section §3.4:
     *
     *   When servers start up, they begin as followers.
     */
    r->state = RAFT_STATE_FOLLOWER;
    r->follower_state.current_leader = NULL;
    r->leader_state.next_index = NULL;
    r->leader_state.match_index = NULL;
    r->candidate_state.votes = NULL;

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

    raft_io__queue_close(r);

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

void raft_set_election_timeout_(struct raft *r, const unsigned election_timeout)
{
    r->election_timeout = election_timeout;
    raft_election__reset_timer(r);
}

const char *raft_state_name(struct raft *r)
{
    return raft_state_names[r->state];
}
