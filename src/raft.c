#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "log.h"
#include "logging.h"
#include "state.h"

#define DEFAULT_ELECTION_TIMEOUT 1000 /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100 /* One tenth of a second */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r->io, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const unsigned id,
              const char *address)
{
    int i;
    int rv;

    assert(r != NULL);

    r->io = io;
    r->io->data = r;
    r->fsm = fsm;
    r->id = id;
    /* Make a copy of the address */
    r->address = raft_malloc(strlen(address) + 1);
    if (r->address == NULL) {
        return RAFT_ENOMEM;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    log__init(&r->log);
    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->election_timeout_rand = 0;
    r->last_tick = 0;
    r->timer = 0;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.put.data = NULL;
    for (i = 0; i < RAFT_EVENT_N; i++) {
        r->watchers[i] = NULL;
    }
    r->close_cb = NULL;
    rv = r->io->init(r->io, r->id, r->address);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

static void io_close_cb(struct raft_io *io)
{
    struct raft *r = io->data;
    infof(r->io, "stopped");

    raft_free(r->address);
    log__close(&r->log);
    raft_configuration_close(&r->configuration);

    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
    assert(r != NULL);
    assert(r->close_cb == NULL);
    if (r->state != RAFT_UNAVAILABLE) {
        convert__to_unavailable(r);
    }
    r->close_cb = cb;
    r->io->close(r->io, io_close_cb);
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
    raft_election__reset_timer(r);
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

const char *raft_state_name(struct raft *r)
{
    return raft_state_names[r->state];
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    assert(r->state == RAFT_UNAVAILABLE);

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}
