#include "../include/raft.h"

#include <string.h>

#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "log.h"
#include "logging.h"

#define DEFAULT_ELECTION_TIMEOUT 1000 /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100 /* One tenth of a second */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              struct raft_logger *logger,
              const unsigned id,
              const char *address)
{
    int rv;

    assert(r != NULL);

    r->io = io;
    r->io->data = r;
    r->fsm = fsm;
    r->logger = logger;
    r->id = id;
    /* Make a copy of the address */
    r->address = raft_malloc(strlen(address) + 1);
    if (r->address == NULL) {
        return RAFT_NOMEM;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    logInit(&r->log);
    raft_configuration_init(&r->configuration);
    r->configuration_index = 0;
    r->configuration_uncommitted_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->snapshot.pending.term = 0;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    r->snapshot.put.data = NULL;
    r->close_cb = NULL;
    rv = r->io->init(r->io, r->logger, r->id, r->address);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

static void io_close_cb(struct raft_io *io)
{
    struct raft *r = io->data;
    infof(r, "stopped");

    raft_free(r->address);
    logClose(&r->log);
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
        convertToUnavailable(r);
    }
    r->close_cb = cb;
    r->io->close(r->io, io_close_cb);
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
    r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
    r->snapshot.trailing = n;
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

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}
