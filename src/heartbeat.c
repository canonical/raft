#include "heartbeat.h"
#include "assert.h"
#include "log.h"
#include "logging.h"
#include "replication.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(MSG, ...) debugf(r->io, MSG, __VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

static void send_heartbeat(struct raft *r, unsigned i)
{
    struct raft_server *server = &r->configuration.servers[i];
    struct raft_progress *progress = &r->leader_state.progress[i];
    int rc;

    /* If we have already sent something during this heartbeat interval. skip
     * the heartbeat */
    if (progress->recent_send) {
        tracef("skip heartbeat for server %u", server->id);
        goto out;
    }

    rc = replication__trigger(r, i);
    if (rc != 0 && rc != RAFT_CANTCONNECT) {
        /* This is not a critical failure, let's just log it. */
        warnf(r->io, "send heartbeat to server %ld: %s", server->id,
              raft_strerror(rc));
    }

out:
    /* Reset the send flag. If during the next heartbeat_interval milliseconds
     * we'll send some message, the flag will be set to true and at the next
     * heatbeat timeout we won't send a heartbeat message. */
    progress->recent_send = false;
}

void heartbeat__send(struct raft *r)
{
    unsigned i;
    for (i = 0; i < r->configuration.n; i++) {
        struct raft_server *server = &r->configuration.servers[i];
        if (server->id == r->id) {
            continue;
        }
        send_heartbeat(r, i);
    }
}
