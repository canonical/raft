#include "snapshot.h"
#include "assert.h"
#include "log.h"
#include "logging.h"

void snapshot__close(struct raft_snapshot *s)
{
    unsigned i;
    raft_configuration_close(&s->configuration);
    for (i = 0; i < s->n_bufs; i++) {
        raft_free(s->bufs[0].base);
    }
    raft_free(s->bufs);
}

void snapshot__destroy(struct raft_snapshot *s)
{
    snapshot__close(s);
    raft_free(s);
}

int snapshot__restore(struct raft *r, struct raft_snapshot *snapshot)
{
    int rc;

    assert(snapshot->n_bufs == 1);

    rc = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
    if (rc != 0) {
        errorf(r->io, "restore snapshot %d: %s", snapshot->index,
               raft_strerror(rc));
        return rc;
    }

    raft_configuration_close(&r->configuration);
    r->configuration = snapshot->configuration;
    r->configuration_index = snapshot->configuration_index;

    r->commit_index = snapshot->index;
    r->last_applied = snapshot->index;
    r->last_stored = snapshot->index;

    /* Don't free the snapshot data buffer, as ownership has been trasfered to
     * the fsm. */
    raft_free(snapshot->bufs);

    return 0;
}
