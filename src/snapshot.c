#include "snapshot.h"
#include "logging.h"
#include "assert.h"
#include "log.h"

void raft_snapshot__close(struct raft_snapshot *s)
{
    unsigned i;
    raft_configuration_close(&s->configuration);
    for (i = 0; i < s->n_bufs; i++) {
        raft_free(s->bufs[0].base);
    }
    raft_free(s->bufs);
}

int snapshot__restore(struct raft *r, struct raft_snapshot *snapshot)
{
    int rc;

    assert(snapshot->n_bufs == 1);
    assert(log__n_entries(&r->log) == 0);

    rc = r->fsm->restore(r->fsm, &snapshot->bufs[0]);
    if (rc != 0) {
        errorf(r->io, "restore snapshot %d: %s", snapshot->index,
               raft_strerror(rc));
        return rc;
    }

    r->snapshot.index = snapshot->index;
    r->snapshot.term = snapshot->term;

    raft_configuration_close(&r->configuration);
    r->configuration = snapshot->configuration;
    r->configuration_index = snapshot->configuration_index;

    log__set_offset(&r->log, snapshot->index);

    r->commit_index = snapshot->index;
    r->last_applied = snapshot->index;
    r->last_stored = snapshot->index;

    raft_free(snapshot->bufs);
    raft_free(snapshot);

    return 0;
}
