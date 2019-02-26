#include "snapshot.h"

void raft_snapshot__close(struct raft_snapshot *s)
{
    unsigned i;
    raft_configuration_close(&s->configuration);
    for (i = 0; i < s->n_bufs; i++) {
        raft_free(s->bufs[0].base);
    }
    raft_free(s->bufs);
}
