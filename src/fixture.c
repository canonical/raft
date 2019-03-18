#include "../include/raft/fixture.h"

static int init_server(unsigned i,
                       struct raft_fixture_server *server,
                       struct raft_fsm *fsm)
{
}

int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       struct raft_fsm *fsms)
{
    unsigned i;
    int rc;
    f->servers = raft_malloc(n * sizeof *f->servers);
    if (f->servers == NULL) {
        return RAFT_ENOMEM;
    }
    for (i = 0; i < n; i++) {
        rc = init_server(i, &f->servers[i], &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}
