#include <stdio.h>

#include "../include/raft/fixture.h"
#include "../include/raft/io_stub.h"

static int setup_server(unsigned i,
                        struct raft_fixture_server *s,
                        struct raft_fsm *fsm)
{
    int rc;
    unsigned id = i + 1;
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    sprintf(s->address, "%u", id);
    rc = raft_init(&s->raft, &s->io, fsm, NULL, id, s->address);
    if (rc != 0) {
        return rc;
    }
    return 0;
}

static void tear_down_server(struct raft_fixture_server *s)
{
    raft_close(&s->raft, NULL);
    raft_io_stub_close(&s->io);
}

int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       struct raft_fsm *fsms)
{
    unsigned i;
    int rc;
    f->n = n;
    f->servers = raft_malloc(n * sizeof *f->servers);
    if (f->servers == NULL) {
        return RAFT_ENOMEM;
    }
    for (i = 0; i < n; i++) {
        rc = setup_server(i, &f->servers[i], &fsms[i]);
        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}

void raft_fixture_tear_down(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        tear_down_server(&f->servers[i]);
    }
    raft_free(f->servers);
}
