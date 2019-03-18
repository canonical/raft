#include <stdio.h>

#include "../include/raft/fixture.h"
#include "../include/raft/io_stub.h"

#include "assert.h"

static int setup_server(unsigned i,
                        struct raft_fixture_server *s,
                        struct raft_fsm *fsm)
{
    int rc;
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    raft_io_stub_set_latency(&s->io, 5, 50);
    s->id = i + 1;
    sprintf(s->address, "%u", s->id);
    rc = raft_init(&s->raft, &s->io, fsm, NULL, s->id, s->address);
    if (rc != 0) {
        return rc;
    }
    s->raft.heartbeat_timeout = 50;
    raft_set_election_timeout(&s->raft, 250);
    return 0;
}

static void tear_down_server(struct raft_fixture_server *s)
{
    raft_close(&s->raft, NULL);
    raft_io_stub_close(&s->io);
}

int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       unsigned n_voting,
                       struct raft_fsm *fsms)
{
    struct raft_configuration configuration;
    unsigned i;
    unsigned j;
    int rc;
    assert(n >= 1);
    assert(n_voting >= 1);
    assert(n_voting <= n);

    f->n = n;
    f->servers = raft_malloc(n * sizeof *f->servers);
    if (f->servers == NULL) {
        return RAFT_ENOMEM;
    }

    /* Setup all servers */
    raft_configuration_init(&configuration);
    for (i = 0; i < n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        bool voting = i < n_voting;
        rc = setup_server(i, s, &fsms[i]);
        if (rc != 0) {
            return rc;
        }
        rc = raft_configuration_add(&configuration, s->id, s->address, voting);
        if (rc != 0) {
            return rc;
        }
    }

    /* Connect all servers to each another */
    for (i = 0; i < f->n; i++) {
        for (j = 0; j < f->n; j++) {
            struct raft_io *io1 = &f->servers[i].io;
            struct raft_io *io2 = &f->servers[j].io;
            if (i == j) {
                continue;
            }
            raft_io_stub_connect(io1, io2);
        }
    }

    /* Bootstrap and start */
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        rc = raft_bootstrap(&s->raft, &configuration);
        if (rc != 0) {
            return rc;
        }
	rc = raft_start(&s->raft);
        if (rc != 0) {
            return rc;
        }
    }

    raft_configuration_close(&configuration);

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
