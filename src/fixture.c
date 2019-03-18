#include <stdio.h>

#include "../include/raft/fixture.h"
#include "../include/raft/io_stub.h"

#include "assert.h"

static int setup_server(unsigned i,
                        struct raft_fixture_server *s,
                        struct raft_fsm *fsm,
                        int (*random)(int, int))
{
    int rc;
    rc = raft_io_stub_init(&s->io);
    if (rc != 0) {
        return rc;
    }
    raft_io_stub_set_random(&s->io, random);
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
                       struct raft_fsm *fsms,
                       int (*random)(int, int))
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
        rc = setup_server(i, s, &fsms[i], random);
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

/* Flush any pending write to the disk and any pending message into the network
 * buffers (this will assign them a latency timer). */
static void flush_io(struct raft_fixture *f)
{
    size_t i;
    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        raft_io_stub_flush_all(io);
    }
}

/* Figure what's the lowest delivery timer across all stub I/O instances,
 * i.e. the time at which the next message should be delivered (if any is
 * pending). */
static int lowest_deliver_timeout(struct raft_fixture *f)
{
    int min_timeout = -1;
    size_t i;

    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        int timeout;
        timeout = raft_io_stub_next_deliver_timeout(io);
        if (timeout == -1) {
            continue;
        }
        if (min_timeout == -1 || timeout < min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/* Check what's the raft instance whose timer is closest to expiration. */
static unsigned lowest_raft_timeout(struct raft_fixture *f)
{
    size_t i;
    unsigned min_timeout = 0; /* Lowest remaining time before expiration */

    for (i = 0; i < f->n; i++) {
        struct raft *r = &f->servers[i].raft;
        unsigned timeout; /* Milliseconds remaining before expiration. */

        timeout = raft_next_timeout(r);
        if (min_timeout == 0 || timeout < min_timeout) {
            min_timeout = timeout;
        }
    }

    return min_timeout;
}

/* Fire either a message delivery or a raft tick, depending on whose timer is
 * closest to expiration. */
static void advance(struct raft_fixture *f, unsigned msecs)
{
    size_t i;

    for (i = 0; i < f->n; i++) {
        struct raft_io *io = &f->servers[i].io;
        raft_io_stub_advance(io, msecs);
    }
}

void raft_fixture_step(struct raft_fixture *f)
{
    int deliver_timeout;
    unsigned raft_timeout;
    unsigned timeout;

    /* First flush I/O operations. */
    flush_io(f);

    /* Second, figure what's the message with the lowest timer (i.e. the
     * message that should be delivered first) */
    deliver_timeout = lowest_deliver_timeout(f);

    /* Now check what's the raft instance whose timer is closest to
     * expiration. */
    raft_timeout = lowest_raft_timeout(f);

    /* Fire either a raft tick or a message delivery. */
    if (deliver_timeout != -1 && (unsigned)deliver_timeout < raft_timeout) {
        timeout = deliver_timeout;
    } else {
        timeout = raft_timeout;
    }

    advance(f, timeout + 1);
}

static unsigned leader_id(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        if (raft_state(&s->raft) == RAFT_LEADER) {
            return s->id;
        }
    }
    return 0;
}

/* Enable/disable dropping outgoing messages of a certain type from all servers
 * except one. */
static void drop_all_except(struct raft_fixture *f,
                            int type,
                            bool flag,
                            unsigned id)
{
    unsigned i;
    for (i = 0; i < f->n; i++) {
        struct raft_fixture_server *s = &f->servers[i];
        if (s->id == id) {
            continue;
        }
        raft_io_stub_drop(&s->io, type, flag);
    }
}

void raft_fixture_elect(struct raft_fixture *f, unsigned id)
{
    unsigned i;

    /* Make sure there's currently no leader. */
    assert(leader_id(f) == 0);

    /* TODO: make sure that the server with the given id is a voting one */

    /* Prevent all servers from sending request vote messages, except for the
     * one to be elected. */
    drop_all_except(f, RAFT_IO_REQUEST_VOTE, true, id);

    for (i = 0; i < 100; i++) {
        unsigned elected_id;
        raft_fixture_step(f);
        elected_id = leader_id(f);
        if (elected_id == 0) {
            continue;
        }
        assert(elected_id == id);
        drop_all_except(f, RAFT_IO_REQUEST_VOTE, false, id);
        return;
    }

    assert(0);
}
