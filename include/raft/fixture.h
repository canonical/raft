/**
 * Raft cluster test fixture, using an in-memory @raft_io implementation. This
 * is meant to be used in unit tests.
 */

#ifndef RAFT_FIXTURE_H
#define RAFT_FIXTURE_H

#include "../raft.h"

struct raft_fixture
{
    unsigned n;
    struct raft_fixture_server
    {
        unsigned id;
        char address[8];
        struct raft_io io;
        struct raft raft;
    } * servers;
};

/**
 * Setup a raft cluster fixture with @n servers, the first @n_voting of which
 * are voting servers. Each server will use an in-memory @raft_io implementation
 * and one of the given @fsms. The @random function can be used in order to be
 * able to reproduce particular test runs.
 */
int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       unsigned n_voting,
                       struct raft_fsm *fsms,
                       int (*random)(int, int));

/**
 * Release any memory used by the given raft cluster fixture.
 */
void raft_fixture_tear_down(struct raft_fixture *f);

void raft_fixture_step(struct raft_fixture *f);

#endif /* RAFT_FAKE_H */
