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
        char address[8];
        struct raft_io io;
        struct raft raft;
    } * servers;
};

/**
 * Setup a raft cluster fixture with @n servers, each one using an in-memory
 * @raft_io implementation and one of the given @fsms.
 */
int raft_fixture_setup(struct raft_fixture *f,
                       unsigned n,
                       struct raft_fsm *fsms);

/**
 * Release any memory used by the given raft cluster fixture.
 */
void raft_fixture_tear_down(struct raft_fixture *f);

#endif /* RAFT_FAKE_H */
