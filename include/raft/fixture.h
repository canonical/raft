/**
 * Raft cluster test fixture, using an in-memory @raft_io implementation. This
 * is meant to be used in unit tests.
 */

#ifndef RAFT_FIXTURE_H
#define RAFT_FIXTURE_H

#include "../raft.h"

#define RAFT_FIXTURE_MAX_SERVERS 16

struct raft_fixture_server
{
    bool alive;
    unsigned id;
    char address[8];
    struct raft_io io;
    struct raft raft;
};

struct raft_fixture
{
    raft_time time; /* Number of milliseconds elapsed. */
    unsigned n;
    struct raft_fixture_server servers[RAFT_FIXTURE_MAX_SERVERS];
    int (*random)(int, int);
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

/**
 * Step through the cluster state advancing the time to the minimum value needed
 * for it to make progress (i.e. for a message to be delivered or for a server
 * time out).
 */
void raft_fixture_step(struct raft_fixture *f);

/**
 * Return the current number of servers in the fixture.
 */
unsigned raft_fixture_n(struct raft_fixture *f);

/**
 * Return the raft instance associated with the i'th server of the fixture.
 */
struct raft * raft_fixture_get(struct raft_fixture *f, unsigned i);

/**
 * Return @true if the i'th server hasn't been killed.
 */
bool raft_fixture_alive(struct raft_fixture *f, unsigned i);

/**
 * Drive the cluster so the server with the given @id gets elected as
 * leader. There must currently be no leader. This is achieved by dropping all
 * RequestVote messages sent by other servers.
 */
void raft_fixture_elect(struct raft_fixture *f, unsigned id);

/**
 * Drive the cluster so the current leader gets deposed. This is achieved by
 * dropping all AppendEntries result messages sent by followers to the leader.
 */
void raft_fixture_depose(struct raft_fixture *f);

/**
 * Return true if the given servers are connected.
 */
bool raft_fixture_connected(struct raft_fixture *f, unsigned id1, unsigned id2);

/**
 * Disconnect the two given servers from one another.
 */
void raft_fixture_disconnect(struct raft_fixture *f,
                             unsigned id1,
                             unsigned id2);

/**
 * Disconnect the given server from all the others.
 */
void raft_fixture_disconnect_from_all(struct raft_fixture *f, unsigned id);

/**
 * Reconnect the two given servers to one another.
 */
void raft_fixture_reconnect(struct raft_fixture *f, unsigned id1, unsigned id2);

/**
 * Reconnect the given server to all the others.
 */
void raft_fixture_reconnect_to_all(struct raft_fixture *f, unsigned id);

/**
 * Kill the server with the given ID. The server won't receive any message and
 * its tick callback won't be invoked.
 */
void raft_fixture_kill(struct raft_fixture *f, unsigned id);

/**
 * Add a new empty server to the cluster and connect it to all others.
 */
int raft_fixture_add_server(struct raft_fixture *f, struct raft_fsm *fsm);

#endif /* RAFT_FAKE_H */
