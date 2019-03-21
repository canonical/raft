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

/**
 * Test implementation of a cluster of N servers, each having a user-provided
 * FSM.
 *
 * Out of the N servers, there can be V voting servers, with V <= N.
 *
 * The cluster can simulate network latency and time elapsed on individual
 * servers.
 *
 * Servers can be alive or dead. Network messages sent to dead servers are
 * dropped. Dead servers do not have their @raft_io_tick_cb callback invoked.
 *
 * Any two servers can be connected or disconnected. Network messages sent
 * between disconnected servers are dropped.
 */
struct raft_fixture
{
    raft_time time;          /* Number of milliseconds elapsed. */
    unsigned n;              /* Number of servers */
    unsigned leader_id;      /* ID of current leader, or 0 */
    struct raft_log log;     /* Copy of leader's log */
    raft_index commit_index; /* Current commit index on leader */
    struct raft_fixture_server servers[RAFT_FIXTURE_MAX_SERVERS];
    int (*random)(int, int);
};

/**
 * Initialize a raft cluster fixture with @n servers. Each server will use an
 * in-memory @raft_io implementation and one of the given @fsms. All servers
 * will be initially connected to one another, but they won't be bootstrapped or
 * started.
 */
int raft_fixture_init(struct raft_fixture *f,
                      unsigned n,
                      struct raft_fsm *fsms);

/**
 * Release all memory used by the fixture.
 */
void raft_fixture_close(struct raft_fixture *f);

/**
 * Bootstrap the initial configuration of each server in the cluster. The first
 * @n_voting servers will be voting. There must be at least one voting server.
 */
int raft_fixture_bootstrap(struct raft_fixture *f, unsigned n_voting);

/**
 * Start all servers in the fixture.
 */
int raft_fixture_start(struct raft_fixture *f);

/**
 * Return the current number of servers in the fixture.
 */
unsigned raft_fixture_n(struct raft_fixture *f);

/**
 * Return the raft instance associated with the i'th server of the fixture.
 */
struct raft *raft_fixture_get(struct raft_fixture *f, unsigned i);

/**
 * Return @true if the i'th server hasn't been killed.
 */
bool raft_fixture_alive(struct raft_fixture *f, unsigned i);

/**
 * Drive the cluster so the i'th server gets elected as leader.
 *
 * This is achieved by resetting the election timeout of all other servers to a
 * very high value, letting the one of the i'th server expire and then advancing
 * the cluster until the election is won.
 *
 * There must currently be no leader and no candidate and the given server must
 * be a voting one.
 */
void raft_fixture_elect(struct raft_fixture *f, unsigned i);

/**
 * Drive the cluster so the current leader gets deposed.
 *
 * This is achieved by dropping all AppendEntries result messages sent by
 * followers to the leader, until the leader decides to step down.
 */
void raft_fixture_depose(struct raft_fixture *f);

/**
 * Wait until all servers have applied the entry at the given index. Abort if
 * @max_msecs elapse without that happening.
 */
void raft_fixture_wait_applied(struct raft_fixture *f,
                               raft_index index,
                               unsigned max_msecs);

/**
 * Step through the cluster state advancing the time to the minimum value needed
 * for it to make progress (i.e. for a message to be delivered or for a server
 * time out).
 *
 * In particular, the following happens:
 *
 * 1. All pending I/O requests across all servers are flushed. This simulates
 *    completion of disk writes (@raft_io_append, @raft_io_snapshot_put, etc),
 *    and completion RPC @raft_io_send requests. A completed network request
 *    does not mean that the receiver immediately receives the message, it just
 *    means that any buffer allocated by the sender can be released (e.g. log
 *    entries). The in-memory I/O implementation assigns a random latency to
 *    each RPC message, which will get delivered to the receiver only after that
 *    amount of time elapses. If the sender and the receiver are currently
 *    disconnected, the RPC message is simply dropped.
 *
 * 2. All pending RPC messages across all servers are scanned and the one with
 *    the lowest delivery time is picked. All servers are scanned too, and the
 *    one with the lowest timer expiration time is picked (that will be either
 *    election timer or heartbeat timer, depending on the server state). The two
 *    times are compared and the lower one is picked. If there's an RPC to be
 *    delivered, the receiver's @raft_io_recv_cb callback gets fired. Then the
 *    @raft_io_tick_cb callback of all servers is invoked, with amount of time
 *    elapsed. The timer of each remaining RPC message is updated accordingly.
 *
 * 3. The current cluster leader is detected (if any). When detecting the leader
 *    the Election Safety property is checked: no servers can be in leader state
 *    for the same term. The server in leader state with the highest term is
 *    considered the current cluster leader, as long as it's "stable", i.e. it
 *    has been acknowledged by all servers connected to it, and those servers
 *    form a majority (this means that no further leader change can happen,
 *    unless the network gets disrupted). If there is a stable leader and it has
 *    not changed with respect to the previous call to @raft_fixture_step(),
 *    then the Leader Append-Only property is checked, by comparing its log with
 *    a copy of it that was taken during the previous iteration.
 *
 * 4. If there is a stable leader, its current log is copied, in order to be
 *    able to check the Leader Append-Only property at the next call.
 *
 * 5. If there is a stable leader, its commit index gets copied.
 *
 */
void raft_fixture_step(struct raft_fixture *f);

/**
 * Step the cluster until the given @stop function returns #true, or @max_msecs
 * have elapsed.
 *
 * Return #true if the @stop function has returned #true within @max_msecs.
 */
bool raft_fixture_step_until(struct raft_fixture *f,
                             bool (*stop)(struct raft_fixture *f, void *arg),
                             void *arg,
                             unsigned max_msecs);

/**
 * Return true if the servers with the given indexes are connected.
 */
bool raft_fixture_connected(struct raft_fixture *f, unsigned i, unsigned j);

/**
 * Disconnect the servers with the given indexes from one another.
 */
void raft_fixture_disconnect(struct raft_fixture *f, unsigned i, unsigned j);

/**
 * Disconnect the server with given index from all the others.
 */
void raft_fixture_disconnect_from_all(struct raft_fixture *f, unsigned i);

/**
 * Reconnect the servers with given indexes to one another.
 */
void raft_fixture_reconnect(struct raft_fixture *f, unsigned i, unsigned j);

/**
 * Reconnect the server with the given index to all other servers.
 */
void raft_fixture_reconnect_to_all(struct raft_fixture *f, unsigned i);

/**
 * Kill the server with the given index. The server won't receive any message
 * and its tick callback won't be invoked.
 */
void raft_fixture_kill(struct raft_fixture *f, unsigned i);

/**
 * Add a new empty server to the cluster and connect it to all others.
 */
int raft_fixture_add_server(struct raft_fixture *f, struct raft_fsm *fsm);

/**
 * Set the network latency in milliseconds. Each RPC message will be assigned a
 * random latency value within the given range.
 */
void raft_fixture_set_latency(struct raft_fixture *f,
                              unsigned min,
                              unsigned max);

/**
 * Set the function to use to to generate random values within a range. The raft
 * servers will use it to generate a random election timeout, and the fixture
 * itself will use it to assign a random network latency to RPC message.
 */
void raft_fixture_set_random(struct raft_fixture *f, int (*random)(int, int));

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

#endif /* RAFT_FAKE_H */
