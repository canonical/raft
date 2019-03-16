/**
 * Test implementation of a cluster of N servers, using an in-memory test
 * network to communicate.
 *
 * Out of the N servers, there are V voting servers, with V <= N.
 *
 * A monotonic global cluster clock is used to simulate network latency and time
 * elapsed on individual servers.
 *
 * Once the cluster loop starts running, at each iteration the following steps
 * are taken:
 *
 * 1. All pending I/O requests across all servers are flushed. This simulates
 *    either a successful disk write of log entries, or a successful network
 *    write of an RPC message (e.g. writev() returns successfully). A successful
 *    network write does not mean that the receiver immediately receives the
 *    message, it just means that any buffer allocated by the sender can be
 *    released (e.g. log entries). The network assigns a random latency to each
 *    RPC message, which will get delivered to the receiver after that amount of
 *    time elapses. If the sender and the receiver are currently disconnected,
 *    the RPC message is simply dropped.
 *
 * 2. All pending RPC messages across all servers are scanned and the one with
 *    the lowest latency expiration time is picked. All servers are scanned too,
 *    and the one with the lowest timer expiration time is picked. (either
 *    election timer or heartbeat timer, depending on the server state). The two
 *    times are compared and the lower one is picked, resulting in either an RPC
 *    message being delivered to the receiver or in a server tick. The global
 *    cluster time is advanced by the latency expiration time or by the raft
 *    timer expiration time, respectively. The latency timer of each RPC message
 *    is updated accordingly and the @raft_io_stub_advance() is invoked against
 *    the raft_io instance of each server, which in turn calls @raft__tick.
 *
 * 3. The current cluster leader is detected (if any). When detecting the leader
 *    the Election Safety property is checked: no servers can be in leader state
 *    for the same term. The server in leader state with the highest term is
 *    considered the current cluster leader, as long as it's "stable", i.e. it
 *    has been acknowledged by all servers connected to it, and those servers
 *    form a majority (this means that no further leader change can happen,
 *    unless the network gets disrupted). If there is a stable leader and it has
 *    not changed with respect to the previous cluster iteration, the Leader
 *    Append-Only property is checked, by comparing its log with a copy of it
 *    that was taken during the previous iteration.
 *
 * 4. If there is a stable leader, its current log is copied, in order to be
 *    able to check the Leader Append-Only property at the next iteration.
 *
 * 5. If there is a stable leader, its commit index gets copied.
 *
 * Servers can be alive or dead. Network messages sent to dead servers are
 * dropped. Dead servers are not tick'ed in step 2.
 *
 * Any two servers can be connected or disconnected. Network messages sent
 * between disconnected servers are dropped.
 */

#ifndef TEST_CLUSTER_H
#define TEST_CLUSTER_H

#include "../../include/raft.h"

#include "fsm.h"
#include "io.h"
#include "logger.h"
#include "munit.h"

/**
 * Munit parameter defining after how many servers to run. Default is 3.
 */
#define TEST_CLUSTER_SERVERS "cluster-servers"

/**
 * Munit parameter defining after how many of the servers are voting
 * servers. Default is 3.
 */
#define TEST_CLUSTER_VOTING "cluster-voting"

struct test_cluster
{
    size_t n;                    /* N. of servers in the cluster. */
    size_t n_voting;             /* N. of voting servers (highest IDs) */
    int time;                    /* Elapsed time, in milliseconds */
    bool *alive;                 /* Whether a server is alive. */
    unsigned leader_id;          /* Current leader, if any. */
    struct raft_logger *loggers; /* Loggers. */
    struct raft_io *ios;         /* Test I/O implementations. */
    struct raft_fsm *fsms;       /* Test FSM implementations. */
    struct raft *rafts;          /* Raft instances */
    raft_index commit_index;     /* Index of last committed entry. */
    struct raft_log log;         /* Copy of leader's log at last iteration. */
};

void test_cluster_setup(const MunitParameter params[], struct test_cluster *c);
void test_cluster_tear_down(struct test_cluster *c);

/**
 * Run a single cluster loop iteration.
 */
void test_cluster_run_once(struct test_cluster *c);

/**
 * Run until the given stop condition becomes true, or max_msecs have elapsed.
 */
int test_cluster_run_until(struct test_cluster *c,
                           bool (*stop)(struct test_cluster *c),
                           int max_msecs);

/**
 * Return the server ID of the leader, or 0 if there's no leader.
 */
unsigned test_cluster_leader(struct test_cluster *c);

/**
 * Return true if the cluster has a leader.
 */
bool test_cluster_has_leader(struct test_cluster *c);

/**
 * Return true if the cluster has no leader.
 */
bool test_cluster_has_no_leader(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to accept a new entry.
 */
void test_cluster_propose(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to accept a new entry containing a
 * configuration change that adds a new non-voting server.
 */
void test_cluster_add_server(struct test_cluster *c);

/**
 * Simulate a client requesting the leader to promote the last server that was
 * added.
 */
void test_cluster_promote(struct test_cluster *c);

/**
 * Return true if at least two log entries were committed to the log.
 */
bool test_cluster_committed_2(struct test_cluster *c);

/**
 * Return true if at least three log entries were committed to the log.
 */
bool test_cluster_committed_3(struct test_cluster *c);

/**
 * Kill a server.
 */
void test_cluster_kill(struct test_cluster *c, unsigned id);

/**
 * Kill the majority of servers (excluding the current leader).
 */
void test_cluster_kill_majority(struct test_cluster *c);

/**
 * Return true if the given servers are connected.
 */
bool test_cluster_connected(struct test_cluster *c, unsigned id1, unsigned id2);

/**
 * Disconnect a server from another.
 */
void test_cluster_disconnect(struct test_cluster *c,
                             unsigned id1,
                             unsigned id2);

/**
 * Reconnect two servers.
 */
void test_cluster_reconnect(struct test_cluster *c, unsigned id1, unsigned id2);

#endif /* TEST_CLUSTER_H */
