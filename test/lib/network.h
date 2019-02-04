/**
 * Test implementation of an in-memory network for dispatching raft RPCs, with
 * support for dropping and delaying messages.
 */

#ifndef TEST_NETWORK_H
#define TEST_NETWORK_H

#include "../../include/raft.h"

#include "munit.h"

/**
 * Maximum number of pending network messages in the incoming queue of a test
 * host. This should be enough for testing purposes.
 */
#define TEST_NETWORK_INCOMING_QUEUE_SIZE 256

/**
 * Munit parameter defining the minimum one-way latency of the network, in
 * milliseconds. Default is 5.
 */
#define TEST_NETWORK_MIN_LATENCY "network-min-latency"

/**
 * Munit parameter defining the maximum one-way latency of the network, in
 * milliseconds. Default is 50.
 */
#define TEST_NETWORK_MAX_LATENCY "network-max-latency"

struct test_message
{
    unsigned sender_id;          /* Origin server. */
    struct raft_message message; /* Message to deliver */
    int timer;                   /* Deliver after this n of msecs. */
};

struct test_network;

struct test_host
{
    struct raft *raft;
    struct test_network *network;
    struct test_message incoming[TEST_NETWORK_INCOMING_QUEUE_SIZE];
};

struct test_network
{
    size_t n;
    struct test_host *hosts;
    unsigned min_latency; /* One-way, in milliseconds */
    unsigned max_latency; /* One-way, in milliseconds */
    bool *connectivity;   /* Connectivity matrix (n by n). */
};

void test_network_setup(const MunitParameter params[],
                        struct test_network *n,
                        size_t n_hosts);
void test_network_tear_down(struct test_network *n);

/**
 * Add a new host to the network.
 */
void test_network_add_host(struct test_network *n);

/**
 * Return the host object associated with the given server ID.
 */
struct test_host *test_network_host(struct test_network *n, unsigned id);

/**
 * Add a message to the incoming queue of the given host.
 */
void test_host_enqueue(struct test_host *h, struct test_message *m);

void test_network_close_message(struct test_message *m);

/**
 * Return the next enqueued message that should be delivered to the host,
 * according to its latency timer.
 */
struct test_message *test_host_peek(struct test_host *h);

#endif /* TEST_NETWORK_H */
