/**
 * Test implementation of the raft_io interface, with fault injection.
 */

#ifndef TEST_IO_H
#define TEST_IO_H

#include "../../include/raft.h"

#include "munit.h"
#include "network.h"

/**
 * Munit parameter defining after how many API calls the test raft_io
 * implementation should start failing and return errors. The default is -1,
 * meaning that no failure will ever occur.
 */
#define TEST_IO_FAULT_DELAY "io-fault-delay"

/**
 * Munit parameter defining how many consecutive times API calls against the
 * test raft_io implementation should keep failingo after they started
 * failing. This parameter has an effect only if 'store-fail-delay' is 0 or
 * greater. The default is 1, and -1 means "keep failing forever".
 */
#define TEST_IO_FAULT_REPEAT "io-fault-repeat"

/**
 * Maximum number of pending I/O events in the queue. This should be enough for
 * testing purposes.
 */
#define TEST_IO_REQUEST_QUEUE_SIZE 64

/**
 * Hold information about a single in-flight log write or RPC.
 */
struct test_io_request
{
    struct raft_io *io;
    int type;
    size_t id;
    union {
        struct
        {
            const struct raft_entry *entries;
            size_t n;
        } write_log;
        struct
        {
            unsigned id;
            const char *address;
            struct raft_request_vote args;
        } request_vote;
        struct
        {
            unsigned id;
            const char *address;
            struct raft_request_vote_result result;
        } request_vote_response;
        struct
        {
            unsigned id;
            const char *address;
            struct raft_append_entries args;
        } append_entries;
        struct
        {
            unsigned id;
            const char *address;
            struct raft_append_entries_result result;
        } append_entries_response;
    };
};

void test_io_setup(const MunitParameter params[],
                   struct raft_io *io,
                   struct raft_logger *logger);

void test_io_tear_down(struct raft_io *io);

/**
 * Bootstrap the on-disk raft state by writing the initial term (1) and log. The
 * log will contain a single configuration entry, whose server IDs are assigned
 * sequentially starting from 1 up to @n_servers. Only servers with IDs in the
 * range [@voting_a, @voting_b] will be voting servers.
 */
void test_io_bootstrap(struct raft_io *io,
                       const int n_servers,
                       const int voting_a,
                       const int voting_b);

/**
 * Convenience for writing term and vote together and checking for errors.
 */
void test_io_set_term_and_vote(struct raft_io *io,
                               const uint64_t term,
                               const uint64_t vote);

/**
 * Synchronously append a new entry.
 */
void test_io_append_entry(struct raft_io *io, const struct raft_entry *entry);

/**
 * Synchronously write a new entry.
 */
void test_io_write_entry(struct raft_io *io, const struct raft_entry *entry);

/**
 * Get the persisted term.
 */
uint64_t test_io_get_term(struct raft_io *io);

/**
 * Get the persisted vote.
 */
uint64_t test_io_get_vote(struct raft_io *io);

/**
 * Get the persisted log entries.
 */
void test_io_get_entries(struct raft_io *io,
                         const struct raft_entry *entries[],
                         size_t *n);

/**
 * Get all pending requests of the given type.
 */
void test_io_get_requests(struct raft_io *io,
                          int type,
                          struct test_io_request **requests,
                          size_t *n);

/**
 * Get the number of pending requests of the given type.
 */
size_t test_io_n_requests(struct raft_io *io, int type);

/**
 * Get a pending event of the given type, and assert that it's the only one with
 * that type.
 */
void test_io_get_one_request(struct raft_io *io,
                             int type,
                             struct test_io_request *request);

/**
 * Execute all pending I/O requests.
 */
void test_io_flush(struct raft_io *io);

/**
 * Schedule the next failure.
 */
void test_io_fault(struct raft_io *io, int delay, int repeat);

/**
 * Connect this test I/O instance to a test network. When network-related events
 * (e.g RPCs requests/responses) are flushed, they will be dispatched to the
 * appropriate incoming queue of the receiver.
 */
void test_io_set_network(struct raft_io *io,
                         struct test_network *network,
                         uint64_t id);

#endif /* TEST_IO_H */
