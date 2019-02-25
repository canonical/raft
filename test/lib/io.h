/**
 * Helpers around the stub implementation of the raft_io interface.
 */

#ifndef TEST_IO_H
#define TEST_IO_H

#include "../../include/raft.h"

#include "munit.h"

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

#endif /* TEST_IO_H */
