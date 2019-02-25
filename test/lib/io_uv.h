/**
 * Helpers around the libuv-based implementation of the raft_io interface.
 */

#ifndef TEST_IO_UV_H
#define TEST_IO_UV_H

#include "../../include/raft.h"

#include "munit.h"

/**
 * Create a valid snapshot metadata file with the given @term, @index and
 * @timestamp. Return the size of the created file.
 */
size_t test_io_uv_write_snapshot_meta_file(const char *dir,
                                           raft_term term,
                                           raft_index index,
                                           unsigned long long timestamp);

/**
 * Create a valid open segment file with counter @counter with @n batches each
 * containing one entry. @data should be an integer that will be used as base
 * value for the data of the firt entry, and will be incremented. Return the
 * size of the created file.
 */
size_t test_io_uv_write_open_segment_file(const char *dir,
                                          unsigned long long counter,
                                          int n,
                                          int data);

/**
 * Create a valid closed segment file with first index @first_index and @n
 * batches each containing one entry. @data should be an integer that will be
 * used as base value for the data of the firt entry, and will be incremented.
 * Return the size of the created file.
 */
size_t test_io_uv_write_closed_segment_file(const char *dir,
                                            raft_index first_index,
                                            int n,
                                            int data);

#endif /* TEST_IO_UV_H */
