/**
 * I/O related logic.
 */

#ifndef RAFT_IO_H
#define RAFT_IO_H

#include "../include/raft.h"

void raft_io__queue_close(struct raft *r);

/**
 * Add a request to the list of pending I/O requests. Return the ID of the newly
 * added request object.
 */
int raft_io__queue_push(struct raft *r, size_t *id);

/**
 * Fetch the request with the given ID. The ID must be a value previously
 * returned by raft_io__queue_push() and not yet passed to raft_io__queue_pop().
 */
struct raft_io_request *raft_io__queue_get(struct raft *r, size_t id);

/**
 * Delete an item from the list of pending I/O requests. This must called both
 * in case the request succeeded or in case it failed.
 */
void raft_io__queue_pop(struct raft *r, size_t id);

#endif /* RAFT_IO_H */
