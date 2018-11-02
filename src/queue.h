/**
 * Queue of pending I/O requests.
 */

#ifndef RAFT_QUEUE_H
#define RAFT_QUEUE_H

#include "../include/raft.h"

void raft_queue__close(struct raft *r);

/**
 * Add a request to the list of pending I/O requests. Return the ID of the newly
 * added request object.
 */
int raft_queue__push(struct raft *r, size_t *id);

/**
 * Fetch the request with the given ID. The ID must be a value previously
 * returned by raft_queue__push() and not yet passed to raft_queue__pop().
 */
struct raft_io_request *raft_queue__get(struct raft *r, size_t id);

/**
 * Delete an item from the list of pending I/O requests. This must called both
 * in case the request succeeded or in case it failed.
 */
void raft_queue__pop(struct raft *r, size_t id);

#endif /* RAFT_QUEUE_H */
