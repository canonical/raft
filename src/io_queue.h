/**
 * Keep track of in-flight I/O request.
 */

#ifndef RAFT_IO_QUEUE_H
#define RAFT_IO_QUEUE_H

#include "../include/raft.h"

void raft_io_queue__init(struct raft_io_queue *q);
void raft_io_queue__close(struct raft_io_queue *q);

/**
 * Add a request to the list of pending I/O requests. Return the ID of the newly
 * added request object.
 */
int raft_io_queue__push(struct raft_io_queue *q, unsigned *id);

/**
 * Delete an item from the list of pending I/O requests. This must called both
 * in case the request succeeded or in case it failed.
 */
void raft_io_queue__pop(struct raft_io_queue *q, unsigned id);

int raft_io_queue__push_(struct raft *r, unsigned *id);
void raft_io_queue__pop_(struct raft *r, unsigned id);
void raft_io_queue__close_(struct raft *r);

#endif /* RAFT_IO_QUEUE_H */
