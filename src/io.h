/* Internal I/O related APIs. */

#ifndef IO_H_
#define IO_H_

#include "../include/raft.h"

/* Increment the io_pending counter. */
void IoPendingIncrement(struct raft *r);

/* Decrement the io_pending counter, possibly invoking the close callback. */
void IoPendingDecrement(struct raft *r);

void io_close_cb(struct raft_io *io);

#endif /* IO_H_ */
