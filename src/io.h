/* Internal I/O related APIs. */

#ifndef IO_H_
#define IO_H_

#include "../include/raft.h"

/* Increment the io_pending counter. */
void IoPendingIncrement(struct raft *r);

/* Decrement the io_pending counter, possibly invoking the close callback. */
void IoPendingDecrement(struct raft *r);

/* Invoked upon shutdown when all pending I/O has been completed and the close
 * callback can be invoked. */
void IoCompleted(struct raft *r);

#endif /* IO_H_ */
