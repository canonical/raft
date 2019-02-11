/**
 * Allocate new open segments ahead of time so they can be used immediately by
 * the writer once the current open segment becomes full.
 */

#ifndef RAFT_IO_UV_STORE_PREPARER_H_
#define RAFT_IO_UV_STORE_PREPARER_H_

#endif /* RAFT_IO_UV_STORE_PREPARER_H */

/**
 * Number of open segments that the preparer will try to maintain and keep ready
 * for writing.
 */
#define RAFT__IO_UV_STORE_PREPARER_N 3
