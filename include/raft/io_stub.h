#ifndef RAFT_IO_SIM_H
#define RAFT_IO_SIM_H

#include <stdbool.h>

struct raft_io;
struct raft_logger;
struct raft_message;

/**
 * Configure the given @raft_io instance to use a stub in-memory I/O
 * implementation.
 */
int raft_io_stub_init(struct raft_io *io, struct raft_logger *logger);

void raft_io_stub_close(struct raft_io *io);

/**
 * Advance the stub time by the given number of milliseconds, and invoke the
 * tick callback accordingly.
 */
void raft_io_stub_advance(struct raft_io *io, unsigned msecs);

/**
 * Dispatch a message, invoking the recv callback.
 */
void raft_io_stub_dispatch(struct raft_io *io, struct raft_message *message);

/**
 * Flush all pending I/O requests, invoking the @notify callback as appropriate.
 */
void raft_io_stub_flush(struct raft_io *io);

/**
 * Inject a failure that will be triggered after @delay I/O requests and occur
 * @repeat times.
 */
void raft_io_stub_fault(struct raft_io *io, int delay, int repeat);

/**
 * Return true if a write request is pending.
 */
bool raft_io_stub_writing(struct raft_io *io);

/**
 * Get the first message of the given @type that is currently being sent, if
 * any.
 */
struct raft_message *raft_io_stub_sending(struct raft_io *io, int type);

/**
 * Convenience for getting the current term stored in the stub.
 */
unsigned raft_io_stub_term(struct raft_io *io);

/**
 * Convenience for getting the current vote stored in the stub.
 */
unsigned raft_io_stub_vote(struct raft_io *io);

#endif /* RAFT_IO_SIM_H */
