#ifndef RAFT_IO_SIM_H
#define RAFT_IO_SIM_H

struct raft_io;
struct raft_message;

/**
 * Configure the given @raft_io instance to use a stub in-memory I/O
 * implementation.
 */
int raft_io_stub_init(struct raft_io *io);

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

#endif /* RAFT_IO_SIM_H */
