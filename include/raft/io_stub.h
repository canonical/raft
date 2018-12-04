#ifndef RAFT_IO_SIM_H
#define RAFT_IO_SIM_H

struct raft_io;

/**
 * Configure the given @raft_io to use a stub in-memory I/O implementation.
 */
int raft_io_stub_init(struct raft_io *io);

/**
 * Advance the stub time by the given number of milliseconds, and invoke the
 * tick function accordingly.
 */
void raft_io_stub_advance(struct raft_io *io, unsigned msecs);

/**
 * Flush all pending I/O requests, invoking the @raft_handle_io as appropriate.
 */
void raft_io_stub_flush(struct raft_io *io);

#endif /* RAFT_IO_SIM_H */
