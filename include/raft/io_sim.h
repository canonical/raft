#ifndef RAFT_IO_SIM_H
#define RAFT_IO_SIM_H

struct raft;

/**
 * Use the simulated in-memory I/O implementation.
 */
int raft_io_sim_init(struct raft *r);

/**
 * Advance the simulated time by the given number of milliseconds, and invoke
 * @raft_tick instance accordingly. Return the value returned by @raft_tick.
 */
int raft_io_sim_advance(struct raft *r, unsigned msecs);

/**
 * Flush all pending I/O requests, invoking the @raft_handle_io as appropriate.
 */
void raft_io_sim_flush(struct raft *r);

#endif /* RAFT_IO_SIM_H */
