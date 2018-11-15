#ifndef RAFT_IO_SIM_H
#define RAFT_IO_SIM_H

struct raft;

/**
 * Use the simulated in-memory I/O implementation.
 */
int raft_io_sim_init(struct raft *r);

/**
 * Execute all pending I/O requests.
 */
void raft_io_sim_flush(struct raft *r);

#endif /* RAFT_IO_SIM_H */
