#ifndef RAFT_ENTRY_H_
#define RAFT_ENTRY_H_

#include "../include/raft.h"

/**
 * Release all memory associated with the given entries, including the array
 * itself. The entries are supposed to belong to one or more batches.
 */
void entry_batches__destroy(struct raft_entry *entries, unsigned n);

#endif /* RAFT_ENTRY_H */

