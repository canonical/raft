#ifndef ENTRY_H_
#define ENTRY_H_

#include "../include/raft.h"

/* Release all memory associated with the given entries, including the array
 * itself. The entries are supposed to belong to one or more batches. */
void entryBatchesDestroy(struct raft_entry *entries, unsigned n);

#endif /* ENTRY_H */
