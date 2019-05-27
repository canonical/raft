#ifndef ENTRY_H_
#define ENTRY_H_

#include "../include/raft.h"

/* Release all memory associated with the given entries, including the array
 * itself. The entries are supposed to belong to one or more batches. */
void entryBatchesDestroy(struct raft_entry *entries, unsigned n);

/* Create a copy of a log entry, including its data. */
int entryCopy(const struct raft_entry *src, struct raft_entry *dst);

/* Create a single batch of entries containing a copy of the given entries,
 * including their data. */
int entryBatchCopy(const struct raft_entry *src,
                   struct raft_entry **dst,
                   unsigned n);

#endif /* ENTRY_H */
