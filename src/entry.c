#include "entry.h"
#include "assert.h"

void entry_batches__destroy(struct raft_entry *entries, unsigned n)
{
    void *batch = NULL;
    unsigned i;
    if (entries == NULL) {
        assert(n == 0);
        return;
    }
    assert(n > 0);
    for (i = 0; i < n; i++) {
        assert(entries[i].batch != NULL);
        if (entries[i].batch != batch) {
            batch = entries[i].batch;
            raft_free(batch);
        }
    }
    raft_free(entries);
}
