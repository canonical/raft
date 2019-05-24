#include <string.h>

#include "entry.h"
#include "assert.h"

void entryBatchesDestroy(struct raft_entry *entries, unsigned n)
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

int entryCopy(const struct raft_entry *src, struct raft_entry *dst)
{
    dst->term = src->term;
    dst->type = src->type;
    dst->buf.len = src->buf.len;
    dst->buf.base = raft_malloc(dst->buf.len);
    if (dst->buf.base == NULL) {
        return RAFT_NOMEM;
    }
    memcpy(dst->buf.base, src->buf.base, dst->buf.len);
    dst->batch = NULL;
    return 0;
}
