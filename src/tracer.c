#include "../include/raft.h"

int raft_tracer_init(struct raft_tracer *t, size_t size)
{
    t->entries = raft_malloc(size);

    if (t->entries == NULL) {
        return RAFT_NOMEM;
    }

    t->head = NULL;
    t->tail = NULL;

    return 0;
}

void raft_tracer_close(struct raft_tracer *t)
{
    raft_free(t->entries);
}
