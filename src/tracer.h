#include "../include/raft.h"

typedef void (*tracerWalkCb)(void *data,
                             raft_time time,
                             unsigned type,
                             const char *message);

/* Add a record to the tracer circular buffer. */
void tracerEmit(struct raft_tracer *t,
                raft_time time,
                unsigned type,
                const char *format,
                va_list args);

/* Iterate through all entries, calling the given hook each time. */
void tracerWalk(const struct raft_tracer *t, tracerWalkCb cb, void *data);
