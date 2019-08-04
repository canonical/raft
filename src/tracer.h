#include "../include/raft.h"

/* Add a record to the tracer circular buffer. */
void tracerEmit(struct raft_tracer *t,
                raft_time time,
                unsigned type,
                const char *format,
                va_list args);
