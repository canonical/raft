#include "tracing.h"

/* No-op trace emit function. */
static inline void noopTracerEmit(struct raft_tracer *t,
                                  raft_time time,
                                  const char *file,
                                  int line,
                                  const char *format,
                                  ...)
{
    (void)t;
    (void)time;
    (void)file;
    (void)line;
    (void)format;
}

/* Default no-op tracer. */
struct raft_tracer NoopTracer = {.impl = NULL, .emit = noopTracerEmit};
