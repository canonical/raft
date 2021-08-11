#include <stdlib.h>

#include "tracing.h"

static inline void noopTracerEmit(struct raft_tracer *t,
                                  const char *file,
                                  int line,
                                  const char *message)
{
    (void)t;
    (void)file;
    (void)line;
    (void)message;
}
struct raft_tracer NoopTracer = {.impl = NULL, .enabled = false, .emit = noopTracerEmit};


static inline void stderrTracerEmit(struct raft_tracer *t,
                                    const char *file,
                                    int line,
                                    const char *message)
{
    (void)t;
    fprintf(stderr, "LIBRAFT %s:%d %s\n", file, line, message);
}
struct raft_tracer StderrTracer = {.impl = NULL, .enabled = false, .emit = stderrTracerEmit};

void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled)
{
    if (getenv(LIBRAFT_TRACE) != NULL) {
        tracer->enabled = enabled;
    }
}
