/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include "../include/raft.h"

/* Default no-op tracer. */
extern struct raft_tracer NoopTracer;

/* Emit a debug message with the given tracer. */
#define Tracef(TRACER, ...)                            \
    do {                                               \
        char msg[1024];                                \
        snprintf(msg, sizeof msg, __VA_ARGS__);        \
        TRACER->emit(TRACER, __FILE__, __LINE__, msg); \
    } while (0)

/* Set to 1 to enable tracing. */
#if 0
#define tracef(...) Tracef(r->tracer, __VA_ARGS__)
#else
#define tracef(...)
#endif

#endif /* TRACING_H_ */
