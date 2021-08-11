/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

/* If an env var with this name is found, tracing can be enabled */
#define LIBRAFT_TRACE "LIBRAFT_TRACE"

#include "../include/raft.h"

extern struct raft_tracer NoopTracer;

/* Default stderr tracer. */
extern struct raft_tracer StderrTracer;

/* Emit a debug message with the given tracer. */
#define Tracef(TRACER, ...)                                              \
    do {                                                                 \
        if (TRACER != NULL && TRACER->emit != NULL && TRACER->enabled) { \
            static char _msg[1024];                                      \
            snprintf(_msg, sizeof _msg, __VA_ARGS__);                    \
            TRACER->emit(TRACER, __FILE__, __LINE__, _msg);              \
        }                                                                \
    } while (0)

/* Enable the tracer if the env variable is set or disable the tracer */
void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled);

#endif /* TRACING_H_ */
