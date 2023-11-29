/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

/* If an env var with this name is found, tracing can be enabled */
#define LIBRAFT_TRACE "LIBRAFT_TRACE"

#include "../include/raft.h"
#include "utils.h"

extern struct raft_tracer NoopTracer;

/* Default stderr tracer. */
extern struct raft_tracer StderrTracer;

/* Emit a debug message with the given tracer. */
#define Tracef0(TRACER, LEVEL, ...)                                            \
    do {                                                                       \
        if (UNLIKELY(TRACER != NULL && TRACER->emit != NULL &&                 \
                     TRACER->enabled)) {                                       \
            char _msg[1024];                                                   \
            snprintf(_msg, sizeof _msg, __VA_ARGS__);                          \
            TRACER->emit(TRACER, __FILE__, __LINE__, __func__, (LEVEL), _msg); \
        }                                                                      \
    } while (0)

enum raft_trace_level {
    /** Represents an invalid trace level */
    TRACE_NONE,
    /** Lower-level information to debug and analyse incorrect behavior */
    TRACE_DEBUG,
    /** Information about current system's state */
    TRACE_INFO,
    /**
     * Condition which requires a special handling, something which doesn't
     * happen normally
     */
    TRACE_WARN,
    /** Resource unavailable, no connectivity, invalid value, etc. */
    TRACE_ERROR,
    /** System is not able to continue performing its basic function */
    TRACE_FATAL,
    TRACE_NR,
};

#define Tracef(TRACER, ...) Tracef0(TRACER, TRACE_DEBUG, __VA_ARGS__)

/* Enable the tracer if the env variable is set or disable the tracer */
void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled);

#endif /* TRACING_H_ */
