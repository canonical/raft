/* Tracing functions and helpers. */

#ifndef TRACING_H_
#define TRACING_H_

#include "../include/raft.h"

/* Default no-op tracer. */
extern struct raft_tracer NoopTracer;

/* Emit a debug message with the given tracer. */
#define Tracef(TRACER, ...) \
    TRACER->emit(TRACER, __FILE__, __LINE__, ##__VA_ARGS__)

#endif /* TRACING_H_ */
