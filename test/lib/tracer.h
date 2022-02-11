/* Raft tracer that emits messages to stderr. */

#ifndef TEST_TRACER_H
#define TEST_TRACER_H

#include "../../include/raft.h"

#define FIXTURE_TRACER struct raft_tracer tracer
#define SET_UP_TRACER f->tracer.emit = TracerEmit; f->tracer.enabled = true
#define TEAR_DOWN_TRACER

void TracerEmit(struct raft_tracer *t,
                const char *file,
                int line,
                const char *message);

#endif /* TEST_TRACER_H */
