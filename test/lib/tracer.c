#include "tracer.h"

#include "munit.h"

void test_tracer_emit(struct raft_tracer *t,
                      const char *file,
                      int line,
                      const char *message)
{
    (void)t;
    fprintf(stderr, "%20s:%*d - %s\n", file, 3, line, message);
}
