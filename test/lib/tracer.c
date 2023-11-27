#include "tracer.h"

#include "munit.h"

void TracerEmit(struct raft_tracer *t,
                const char *file,
                unsigned int line,
                const char *func,
                unsigned int level,
                const char *message)
{
    (void)t;
    (void)level;
    (void)func;
    fprintf(stderr, "%20s:%*d - %s\n", file, 3, line, message);
}
