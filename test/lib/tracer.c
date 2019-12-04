#include "tracer.h"

#include "munit.h"

void test_tracer_emit(struct raft_tracer *t,
                      const char *file,
                      int line,
                      const char *format,
                      ...)
{
    va_list args;
    char buf[1024];
    (void)t;
    va_start(args, format);
    vsnprintf(buf, sizeof buf, format, args);
    fprintf(stderr, "%20s:%*d - %s\n", file, 3, line, buf);
    va_end(args);
}
