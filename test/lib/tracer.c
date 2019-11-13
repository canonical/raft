#include "tracer.h"

#include "munit.h"

void test_tracer_emit(struct raft_tracer *t,
                      raft_time time,
                      const char *file,
                      int line,
                      const char *format,
                      ...)
{
    va_list args;
    char buf[1024];
    (void)t;
    (void)time;
    va_start(args, format);
    vsnprintf(buf, sizeof buf, format, args);
    munit_logf(MUNIT_LOG_INFO, "%30s:%*d - %s", file, 3, line, buf);
    va_end(args);
}
