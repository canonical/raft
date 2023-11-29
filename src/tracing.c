#include <inttypes.h>
#include <stdlib.h>
#include <string.h>      /* strstr, strlen */
#include <sys/syscall.h> /* syscall */
#include <time.h>
#include <unistd.h> /* syscall, getpid */

#include "assert.h" /* assert */
#include "tracing.h"

static inline void noopTracerEmit(struct raft_tracer *t UNUSED,
                                  const char *file UNUSED,
                                  unsigned int line UNUSED,
                                  const char *func UNUSED,
                                  unsigned int level UNUSED,
                                  const char *message UNUSED)
{
}
struct raft_tracer NoopTracer = {.impl = NULL,
                                 .enabled = false,
                                 .emit = noopTracerEmit};

static inline const char *tracerShortFileName(const char *fname)
{
    static const char top_src_dir[] = "raft/";
    const char *p;

    p = strstr(fname, top_src_dir);
    return p != NULL ? p + strlen(top_src_dir) : fname;
}

static inline const char *tracerTraceLevelName(unsigned int level)
{
    static const char *levels[] = {
        "NONE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL",
    };

    return level < ARRAY_SIZE(levels) ? levels[level] : levels[0];
}

static pid_t tracerPidCached;

/* NOTE: on i386 and other platforms there're no specifically imported gettid()
   functions in unistd.h
*/
static inline pid_t gettidImpl(void)
{
    return (pid_t)syscall(SYS_gettid);
}

static inline void tracerEmit(const char *file,
                              unsigned int line,
                              const char *func,
                              unsigned int level,
                              const char *message)
{
    struct timespec ts = {0};
    struct tm tm;
    pid_t tid = gettidImpl();

    clock_gettime(CLOCK_REALTIME, &ts);
    gmtime_r(&ts.tv_sec, &tm);

    /*
      Example:
      LIBRAFT[182942] 2023-11-27T14:46:24.912050507 001132 INFO
      uvClientSend  src/uv_send.c:218 connection available...
    */
    fprintf(stderr,
            "LIBRAFT[%6.6u] %04d-%02d-%02dT%02d:%02d:%02d.%09lu "
            "%6.6u %-7s %-20s %s:%-3i %s\n",
            tracerPidCached,

            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
            tm.tm_sec, ts.tv_nsec,

            (unsigned)tid, tracerTraceLevelName(level), func,
            tracerShortFileName(file), line, message);
}

static inline void stderrTracerEmit(struct raft_tracer *t,
                                    const char *file,
                                    unsigned int line,
                                    const char *func,
                                    unsigned int level,
                                    const char *message)
{
    assert(t->level < TRACE_NR);

    if (level >= t->level)
        tracerEmit(file, line, func, level, message);
}

struct raft_tracer StderrTracer = {.impl = NULL,
                                   .enabled = false,
                                   .emit = stderrTracerEmit};

void raft_tracer_maybe_enable(struct raft_tracer *tracer, bool enabled)
{
    const char *trace_level = getenv(LIBRAFT_TRACE);

    if (trace_level != NULL) {
        tracerPidCached = getpid();
        tracer->enabled = enabled;

        tracer->level = (unsigned)atoi(trace_level);
        tracer->level = tracer->level < TRACE_NR ? tracer->level : TRACE_NONE;
    }
}
