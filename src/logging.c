#include <string.h>
#include <time.h>

#include "logging.h"

#define EMIT_BUF_LEN 1024

static void emitToStream(FILE *stream,
                  raft_time time,
                  int level,
                  const char *format,
                  va_list args)
{
    char buf[EMIT_BUF_LEN];
    char *cursor = buf;
    int n = sizeof buf;
    struct tm tm;
    time_t secs = time / 1000;
    unsigned msecs = time % 1000;

    memset(buf, 0, sizeof buf);

    gmtime_r(&secs, &tm);

    strftime(cursor, 10, "%H:%M:%S", &tm);
    cursor = buf + strlen(buf);

    sprintf(cursor, ".%03d ", msecs);
    cursor = buf + strlen(buf);

    /* First, render the logging level. */
    switch (level) {
        case RAFT_DEBUG:
            sprintf(cursor, "[DEBUG]: ");
            break;
        case RAFT_INFO:
            sprintf(cursor, "[INFO ]: ");
            break;
        case RAFT_WARN:
            sprintf(cursor, "[WARN ]: ");
            break;
        case RAFT_ERROR:
            sprintf(cursor, "[ERROR]: ");
            break;
        default:
            sprintf(cursor, "[     ]: ");
            break;
    };

    cursor = buf + strlen(buf);

    /* Then render the message, possibly truncating it. */
    n = EMIT_BUF_LEN - strlen(buf) - 1;
    vsnprintf(cursor, n, format, args);

    fprintf(stream, "%s\n", buf);
}

static void defaultEmit(struct raft_logger *l,
                        int level,
                        raft_time time,
                        const char *format,
                        ...)
{
    va_list args;

    if (level < l->level) {
        return;
    }

    va_start(args, format);
    emitToStream(stderr, time, level, format, args);
    va_end(args);
}

int raft_default_logger_init(struct raft_logger *l)
{
    l->impl = NULL;
    l->level = RAFT_INFO;
    l->emit = defaultEmit;
    return 0;
}
