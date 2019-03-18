#include <string.h>
#include <time.h>

#include "logging.h"

#define EMIT_BUF_LEN 1024

void debugf(struct raft_io *io, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    io->emit(io, RAFT_DEBUG, format, args);
    va_end(args);
}

void infof(struct raft_io *io, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    io->emit(io, RAFT_INFO, format, args);
    va_end(args);
}

void warnf(struct raft_io *io, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    io->emit(io, RAFT_WARN, format, args);
    va_end(args);
}

void errorf(struct raft_io *io, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    io->emit(io, RAFT_ERROR, format, args);
    va_end(args);
}

void emit_to_stream(FILE *stream,
                    unsigned server_id,
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

    sprintf(cursor, "%d -> ", server_id);
    cursor = buf + strlen(buf);

    /* Then render the message, possibly truncating it. */
    n = EMIT_BUF_LEN - strlen(buf) - 1;
    vsnprintf(cursor, n, format, args);

    fprintf(stream, "%s\n", buf);
}
