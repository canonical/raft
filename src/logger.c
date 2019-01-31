#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "../include/raft.h"

#define RAFT__DEFAULT_LOGGER_BUF_LEN 1024
#define RAFT__DEFAULT_LOGGER_MSG_LEN 256

static void raft__default_logger_emit(void *data,
                                      int level,
                                      const char *format,
                                      va_list args)
{
    char buf[RAFT__DEFAULT_LOGGER_BUF_LEN];
    char *cursor = buf;
    int offset;
    int n = sizeof buf;
    int i;

    (void)data;

    memset(buf, 0, sizeof buf);

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

    offset = strlen(buf);
    cursor = buf + offset;
    n = RAFT__DEFAULT_LOGGER_MSG_LEN;

    /* Then render the message, possibly truncating it. */
    vsnprintf(cursor, n, format, args);

    fprintf(stderr, "%s\n", buf);
    return;

    /* Fill the message section with blanks. */
    for (i = strlen(buf); i < offset + RAFT__DEFAULT_LOGGER_MSG_LEN; i++) {
        buf[i] = ' ';
    }

    offset = strlen(buf);
    cursor = buf + offset;

    /* Then render the context, possibly truncating it. */
    /* n = sizeof buf - offset; */
    /* sprintf(cursor, " "); */
    /* cursor++; */
    /* n--; */
    /* raft_context_format(cursor, n, ctx); */

    fprintf(stderr, "%s\n", buf);
}

struct raft_logger raft_default_logger = {
    NULL,
    raft__default_logger_emit,
};

void raft_debugf(struct raft_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, RAFT_DEBUG, format, args);
    va_end(args);
}

void raft_infof(struct raft_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, RAFT_INFO, format, args);
    va_end(args);
}

void raft_warnf(struct raft_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, RAFT_WARN, format, args);
    va_end(args);
}

void raft_errorf(struct raft_logger *logger, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    logger->emit(logger->data, RAFT_ERROR, format, args);
    va_end(args);
}
