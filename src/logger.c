#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "logger.h"

#define RAFT__DEFAULT_LOGGER_BUF_LEN 1024
#define RAFT__DEFAULT_LOGGER_MSG_LEN 32

static void raft__default_logger_emit(void *data,
                                      struct raft_context *ctx,
                                      int level,
                                      const char *format,
                                      ...)
{
    char buf[RAFT__DEFAULT_LOGGER_BUF_LEN];
    char *cursor = buf;
    va_list args;
    int offset;
    int n = sizeof buf;
    int i;

    (void)data;
    (void)ctx;

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
    va_start(args, format);
    vsnprintf(cursor, n, format, args);
    va_end(args);

    /* Fill the message section with blanks. */
    for (i = strlen(buf); i < offset + RAFT__DEFAULT_LOGGER_MSG_LEN; i++) {
        buf[i] = ' ';
    }

    offset = strlen(buf);
    cursor = buf + offset;

    /* Then render the context, possibly truncating it. */
    n = sizeof buf - offset;
    sprintf(cursor, " ");
    cursor++;
    n--;
    raft_context_format(cursor, n, ctx);

    fprintf(stderr, "%s\n", buf);
}

struct raft_logger raft_default_logger = {
    NULL,
    raft__default_logger_emit,
};
