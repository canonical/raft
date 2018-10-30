#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "logger.h"

#define RAFT__DEFAULT_LOGGER_MAX_MSG_LEN 1024

static void raft__default_logger_emit(void *data,
                                      struct raft_context *ctx,
                                      int level,
                                      const char *format,
                                      ...)
{
    char buf[RAFT__DEFAULT_LOGGER_MAX_MSG_LEN];
    va_list args;
    int offset;

    (void)data;
    (void)ctx;

    switch (level) {
        case RAFT_DEBUG:
            sprintf(buf, "[DEBUG]: ");
            break;
        case RAFT_INFO:
            sprintf(buf, "[INFO ]: ");
            break;
        case RAFT_WARN:
            sprintf(buf, "[WARN ]: ");
            break;
        case RAFT_ERROR:
            sprintf(buf, "[ERROR]: ");
            break;
        default:
            sprintf(buf, "[     ]: ");
            break;
    };

    offset = strlen(buf);

    va_start(args, format);
    vsnprintf(buf + offset, RAFT__DEFAULT_LOGGER_MAX_MSG_LEN - offset, format,
              args);
    va_end(args);

    fprintf(stderr, "%s\n", buf);
}

struct raft_logger raft_default_logger = {
    NULL,
    raft__default_logger_emit,
};
