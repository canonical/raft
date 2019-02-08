#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "../include/raft.h"

#include "assert.h"

#define RAFT__DEFAULT_LOGGER_BUF_LEN 1024

struct raft_default_logger_options
{
    unsigned server_id; /* Server ID to include in emitted messages */
    int level;          /* Minum logging level */
};

struct raft_default_logger_options raft_default_logger_options = {
    0,
    RAFT_WARN,
};

static void raft__default_logger_emit(void *data,
                                      int level,
                                      const char *format,
                                      va_list args)
{
    struct raft_default_logger_options *options;
    char buf[RAFT__DEFAULT_LOGGER_BUF_LEN];
    char *cursor = buf;
    int offset = 0;
    int n = sizeof buf;

    assert(data != NULL);

    options = data;

    if (level < options->level) {
        return;
    }

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

    offset += strlen(cursor);
    cursor = buf + offset;

    if (options->server_id != 0) {
        sprintf(cursor, "%d -> ", options->server_id);
        offset += strlen(cursor);
        cursor = buf + offset;
    }

    /* Then render the message, possibly truncating it. */
    n = RAFT__DEFAULT_LOGGER_BUF_LEN - offset - 1;
    vsnprintf(cursor, n, format, args);

    fprintf(stderr, "%s\n", buf);
}

struct raft_logger raft_default_logger = {
    &raft_default_logger_options,
    raft__default_logger_emit,
};

void raft_default_logger_set_server_id(unsigned server_id)
{
    raft_default_logger_options.server_id = server_id;
}

void raft_default_logger_set_level(int level)
{
    raft_default_logger_options.level = level;
}

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
