/**
 * Logging functions and helpers.
 */

#ifndef RAFT_LOGGING_H_
#define RAFT_LOGGING_H_

#include <stdio.h>

#include "../include/raft.h"

/**
 * Emit a message with level #RAFT_DEBUG
 */
void debugf(struct raft_io *io, const char *format, ...);

/**
 * Emit a message with level #RAFT_INFO
 */
void infof(struct raft_io *io, const char *format, ...);

/**
 * Emit a message with level #RAFT_WARN
 */
void warnf(struct raft_io *io, const char *format, ...);

/**
 * Emit a message with level #RAFT_ERROR
 */
void errorf(struct raft_io *io, const char *format, ...);

/**
 * Emit a message to the given stream.
 */
void emit_to_stream(FILE *stream,
                    unsigned server_id,
                    raft_time time,
                    int level,
                    const char *format,
                    va_list args);

#endif /* RAFT_LOGGING__H */
