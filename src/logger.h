/**
 *
 * Logging helpers.
 *
 */

#ifndef RAFT_LOGGER_H
#define RAFT_LOGGER_H

#include "../include/raft.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#endif

/* For performance, debug level is a no-op on debug builds */
#ifdef RAFT_ENABLE_DEBUG_LOGGING
#define raft__debugf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, &(P)->ctx, RAFT_DEBUG, FORMAT, ##__VA_ARGS__)
#else
#define raft__debugf(P, FORMAT, ...)
#endif /* RAFT_ENABLE_DEBUG_LOGGING */

#define raft__infof(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, &(P)->ctx, RAFT_INFO, FORMAT, ##__VA_ARGS__)

#define raft__warnf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, &(P)->ctx, RAFT_WARN, FORMAT, ##__VA_ARGS__)

#define raft__errorf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, &(P)->ctx, RAFT_ERROR, FORMAT, ##__VA_ARGS__)

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif /* RAFT_LOGGER_H */
