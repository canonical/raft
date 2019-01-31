/**
 * Logging helpers.
 */

#ifndef RAFT_LOGGER_H
#define RAFT_LOGGER_H

#include "../include/raft.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#endif

#define raft_infof(L, FORMAT, ...) \
    L->emit(L->data, RAFT_INFO, FORMAT, ##__VA_ARGS__)

#define raft_warnf(L, FORMAT, ...) \
    L->emit(L->data, RAFT_WARN, FORMAT, ##__VA_ARGS__)

#define raft_errorf_(L, FORMAT, ...) \
    L->emit(L->data, RAFT_ERROR, FORMAT, ##__VA_ARGS__)

/* For performance, debug level is a no-op on debug builds */
#if RAFT_ENABLE_DEBUG_LOGGING
#define raft__debugf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, RAFT_DEBUG, FORMAT, ##__VA_ARGS__)
#else
#define raft__debugf(P, FORMAT, ...)
#endif /* RAFT_ENABLE_DEBUG_LOGGING */

#define raft__infof(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, RAFT_INFO, FORMAT, ##__VA_ARGS__)

#define raft__warnf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, RAFT_WARN, FORMAT, ##__VA_ARGS__)

#define raft__errorf(P, FORMAT, ...) \
    P->logger.emit(P->logger.data, RAFT_ERROR, FORMAT, ##__VA_ARGS__)

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif /* RAFT_LOGGER_H */
