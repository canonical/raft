/* Logging functions and helpers. */

#ifndef LOGGING_H_
#define LOGGING_H_

#include <stdio.h>

#include "../include/raft.h"

/* Emit a log message */
#define emitf(R, LEVEL, FORMAT, ...)                              \
    R->logger->emit(R->logger, LEVEL, R->io->time(R->io), FORMAT, \
                    ##__VA_ARGS__);

/* Emit a log message with a certain level. */
#define debugf(R, FORMAT, ...) emitf(R, RAFT_DEBUG, FORMAT, ##__VA_ARGS__);
#define infof(R, FORMAT, ...) emitf(R, RAFT_INFO, FORMAT, ##__VA_ARGS__);
#define warnf(R, FORMAT, ...) emitf(R, RAFT_WARN, FORMAT, ##__VA_ARGS__);
#define errorf(R, FORMAT, ...) emitf(R, RAFT_ERROR, FORMAT, ##__VA_ARGS__);

#endif /* LOGGING_H_ */
