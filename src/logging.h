/* Logging functions and helpers. */

#ifndef LOGGING_H_
#define LOGGING_H_

#include <stdio.h>

#include "../include/raft.h"

/* Emit a log message */
#define emitf(LEVEL, R, ...)                                                  \
    R->logger->emit(R->logger, LEVEL, R->io->time(R->io), __FILE__, __LINE__, \
                    ##__VA_ARGS__);

/* Emit a log message with a certain level. */
#define debugf(R, ...) emitf(RAFT_DEBUG, R, ##__VA_ARGS__);
#define infof(R, ...) emitf(RAFT_INFO, R, ##__VA_ARGS__);
#define warnf(R, ...) emitf(RAFT_WARN, R, ##__VA_ARGS__);
#define errorf(R, ...) emitf(RAFT_ERROR, R, ##__VA_ARGS__);

#endif /* LOGGING_H_ */
