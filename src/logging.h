/* Logging functions and helpers. */

#ifndef LOGGING_H_
#define LOGGING_H_

#include <stdio.h>

#include "../include/raft.h"

/* Emit a log message with a certain level. */
#define debugf(IO, FORMAT, ...) IO->emit(IO, RAFT_DEBUG, FORMAT, ##__VA_ARGS__);
#define infof(IO, FORMAT, ...) IO->emit(IO, RAFT_INFO, FORMAT, ##__VA_ARGS__);
#define warnf(IO, FORMAT, ...) IO->emit(IO, RAFT_WARN, FORMAT, ##__VA_ARGS__);
#define errorf(IO, FORMAT, ...) IO->emit(IO, RAFT_ERROR, FORMAT, ##__VA_ARGS__);

/* Emit a message to the given stream. */
void emitToStream(FILE *stream,
                  unsigned server_id,
                  raft_time time,
                  int level,
                  const char *format,
                  va_list args);

#endif /* LOGGING_H_ */
