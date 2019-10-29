/* Utilities around error handling. */

#ifndef ERROR_H_
#define ERROR_H_

#include <stddef.h>

/* Format an error message. This is basically just a version of asprintf that
 * uses raft_malloc(). */
char *errMsgPrintf(const char *format, ...);

/* Wrap the given error message with an additional prefix message. The wrapped
 * message will be freed. */
char *errMsgWrapf(char *e, const char *format, ...);

/* Convert a numeric raft error code to a human-readable error message. */
const char *errCodeToString(int errnum);

#endif /* ERROR_H_ */
