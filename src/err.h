/* Utilities around error handling. */

#ifndef ERROR_H_
#define ERROR_H_

#include <stddef.h>
#include <string.h>

/* Format an error message. */
#define ErrMsgPrintf(ERRMSG, ...) \
    snprintf(ERRMSG, RAFT_ERRMSG_BUF_SIZE, __VA_ARGS__)

/* Wrap the given error message with an additional prefix message.. */
#define ErrMsgWrapf(ERRMSG, ...)            \
    do {                                    \
        char _errmsg[RAFT_ERRMSG_BUF_SIZE]; \
        ErrMsgPrintf(_errmsg, __VA_ARGS__); \
        errMsgWrap(ERRMSG, _errmsg);        \
    } while (0)

void errMsgWrap(char *e, const char *format);

/* Transfer an error message from an object to another, wrapping it. */
#define ErrMsgTransfer(ERRMSG1, ERRMSG2, FORMAT)     \
    strncpy(ERRMSG2, ERRMSG1, RAFT_ERRMSG_BUF_SIZE); \
    ErrMsgWrapf(ERRMSG2, FORMAT)

#define ErrMsgTransferf(ERRMSG1, ERRMSG2, FORMAT, ...) \
    strncpy(ERRMSG2, ERRMSG1, RAFT_ERRMSG_BUF_SIZE);   \
    ErrMsgWrapf(ERRMSG2, FORMAT, __VA_ARGS__)

/* Format the out of memory error message. */
#define ErrMsgOom(ERRMSG) \
    snprintf(ERRMSG, RAFT_ERRMSG_BUF_SIZE, "out of memory")

/* Convert a numeric raft error code to a human-readable error message. */
const char *errCodeToString(int errnum);

#endif /* ERROR_H_ */
