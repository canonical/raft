/* Utilities around error handling. */

#ifndef ERROR_H_
#define ERROR_H_

#include <stddef.h>

struct ErrMsg
{
    char msg[256];
};

/* Return the underlying message. */
const char *ErrMsgString(struct ErrMsg *e);

/* Format an error message. */
void ErrMsgPrintf(struct ErrMsg *e, const char *format, ...);

/* Wrap the given error message with an additional prefix message.. */
void ErrMsgWrapf(struct ErrMsg *e, const char *format, ...);

/* Set to message for the given code. */
void ErrMsgFromCode(struct ErrMsg *e, int code);

/* Convert a numeric raft error code to a human-readable error message. */
const char *errCodeToString(int errnum);

#endif /* ERROR_H_ */
