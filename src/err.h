/* Utilities around error handling. */

#ifndef ERROR_H_
#define ERROR_H_

/* Maximum length of an error message. */
#define ERRMSG_MAX_LEN 2048

/* Convenience for declaring a fixed-size char buffer holding an error
 * message. */
typedef char ErrMsg[ERRMSG_MAX_LEN];

/* Convert a numeric raft error code to a human-readable error message. */
const char *errCodeToString(int errnum);

#endif /* ERROR_H_ */
