/* Error codes and utilities. */

#ifndef UV_ERROR_H_
#define UV_ERROR_H_

#define UV__ERRMSG_MAX_LEN 2048

/* Convenience for declaring a char buffer holding an error message. */
typedef char uvErrMsg[UV__ERRMSG_MAX_LEN];

/* Format an error message, truncating it if it's too long. */
#define uvErrMsgPrintf(ERRMSG, ...)			\
    snprintf(ERRMSG, UV__ERRMSG_MAX_LEN, __VA_ARGS__);

#endif /* UV_ERROR_H_ */
