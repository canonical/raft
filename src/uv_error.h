/* Error codes and utilities. */

#ifndef UV_ERROR_H_
#define UV_ERROR_H_

#include <uv.h>

#include "err.h"

/* Error codes. */
enum {
    UV__ERROR = 1, /* Generic system error. */
    UV__CANCELED,  /* Request was canceled */
    UV__NODATA,    /* Short read */
    UV__NOENT,     /* File does not exist */
    UV__NOTSUPP,   /* Operation not supported */
    UV__AGAIN      /* Try again later */
};

/* Format an error message caused by a failed system call or stdlib function. */
#define UvErrMsgSys(ERRMSG, SYSCALL, ERRNUM)             \
    {                                                    \
        ErrMsgPrintf(ERRMSG, "%s", uv_strerror(ERRNUM)); \
        ErrMsgWrapf(ERRMSG, SYSCALL);                    \
    }

#endif /* UV_ERROR_H_ */
