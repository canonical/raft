/* Error codes and utilities. */

#ifndef UV_ERROR_H_
#define UV_ERROR_H_

/* Error codes. */
enum {
    UV__ERROR = 1, /* Generic system error. */
    UV__CANCELED,  /* Request was canceled */
    UV__NODATA,    /* Short read */
    UV__NOENT,     /* File does not exist */
    UV__NOTSUPP,   /* Operation not supported */
    UV__AGAIN      /* Try again later */
};

/* Maximum length of an error message. */
#define UV__ERRMSG_MAX_LEN 2048

/* Convenience for declaring a char buffer holding an error message. */
typedef char uvErrMsg[UV__ERRMSG_MAX_LEN];

/* Format an error message, truncating it if it's too long. */
#define uvErrMsgPrintf(ERRMSG, ...) \
    snprintf(ERRMSG, UV__ERRMSG_MAX_LEN, __VA_ARGS__);

/* Format an error message caused by a failed system call or stdlib function. */
#define uvErrMsgSys(ERRMSG, SYSCALL, ERRNUM)                             \
    {                                                                        \
        char e_[1024];                                                       \
        sprintf(ERRMSG, #SYSCALL ": %s", strerror_r(ERRNUM, e_, sizeof e_)); \
    }

#endif /* UV_ERROR_H_ */
