#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "error.h"

void raft_errorf(char *errmsg, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vsnprintf(errmsg, RAFT_ERRMSG_SIZE, fmt, args);
    va_end(args);
}

#define RAFT_ERRNO__STRERROR(CODE, MSG) \
    case CODE:                          \
        return MSG;

const char *raft_strerror(int errnum)
{
    switch (errnum) {
        RAFT_ERRNO_MAP(RAFT_ERRNO__STRERROR);

        default:
            return "unknown error";
    }
}

const char *raft_errmsg(struct raft *r)
{
    assert(r != NULL);
    return r->errmsg;
}

void raft_error__printf(struct raft *r, const int rv, const char *fmt, ...)
{
    const char *msg = raft_strerror(rv);
    va_list args;

    strncpy(r->errmsg, msg, strlen(msg));

    if (fmt == NULL) {
        return;
    }

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}

void raft_error__vwrapf(struct raft *r, const char *fmt, va_list args)
{
    char msg[sizeof r->errmsg];

    strncpy(msg, r->errmsg, sizeof msg);
    vsnprintf(r->errmsg, sizeof r->errmsg, fmt, args);

    snprintf(r->errmsg + strlen(r->errmsg),
             sizeof r->errmsg - strlen(r->errmsg), ": %s", msg);
}

void raft_error__wrapf(struct raft *r, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}
