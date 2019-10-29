#include "err.h"

#include <string.h>

#include "../include/raft.h"
#include "assert.h"

#define WRAP_SEP ": "
#define WRAP_SEP_LEN (int)strlen(WRAP_SEP)

char *errMsgPrintf(const char *format, ...)
{
    int size;
    char *e;
    va_list args;
    int rv;

    va_start(args, format);
    size = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (size == -1) {
        return NULL;
    }
    size++;

    e = raft_malloc(size);
    if (e == NULL) {
        return NULL;
    }

    va_start(args, format);
    rv = vsnprintf(e, size, format, args);
    va_end(args);

    assert(rv == size - 1);

    return e;
}

char *errMsgWrapf(char *e, const char *format, ...)
{
    int size;
    char *prefix;
    char *result;
    va_list args;
    int rv;

    /* Calculate the lenght of the prefix. */
    va_start(args, format);
    size = vsnprintf(NULL, 0, format, args);
    va_end(args);

    if (size == -1) {
        return e;
    }
    size++;

    prefix = raft_malloc(size);
    if (prefix == NULL) {
        return e;
    }

    va_start(args, format);
    rv = vsnprintf(prefix, size, format, args);
    va_end(args);

    assert(rv == size - 1);

    /* Total size of the new message. */
    size += WRAP_SEP_LEN + strlen(e);

    result = raft_malloc(size);
    if (result == NULL) {
        raft_free(prefix);
        return e;
    }

    rv = snprintf(result, size, "%s" WRAP_SEP "%s", prefix, e);
    assert(rv == size - 1);

    raft_free(prefix);
    raft_free(e);

    return result;
}

#define ERR_CODE_TO_STRING_MAP(X)                                       \
    X(RAFT_NOMEM, "out of memory")                                      \
    X(RAFT_BADID, "server ID is not valid")                             \
    X(RAFT_DUPLICATEID, "server ID already in use")                     \
    X(RAFT_DUPLICATEADDRESS, "server address already in use")           \
    X(RAFT_ALREADYVOTING, "server is already voting")                   \
    X(RAFT_MALFORMED, "encoded data is malformed")                      \
    X(RAFT_NOTLEADER, "server is not the leader")                       \
    X(RAFT_LEADERSHIPLOST, "server has lost leadership")                \
    X(RAFT_SHUTDOWN, "server is shutting down")                         \
    X(RAFT_CANTBOOTSTRAP, "bootstrap only works on new clusters")       \
    X(RAFT_CANTCHANGE, "a configuration change is already in progress") \
    X(RAFT_CORRUPT, "persisted data is corrupted")                      \
    X(RAFT_CANCELED, "operation canceled")                              \
    X(RAFT_NAMETOOLONG, "data directory path is too long")              \
    X(RAFT_TOOBIG, "data is too big")                                   \
    X(RAFT_NOCONNECTION, "no connection to remote server available")    \
    X(RAFT_BUSY, "operation can't be performed at this time")           \
    X(RAFT_IOERR, "I/O error")

#define ERR_CODE_TO_STRING_CASE(CODE, MSG) \
    case CODE:                             \
        return MSG;

const char *errCodeToString(int errnum)
{
    switch (errnum) {
        ERR_CODE_TO_STRING_MAP(ERR_CODE_TO_STRING_CASE);
        default:
            return "unknown error";
    }
}
