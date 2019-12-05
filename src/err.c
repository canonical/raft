#include "err.h"

#include <string.h>

#include "../include/raft.h"
#include "assert.h"

#define WRAP_SEP ": "
#define WRAP_SEP_LEN (int)strlen(WRAP_SEP)

void errMsgWrap(char *e, const char *format)
{
    size_t n = RAFT_ERRMSG_BUF_SIZE;
    size_t prefix_n;
    size_t prefix_and_sep_n;
    size_t trail_n;
    size_t i;

    /* Calculate the lenght of the prefix. */
    prefix_n = strlen(format);

    /* If there isn't enough space for the ": " separator and at least one
     * character of the wrapped error message, then just print the prefix. */
    if (prefix_n >= n - (WRAP_SEP_LEN + 1)) {
        ErrMsgPrintf(e, "%s", format);
        return;
    }

    /* Right-shift the wrapped message, to make room for the prefix. */
    prefix_and_sep_n = prefix_n + WRAP_SEP_LEN;
    trail_n = strnlen(e, n - prefix_and_sep_n - 1);
    memmove(e + prefix_and_sep_n, e, trail_n);
    e[prefix_and_sep_n + trail_n] = 0;

    /* Print the prefix. */
    ErrMsgPrintf(e, "%s", format);

    /* Print the separator.
     *
     * Avoid using strncpy(e->msg + prefix_n, WRAP_SEP, WRAP_SEP_LEN) since it
     * generates a warning. */
    for (i = 0; i < WRAP_SEP_LEN; i++) {
        e[prefix_n + i] = WRAP_SEP[i];
    }
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
    X(RAFT_NAMETOOLONG, "resource name too long")                       \
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
