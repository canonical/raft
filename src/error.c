#include "../include/raft.h"

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
