#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../include/raft.h"

#include "error.h"

struct raft_uv__data
{
    struct raft *raft;
    struct uv_loop_s *loop;
    struct uv_timer_s ticker;
};

static void raft_error__uv(struct raft *r, const int rv, const char *fmt, ...)
{
    const char *msg = uv_strerror(rv);
    va_list args;

    strncpy(r->errmsg, msg, strlen(msg));

    if (fmt == NULL) {
        return;
    }

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}

static int raft_uv__tick(struct raft *r, unsigned msecs) {}

static int raft_uv__stop(struct raft *r) {}

static void raft_uv__close(struct raft *r)
{
    raft_free(r->backend.data);
}

int raft_uv(struct raft *r, struct uv_loop_s *loop)
{
    struct raft_uv__data *data;

    assert(r != NULL);
    assert(loop != NULL);

    data = raft_malloc(sizeof *data);
    if (data == NULL) {
        return RAFT_ERR_NOMEM;
    }

    data->raft = r;
    data->loop = loop;

    r->backend.data = data;
    r->backend.tick = raft_uv__tick;
    r->backend.stop = raft_uv__stop;
    r->backend.close = raft_uv__close;

    return 0;
}
