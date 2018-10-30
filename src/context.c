#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "context.h"

void raft_context_format(char *str, size_t size, struct raft_context *ctx)
{
    char *head = str;
    size_t tail = size;
    size_t n;

    if (size == 0 || str == NULL) {
        return;
    }

    n = snprintf(head, tail, "(");
    if (n >= tail) {
        return;
    }
    head += n;
    tail -= n;

    if (ctx->state != NULL) {
        const char *state;

        assert(*ctx->state == RAFT_STATE_FOLLOWER ||
               *ctx->state == RAFT_STATE_CANDIDATE ||
               *ctx->state == RAFT_STATE_LEADER);

        state = raft_state_names[*ctx->state];
        n = snprintf(head, tail, "state=%s ", state);
        if (n >= tail) {
            return;
        }
        head += n;
        tail -= n;
    }

    if (ctx->current_term != NULL) {
        n = snprintf(head, tail, "current-term=%lld ", *ctx->current_term);
        if (n >= tail) {
            return;
        }
        head += n;
        tail -= n;
    }

    if ((head - 1)[0] == ' ') {
        head--;
        tail++;
    }

    snprintf(head, tail, ")");
}

void raft_context__errorf(struct raft_context *ctx, const char *fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    vsnprintf(ctx->errmsg, sizeof ctx->errmsg, fmt, args);
    va_end(args);
}

void raft_context__status(struct raft_context *ctx, int rv)
{
    raft_context__errorf(ctx, raft_strerror(rv));
}

void raft_context__wrapf(struct raft_context *ctx, const char *fmt, ...)
{
    char errmsg[sizeof ctx->errmsg];
    va_list args;

    strncpy(errmsg, ctx->errmsg, sizeof errmsg);

    va_start(args, fmt);
    vsnprintf(ctx->errmsg, sizeof ctx->errmsg, fmt, args);
    va_end(args);

    snprintf(ctx->errmsg + strlen(ctx->errmsg),
             sizeof ctx->errmsg - strlen(ctx->errmsg), ": %s", errmsg);
}
