#include <assert.h>
#include <stdio.h>

#include "../include/raft.h"

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
