#include <assert.h>
#include <stdio.h>

#include "../include/raft.h"

void raft_context_format(char *buf, const size_t size, struct raft_context *ctx)
{
    char *cursor = buf; /* Position of the next character to render */
    size_t cap = size;  /* Remaining capacity of the buffer */
    size_t n;

    if (size == 0 || buf == NULL) {
        return;
    }

    /* Render the opening parenthesis */
    n = snprintf(cursor, cap, "(");
    if (n >= cap) {
        return;
    }
    cursor += n;
    cap -= n;
    assert(cap > 0);

    /* Render the state name if available. */
    if (ctx->state != NULL) {
        const char *state;

        assert(*ctx->state == RAFT_STATE_UNAVAILABLE ||
               *ctx->state == RAFT_STATE_FOLLOWER ||
               *ctx->state == RAFT_STATE_CANDIDATE ||
               *ctx->state == RAFT_STATE_LEADER);

        state = raft_state_names[*ctx->state];
        n = snprintf(cursor, cap, "state=%s ", state);
        if (n >= cap) {
            return;
        }
        cursor += n;
        cap -= n;
	assert(cap > 0);
    }

    /* Render the current term if available. */
    if (ctx->current_term != NULL) {
        n = snprintf(cursor, cap, "current-term=%lld ", *ctx->current_term);
        if (n >= cap) {
            return;
        }
        cursor += n;
        cap -= n;
	assert(cap > 0);
    }

    /* If the last rendered character is the field separator, remove it. */
    if ((cursor - 1)[0] == ' ') {
        cursor--;
        cap++;
    }

    assert(cap > 0);
    snprintf(cursor, cap, ")");
}
