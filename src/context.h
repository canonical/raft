/**
 * Internal APIs for manipulating raft_context state.
 */

#ifndef RAFT_CONTEXT_H
#define RAFT_CONTEXT_H

#include "../include/raft.h"

/**
 * Set new content in the errmsg buffer.
 */
void raft_context__errorf(struct raft_context *ctx, const char *fmt, ...);

/**
 * Convenience to set the new content of the error string from the given code.
 */
void raft_context__status(struct raft_context *ctx, int rv);

/**
 * Prepend new content to the errmsg buffer.
 */
void raft_context__wrapf(struct raft_context *ctx, const char *fmt, ...);

#endif /* RAFT_CONTEXT_H */
