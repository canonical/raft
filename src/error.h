/**
 * Internal APIs for populating the last error message buffer.
 */

#ifndef RAFT_ERROR_H
#define RAFT_ERROR_H

#include "../include/raft.h"

/**
 * Set new content in the errmsg buffer.
 */
void raft_error__printf(struct raft *r, const int rv, const char *fmt, ...);

/**
 * Prepend new content to the errmsg buffer.
 */
void raft_error__vwrapf(struct raft *r, const char *fmt, va_list args);

/**
 * Prepend new content to the errmsg buffer.
 */
void raft_error__wrapf(struct raft *r, const char *fmt, ...);

#endif /* RAFT_ERROR_H */
