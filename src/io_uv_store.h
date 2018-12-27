/**
 * Handle on-disk storage logic in the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_STORE_H
#define RAFT_IO_UV_STORE_H

#include <sys/types.h>

#include "../include/raft.h"

/**
 * Information persisted in a single metadata file.
 */
struct raft_io_uv_metadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
    raft_index start_index;     /* Raft log start index */
};

struct raft_io_uv_store
{
    char *dir;         /* Data directory */
    size_t block_size; /* File system block size */

    /* Cache of the last metadata file that was written (either metadata1 or
     * metadata2). */
    struct raft_io_uv_metadata metadata;
};

int raft_io_uv_store__init(struct raft_io_uv_store *s,
                           const char *dir,
                           char *errmsg);

void raft_io_uv_store__close(struct raft_io_uv_store *s);

/**
 * Synchronously load all state from disk.
 */
int raft_io_uv_store__load(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg);

/**
 * Synchronously persist the term in the given request.
 */
int raft_io_uv_store__term(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg);

/**
 * Synchronously persist the vote in the given request.
 */
int raft_io_uv_store__vote(struct raft_io_uv_store *s,
                           struct raft_io_request *request,
                           char *errmsg);

/**
 * Asynchronously persist the entries in the given request.
 */
int raft_io_uv_store__entries(struct raft_io_uv_store *s,
                              const unsigned request_id,
                              char *errmsg);

#endif /* RAFT_IO_UV_STORE_H */
