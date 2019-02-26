/**
 * Read and write Raft metadata state.
 */

#ifndef RAFT_IO_UV_METADATA_H_
#define RAFT_IO_UV_METADATA_H_

#include "../include/raft.h"

/**
 * Information persisted in a single metadata file.
 */
struct io_uv__metadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
};

/**
 * Read Raft metadata from disk, choosing the most recent version (either the
 * metadata1 or metadata2 file).
 */
int io_uv__metadata_load(struct raft_logger *logger,
                         const char *dir,
                         struct io_uv__metadata *metadata);

/**
 * Write the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2).
 */
int io_uv__metadata_store(struct raft_logger *logger,
                          const char *dir,
                          const struct io_uv__metadata *metadata);

#endif /* RAFT_IO_UV_METADATA_H */
