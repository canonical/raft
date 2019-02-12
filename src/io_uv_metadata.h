/**
 * Read and write Raft metadata state.
 */

#ifndef RAFT_IO_UV_METADATA_H_
#define RAFT_IO_UV_METADATA_H_

#include "../include/raft.h"

/**
 * Information persisted in a single metadata file.
 */
struct raft__io_uv_metadata;

/**
 * Read Raft metadata from disk, choosing the most recent version (either the
 * metadata1 or metadata2 file).
 */
int raft__io_uv_metadata_load(struct raft_logger *logger,
                              const char *dir,
                              struct raft__io_uv_metadata *metadata);

/**
 * Write the given metadata to disk, writing the appropriate metadata file
 * according to the metadata version (if the version is odd, write metadata1,
 * otherwise write metadata2).
 */
int raft__io_uv_metadata_store(struct raft_logger *logger,
                               const char *dir,
                               const struct raft__io_uv_metadata *metadata);

struct raft__io_uv_metadata
{
    unsigned long long version; /* Monotonically increasing version */
    raft_term term;             /* Current term */
    unsigned voted_for;         /* Server ID of last vote, or 0 */
    raft_index start_index;     /* Raft log start index */
};

#endif /* RAFT_IO_UV_METADATA_H */
