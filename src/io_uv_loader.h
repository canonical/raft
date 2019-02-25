/**
 * Read log entries from disk.
 */

#ifndef RAFT_IO_UV_LOADER_H_
#define RAFT_IO_UV_LOADER_H_

#include "../include/raft.h"

#include "io_uv_fs.h"

/**
 * Segment loader.
 */
struct raft__io_uv_loader
{
    struct raft_logger *logger; /* Logger to use */
    const char *dir;            /* Data directory */
};

/**
 * Metadata about a snapshot metadata file.
 */
struct raft__io_uv_loader_snapshot
{
    raft_term term;
    raft_index index;
    unsigned long long timestamp;
    raft__io_uv_fs_filename filename;
};

/**
 * Metadata about a segment file.
 */
struct raft__io_uv_loader_segment
{
    bool is_open; /* Whether the segment is open */
    union {
        struct
        {
            raft_index first_index; /* First index in a closed segment */
            raft_index end_index;   /* Last index in a closed segment */
        };
        struct
        {
            unsigned long long counter; /* Open segment counter */
        };
    };
    raft__io_uv_fs_filename filename; /* Segment filename */
};

void raft__io_uv_loader_init(struct raft__io_uv_loader *l,
                             struct raft_logger *logger,
                             const char *dir);

/**
 * Return a list of all snapshots and segments found in the data directory. Both
 * snapshots and segments are ordered by filename (closed segments come before
 * open ones).
 */
int raft__io_uv_loader_list(struct raft__io_uv_loader *l,
                            struct raft__io_uv_loader_snapshot *snapshots[],
                            size_t *n_snapshots,
                            struct raft__io_uv_loader_segment *segments[],
                            size_t *n_segments);

/**
 * Load the snapshot with the given metadata.
 */
int raft__io_uv_loader_load_snapshot(struct raft__io_uv_loader *l,
                                     struct raft__io_uv_loader_snapshot *meta,
                                     struct raft_snapshot *snapshot);

/**
 * Load all entries contained in the given closed segment.
 */
int raft__io_uv_loader_load_closed(struct raft__io_uv_loader *l,
                                   struct raft__io_uv_loader_segment *segment,
                                   struct raft_entry *entries[],
                                   size_t *n);

/**
 * Load the last snapshot (if any) and all entries contained in all segment
 * files of the data directory.
 */
int raft__io_uv_loader_load_all(struct raft__io_uv_loader *l,
				struct raft_snapshot **snapshot,
                                struct raft_entry *entries[],
                                size_t *n);

#endif /* RAFT_IO_UV_LOADER_H */
