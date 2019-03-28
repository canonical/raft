/**
 * Read log entries and snapshots from disk.
 */

#ifndef RAFT_IO_UV_LOAD_H_
#define RAFT_IO_UV_LOAD_H_

#include "../include/raft.h"

#include "io_uv_fs.h"

struct io_uv;

/**
 * Info about a snapshot metadata file.
 */
struct io_uv__snapshot_meta
{
    raft_term term;
    raft_index index;
    unsigned long long timestamp;
    io_uv__filename filename;
};

/**
 * Metadata about a segment file.
 */
struct io_uv__segment_meta
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
    io_uv__filename filename; /* Segment filename */
};

/**
 * Return a list of all snapshots and segments found in the data directory. Both
 * snapshots and segments are ordered by filename (closed segments come before
 * open ones).
 */
int io_uv__load_list(struct io_uv *uv,
                     struct io_uv__snapshot_meta *snapshots[],
                     size_t *n_snapshots,
                     struct io_uv__segment_meta *segments[],
                     size_t *n_segments);

/**
 * Load the snapshot with the given metadata.
 */
int io_uv__load_snapshot(struct io_uv *uv,
                         struct io_uv__snapshot_meta *meta,
                         struct raft_snapshot *snapshot);

/**
 * Load all entries contained in the given closed segment.
 */
int io_uv__load_closed(struct io_uv *uv,
                       struct io_uv__segment_meta *segment,
                       struct raft_entry *entries[],
                       size_t *n);

/**
 * Load the last snapshot (if any) and all entries contained in all segment
 * files of the data directory.
 */
int io_uv__load_all(struct io_uv *uv,
                    struct raft_snapshot **snapshot,
		    raft_index *start_index,
                    struct raft_entry *entries[],
                    size_t *n);

#endif /* RAFT_IO_UV_LOAD_H */
