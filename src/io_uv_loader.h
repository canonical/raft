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
struct raft__io_uv_loader;

/**
 * Metadata about a loaded segment.
 */
struct raft__io_uv_loader_segment;

void raft__io_uv_loader_init(struct raft__io_uv_loader *l,
                             struct raft_logger *logger,
                             const char *dir);

/**
 * Return a list of all segments found in the data directory. The segments are
 * ordered by filename (closed segments come before open ones).
 */
int raft__io_uv_loader_list(struct raft__io_uv_loader *l,
                            struct raft__io_uv_loader_segment *segments[],
                            size_t *n);

/**
 * Load all entries contained in the given closed segment.
 */
int raft__io_uv_loader_load_closed(struct raft__io_uv_loader *l,
                                   struct raft__io_uv_loader_segment *segment,
                                   struct raft_entry *entries[],
                                   size_t *n);

/**
 * Load all entries contained in all segment files of the data directory.
 */
int raft__io_uv_loader_load_all(struct raft__io_uv_loader *l,
                                raft_index start_index,
                                struct raft_entry *entries[],
                                size_t *n);

struct raft__io_uv_loader
{
    struct raft_logger *logger; /* Logger to use */
    const char *dir;            /* Data directory */
};

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

    /* Filename of the segment */
    raft__io_uv_fs_filename filename;
};

#endif /* RAFT_IO_UV_LOADER_H */
