/**
 * Information about a single store segment.
 */

#ifndef RAFT_IO_UV_STORE_SEGMENT_H_
#define RAFT_IO_UV_STORE_SEGMENT_H_

#include "../include/raft.h"

#include "uv_file.h"

struct raft__io_uv_segment
{
    const char *dir;            /* Store directory */
    raft_index first_index;     /* Index of first entry */
    raft_index end_index;       /* Index last entry, for closed segments  */
    unsigned long long counter; /* Segment counter, for open segments */
    size_t block_size;          /* Block size */

    struct raft__uv_file file;        /* Open segment file */
    struct raft__uv_file_write write; /* Write file system request */
};

/**
 * Request to create a new open segment.
 */
struct raft__io_uv_segment_create
{
    void *data;                         /* User data */
    struct raft__io_uv_segment *s;      /* Segment */
    struct raft__uv_file_create create; /* Create file request */
};

int raft__io_uv_segment_create(struct raft__io_uv_segment *s,
                               struct raft__io_uv_segment_create *req,
                               struct uv_loop_s *loop,
                               const char *dir,
                               size_t block_size,
			       size_t n_blocks,
                               raft_index first_index,
                               unsigned long long counter,
                               unsigned max_concurrent_writes,
                               raft__uv_file_create_cb cb);

#endif /* RAFT_IO_UV_STORE_SEGMENT_H_ */
