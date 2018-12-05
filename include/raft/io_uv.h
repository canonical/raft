#ifndef RAFT_IO_UV_H
#define RAFT_IO_UV_H

#include <uv.h>

struct raft_io;

/**
 * Configure the given @raft_io instance to use a libuv-based I/O
 * implementation.
 */
int raft_io_uv_init(struct raft_io *io,
                    struct uv_loop_s *loop,
                    const char *dir);

#endif /* RAFT_IO_UV_H */
