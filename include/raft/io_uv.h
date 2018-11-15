#ifndef RAFT_IO_UV_H
#define RAFT_IO_UV_H

#include <uv.h>

struct raft;

/**
 * Use the I/O implementation based on libuv.
 */
int raft_io_uv_init(struct raft *r, struct uv_loop_s *loop, const char *dir);

#endif /* RAFT_IO_UV_H */
