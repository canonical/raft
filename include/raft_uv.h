#ifndef RAFT_UV_H
#define RAFT_UV_H

#include <uv.h>

struct raft;

int raft_uv(struct raft *r, struct uv_loop_s *loop);

#endif /* RAFT_IO_UV_H */
