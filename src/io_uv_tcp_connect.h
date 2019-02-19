/**
 * Establish outgoing TCP connections to other servers.
 */

#ifndef RAFT_IO_UV_TCP_CONNECT_H_
#define RAFT_IO_UV_TCP_CONNECT_H_

#include <uv.h>

#include "../include/raft.h"

#include "io_uv_tcp.h"

/**
 * Implementation of raft_io_uv_transport->connect.
 */
int raft__io_uv_tcp_connect(struct raft_io_uv_transport *t,
                             struct raft_io_uv_connect *req,
                             unsigned id,
                             const char *address,
                             raft_io_uv_connect_cb cb);

/**
 * Implementation of raft_io_uv_transport->cancel.
 */
void raft__io_uv_tcp_cancel(struct raft_io_uv_transport *t,
                            struct raft_io_uv_connect *req);

#endif /* RAFT_IO_UV_TCP_CONNECT_H_ */
