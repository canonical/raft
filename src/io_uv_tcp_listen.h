/**
 * Accept incoming TCP connections from other servers.
 */

#ifndef RAFT_IO_UV_TCP_ACCEPT_H_
#define RAFT_IO_UV_TCP_ACCEPT_H_

#include <uv.h>

#include "../include/raft.h"

#include "io_uv_tcp.h"

/**
 * Implementation of raft_io_uv_transport->listen.
 */
int raft__io_uv_tcp_listen(struct raft_io_uv_transport *t,
                           raft_io_uv_accept_cb cb);

/**
 * Stop accepting incoming connections. All incoming connections being accepted
 * will be canceled.
 */
void raft__io_uv_tcp_stop(struct raft_io_uv_transport *t);

#endif /* RAFT_IO_UV_TCP_ACCEPT_H_ */
