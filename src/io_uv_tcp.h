#ifndef RAFT_IO_UV_TCP_H_
#define RAFT_IO_UV_TCP_H_

#include "../include/raft.h"

#include "queue.h"

/* Protocol version. */
#define RAFT__IO_UV_TCP_PROTOCOL 1

/* State codes */
enum {
    RAFT__IO_UV_TCP_ACTIVE = 1,
    RAFT__IO_UV_TCP_CLOSING,
    RAFT__IO_UV_TCP_CLOSED
};

struct raft__io_uv_tcp
{
    struct raft_io_uv_transport *transport;
    struct raft_logger *logger;
    struct uv_loop_s *loop;
    unsigned id;
    const char *address;
    int state;

    struct uv_tcp_s listener;
    bool listening;

    raft__queue accept_queue; /* Incoming connections waiting for handshake */

    raft_io_uv_accept_cb accept_cb;
    raft_io_uv_transport_close_cb close_cb;
};

void raft__io_uv_tcp_continue(struct raft__io_uv_tcp *tcp);

#endif /* RAFT_IO_UV_TCP_H_ */
