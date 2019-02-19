/**
 * Handle network RPC logic in the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_RPC_H
#define RAFT_IO_UV_RPC_H

#include "../include/raft.h"

/* State codes */
enum {
    RAFT__IO_UV_RPC_ACTIVE = 1,
    RAFT__IO_UV_RPC_CLOSING,
    RAFT__IO_UV_RPC_CLOSED
};

struct raft__io_uv_rpc;
typedef void (*raft__io_uv_rpc_close_cb)(struct raft__io_uv_rpc *r);
typedef void (*raft__io_uv_rpc_recv_cb)(struct raft__io_uv_rpc *r,
                                        struct raft_message *msg);

/**
 * Send and receive requests to/from other servers.
 */
struct raft__io_uv_rpc
{
    void *data;                              /* User data */
    struct raft_logger *logger;              /* Logger to use */
    struct uv_loop_s *loop;                  /* Event loop to use */
    struct raft_io_uv_transport *transport;  /* Outbound and inbound streams */
    struct raft__io_uv_rpc_client **clients; /* Outgoing connections */
    struct raft__io_uv_rpc_server **servers; /* Incoming connections */
    unsigned n_clients;                      /* Length of the clients array */
    unsigned n_servers;                      /* Length of the servers array */
    unsigned connect_retry_delay;            /* Connection retry delay */
    int state;

    raft__io_uv_rpc_recv_cb recv_cb;   /* Receive callback */
    raft__io_uv_rpc_close_cb close_cb; /* Close callback */
};

/**
 * Initialize the RPC system.
 */
int raft__io_uv_rpc_init(struct raft__io_uv_rpc *r,
                         struct raft_logger *logger,
                         struct uv_loop_s *loop,
                         struct raft_io_uv_transport *transport);

/**
 * Start accepting incoming requests.
 */
int raft__io_uv_rpc_start(struct raft__io_uv_rpc *r,
			  raft__io_uv_rpc_recv_cb recv_cb);

int raft__io_uv_rpc_send(struct raft__io_uv_rpc *r,
                         struct raft_io_send *req,
                         const struct raft_message *message,
                         raft_io_send_cb cb);
/**
 * Stop sending and receiving RPC messages.
 */
void raft__io_uv_rpc_close(struct raft__io_uv_rpc *r,
                           raft__io_uv_rpc_close_cb cb);

#endif /* RAFT_IO_UV_RPC_H */
