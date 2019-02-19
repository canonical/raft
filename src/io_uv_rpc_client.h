/**
 * Maintain connections from this server to other servers.
 */

#ifndef RAFT_IO_UV_RPC_CLIENT_H_
#define RAFT_IO_UV_RPC_CLIENT_H_

#include "io_uv_rpc.h"
#include "queue.h"

/**
 * Maximum number of requests that can be buffered.
 */
#define RAFT__IO_UV_RPC_CLIENT_QUEUE_SIZE 16

struct raft__io_uv_rpc_send
{
    struct raft__io_uv_rpc_client *client;
    struct raft_io_send *req;
    uv_buf_t *bufs;
    unsigned n_bufs;
    uv_write_t write;
    raft__queue queue;
};

struct raft__io_uv_rpc_client
{
    struct raft__io_uv_rpc *rpc;   /* The backend that owns us */
    struct uv_timer_s timer;       /* Schedule connection attempts */
    struct raft_io_uv_connect req; /* Connection request */
    struct uv_stream_s *stream;    /* Connection handle */
    unsigned n_connect_attempt;    /* Consecutive connection attempts */
    unsigned id;                   /* ID of the other server */
    char *address;                 /* Address of the other server */
    int state;                     /* Current state (closed, connecting, ...) */

    /* Pending requests */
    raft__queue send_queue;
    unsigned n_send_queue;
};

/**
 * Search a client object matching the given server ID. If not found, a new one
 * will be created and appended to the clients array.
 */
int raft__io_uv_rpc_client_get(struct raft__io_uv_rpc *r,
                               const unsigned id,
                               const char *address,
                               struct raft__io_uv_rpc_client **client);

/**
 * Finish to use this client. It will eventually destroy the object
 * altogether. Must be only called upon shutdown.
 */
void raft__io_uv_rpc_client_put(struct raft__io_uv_rpc_client *c);

/**
 * Send a request to the remote server.
 */
int raft__io_uv_rpc_client_send(struct raft__io_uv_rpc_client *c,
                                struct raft__io_uv_rpc_send *req);


#endif /* RAFT_IO_UV_RPC_CLIENT_H_ */
