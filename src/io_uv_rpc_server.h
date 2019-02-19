#ifndef RAFT_IO_UV_RPC_SERVER_H_
#define RAFT_IO_UV_RPC_SERVER_H_

#include "io_uv_rpc.h"

/**
 * A connection from another server to this server, used to receive requests.
 */
struct raft__io_uv_rpc_server
{
    struct raft__io_uv_rpc *rpc;  /* The backend that owns us */
    unsigned id;                 /* ID of the server */
    char *address;               /* Address of the other server */
    struct uv_stream_s *stream;  /* Connection handle */
    uv_buf_t buf;                /* Sliding buffer for reading incoming data */
    uint64_t preamble[2];        /* Static buffer with the request preamble */
    uv_buf_t header;             /* Dynamic buffer with the request header */
    uv_buf_t payload;            /* Dynamic buffer with the request payload */
    struct raft_message message; /* The message being received */
    bool aborted;                /* Whether the connection has been aborted */
};

/**
 * Create a new server object and append it to the servers array.
 */
int raft__io_uv_rpc_add_server(struct raft__io_uv_rpc *r,
                               unsigned id,
                               const char *address,
                               struct uv_stream_s *stream);

void raft__io_uv_rpc_server_stop(struct raft__io_uv_rpc_server *s);

#endif /* RAFT_IO_UV_RPC_SERVER_H_ */
