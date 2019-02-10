/**
 * Handle network RPC logic in the libuv-based @raft_io backend.
 */

#ifndef RAFT_IO_UV_RPC_H
#define RAFT_IO_UV_RPC_H

#include "../include/raft.h"

struct raft_io_uv_rpc_client;

/**
 * Maximum number of requests that can be buffered.
 */
#define RAFT_IO_UV_RPC_CLIENT__QUEUE_SIZE 16

/**
 * An outgoing RPC request from this server to another server.
 */
struct raft_io_uv_rpc_request
{
    struct raft_io_uv_rpc_client *client;
    uv_write_t req;
    uv_buf_t *bufs;
    unsigned n_bufs;
    void *data;
    void (*cb)(void *data, const int status);
};

struct raft_io_uv_rpc;

/**
 * A connection from this server to another server, used to sent request.
 */
struct raft_io_uv_rpc_client
{
    struct raft_io_uv_rpc *rpc; /* The backend that owns us */
    struct uv_timer_s timer;    /* Schedule connection attempts */
    struct uv_connect_s req;    /* Connection request */
    struct uv_stream_s *stream; /* Connection handle */
    unsigned n_connect_attempt; /* Consecutive connection attempts */
    unsigned id;                /* ID of the other server */
    char *address;              /* Address of the other server */
    int state;                  /* Current state (closed, connecting, ...) */

    /* Buffered queue of pending requests */
    struct raft_io_uv_rpc_request *queue[RAFT_IO_UV_RPC_CLIENT__QUEUE_SIZE];
    size_t n_queue;
};

/**
 * A connection from another server to this server, used to receive requests.
 */
struct raft_io_uv_rpc_server
{
    struct raft_io_uv_rpc *rpc;  /* The backend that owns us */
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
 * Send and receive requests to/from other servers.
 */
struct raft_io_uv_rpc
{
    struct raft_logger *logger;             /* Logger to use */
    struct uv_loop_s *loop;                 /* Event loop to use */
    struct raft_io_uv_transport *transport; /* Outbound and inbound streams */
    struct raft_io_uv_rpc_client **clients; /* Outgoing connections */
    struct raft_io_uv_rpc_server **servers; /* Incoming connections */
    unsigned n_clients;                     /* Length of the clients array */
    unsigned n_servers;                     /* Length of the servers array */
    unsigned connect_retry_delay;           /* Connection retry delay */

    /* Track the number of active asynchronous operations that need to be
     * completed before this backend can be considered fully stopped. */
    unsigned n_active;

    /* Receive callback */
    struct
    {
        void *data;
        void (*cb)(void *data, struct raft_message *msg);
    } recv;

    /* Stop callback */
    struct
    {
        void *data;
        void (*cb)(void *data);
    } stop;
};

/**
 * Initialize the RPC system.
 */
int raft_io_uv_rpc__init(struct raft_io_uv_rpc *r,
                         struct raft_logger *logger,
                         struct uv_loop_s *loop,
                         struct raft_io_uv_transport *transport);

/**
 * Release all resources used by the given RPC system.
 */
void raft_io_uv_rpc__close(struct raft_io_uv_rpc *r);

/**
 * Start accepting incoming requests.
 */
int raft_io_uv_rpc__start(struct raft_io_uv_rpc *r,
                          unsigned id,
                          const char *address,
                          void *data,
                          void (*recv)(void *data, struct raft_message *msg));

/**
 * Stop accepting incoming requests and shutdown all connections.
 */
void raft_io_uv_rpc__stop(struct raft_io_uv_rpc *r,
                          void *data,
                          void (*cb)(void *data));

/**
 * Request a message to be delivered to its recipient.
 */
int raft_io_uv_rpc__send(struct raft_io_uv_rpc *r,
                         const struct raft_message *message,
                         void *data,
                         void (*cb)(void *data, int status));

#endif /* RAFT_IO_UV_RPC_H */
