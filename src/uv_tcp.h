#ifndef UV_TCP_H_
#define UV_TCP_H_

#include "../include/raft.h"
#include "../include/raft/uv.h"
#include "queue.h"

/* Protocol version. */
#define UV__TCP_HANDSHAKE_PROTOCOL 1

struct UvTcp
{
    struct raft_uv_transport *transport; /* Interface object we implement */
    struct uv_loop_s *loop;              /* UV loop */
    unsigned id;                         /* ID of this raft server */
    const char *address;                 /* Address of this raft server */
    struct uv_tcp_s listener;            /* Listening TCP socket handle */
    raft_uv_accept_cb accept_cb;         /* Call after accepting a connection */
    queue accept_conns;                  /* Connections being accepted */
    queue connect_reqs;                  /* Pending connection requests */
    bool closing;                        /* True after close() is called */
    raft_uv_transport_close_cb close_cb; /* Call when it's safe to free us */
};

/* Hold state for a single connection request. */
struct UvTcpConnect
{
    struct UvTcp *t;             /* Transport implementation */
    struct raft_uv_connect *req; /* User request */
    uv_buf_t handshake;          /* Handshake data */
    struct uv_tcp_s *tcp;        /* TCP connection socket handle */
    struct uv_connect_s connect; /* TCP connectionr request */
    struct uv_write_s write;     /* TCP handshake request */
    int status;                  /* Returned to the request callback */
    queue queue;                 /* Pending connect queue */
};

/* Implementation of raft_uv_transport->start. */
int UvTcpStart(struct raft_uv_transport *t, raft_uv_accept_cb cb);

/* Implementation of raft_uv_transport->stop. */
int UvTcpStop(struct raft_uv_transport *t);

/* Implementation of raft_uv_transport->connect. */
int UvTcpConnect(struct raft_uv_transport *transport,
                 struct raft_uv_connect *req,
                 unsigned id,
                 const char *address,
                 raft_uv_connect_cb cb);

/* Cancel a pending connection requests. */
void UvTcpConnectCancel(struct UvTcpConnect *req);

#endif /* UV_TCP_H_ */
