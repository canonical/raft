#include <string.h>

#include "../include/raft.h"
#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv_ip.h"
#include "io_uv_tcp.h"

/* Implementation of raft_io_uv_transport->init. */
static int tcp_init(struct raft_io_uv_transport *transport,
                    unsigned id,
                    const char *address)
{
    struct io_uv__tcp *t;
    int rv;
    t = transport->impl;
    t->id = id;
    t->address = address;
    rv = uv_tcp_init(t->loop, &t->listener);
    assert(rv == 0);
    return 0;
}

/* Close callback for io_uv__tcp->listener. */
static void listener_close_cb(struct uv_handle_s *handle)
{
    struct io_uv__tcp *t = handle->data;
    if (t->close_cb != NULL) {
        t->close_cb(t->transport);
    }
}

/* Implementation of raft_io_uv_transport->close. */
static void tcp_close(struct raft_io_uv_transport *transport,
                      raft_io_uv_transport_close_cb cb)
{
    struct io_uv__tcp *t = transport->impl;
    t->close_cb = cb;
    io_uv__tcp_connect_stop(t);
    io_uv__tcp_listen_stop(t);
    uv_close((struct uv_handle_s *)&t->listener, listener_close_cb);
}

int raft_io_uv_tcp_init(struct raft_io_uv_transport *transport,
                        struct uv_loop_s *loop)
{
    struct io_uv__tcp *t;

    t = raft_malloc(sizeof *t);
    if (t == NULL) {
        /* UNTESTED: not interesting */
        return RAFT_ENOMEM;
    }
    t->transport = transport;
    t->loop = loop;
    t->id = 0;
    t->address = NULL;
    t->listener.data = t;
    t->accept_cb = NULL;
    t->close_cb = NULL;
    RAFT__QUEUE_INIT(&t->accept_conns);
    RAFT__QUEUE_INIT(&t->connect_reqs);

    transport->impl = t;
    transport->init = tcp_init;
    transport->listen = io_uv__tcp_listen;
    transport->connect = io_uv__tcp_connect;
    transport->close = tcp_close;

    return 0;
}

void raft_io_uv_tcp_close(struct raft_io_uv_transport *transport)
{
    raft_free(transport->impl);
}
