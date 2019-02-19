#include <string.h>

#include "../include/raft.h"

#include "assert.h"
#include "binary.h"
#include "io_uv_ip.h"
#include "io_uv_tcp.h"
#include "io_uv_tcp_connect.h"
#include "io_uv_tcp_listen.h"

static int raft__io_uv_tcp_init(struct raft_io_uv_transport *t,
                                unsigned id,
                                const char *address);

static void raft__io_uv_tcp_close(struct raft_io_uv_transport *t,
                                  raft_io_uv_transport_close_cb cb);

static void raft__io_uv_tcp_maybe_closed(struct raft__io_uv_tcp *tcp);

int raft_io_uv_tcp_init(struct raft_io_uv_transport *t,
                        struct raft_logger *logger,
                        struct uv_loop_s *loop)
{
    struct raft__io_uv_tcp *tcp;

    tcp = raft_malloc(sizeof *tcp);
    if (tcp == NULL) {
        /* UNTESTED: not interesting */
        return RAFT_ERR_NOMEM;
    }

    tcp->transport = t;
    tcp->logger = logger;
    tcp->loop = loop;
    tcp->state = 0;
    tcp->listening = false;

    tcp->listener.data = tcp;

    RAFT__QUEUE_INIT(&tcp->accept_queue);

    t->impl = tcp;
    t->init = raft__io_uv_tcp_init;
    t->listen = raft__io_uv_tcp_listen;
    t->connect = raft__io_uv_tcp_connect;
    t->stop = raft__io_uv_tcp_stop;
    t->close = raft__io_uv_tcp_close;

    return 0;
}

void raft_io_uv_tcp_close(struct raft_io_uv_transport *t)
{
    struct raft__io_uv_tcp *tcp = t->impl;

    raft_free(tcp);
}

void raft__io_uv_tcp_continue(struct raft__io_uv_tcp *tcp)
{
    assert(tcp->state == RAFT__IO_UV_TCP_ACTIVE ||
           tcp->state == RAFT__IO_UV_TCP_CLOSING);

    if (tcp->state == RAFT__IO_UV_TCP_CLOSING) {
        raft__io_uv_tcp_maybe_closed(tcp);
    }
}

static int raft__io_uv_tcp_init(struct raft_io_uv_transport *t,
                                unsigned id,
                                const char *address)
{
    struct raft__io_uv_tcp *tcp;

    tcp = t->impl;

    tcp->id = id;
    tcp->address = address;

    tcp->state = RAFT__IO_UV_TCP_ACTIVE;

    return 0;
}

static void raft__io_uv_tcp_close(struct raft_io_uv_transport *t,
                                  raft_io_uv_transport_close_cb cb)
{
    struct raft__io_uv_tcp *tcp = t->impl;

    assert(tcp->state == RAFT__IO_UV_TCP_ACTIVE);

    tcp->state = RAFT__IO_UV_TCP_CLOSING;
    tcp->close_cb = cb;

    raft__io_uv_tcp_maybe_closed(tcp);
}

static void raft__io_uv_tcp_maybe_closed(struct raft__io_uv_tcp *tcp)
{
    assert(tcp->state == RAFT__IO_UV_TCP_CLOSING);

    if (tcp->listening) {
        return;
    }

    tcp->state = RAFT__IO_UV_TCP_CLOSED;

    if (tcp->close_cb != NULL) {
        tcp->close_cb(tcp->transport);
    }
}
