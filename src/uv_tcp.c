#include "uv_tcp.h"

#include <string.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"
#include "assert.h"

/* Implementation of raft_io_uv_transport->config. */
static void uvTcpConfig(struct raft_uv_transport *transport,
                        unsigned id,
                        const char *address)
{
    struct uvTcp *t;
    int rv;
    t = transport->impl;
    t->id = id;
    t->address = address;
    rv = uv_tcp_init(t->loop, &t->listener);
    assert(rv == 0);
    t->listener.data = t;
}

/* Close callback for uvTcp->listener. */
static void listenerCloseCb(struct uv_handle_s *handle)
{
    struct uvTcp *t = handle->data;
    t->listener.data = NULL;
    if (t->close_cb != NULL) {
        t->close_cb(t->transport);
    }
}

/* Implementation of raft_uv_transport->stop. */
static int uvTcpStop(struct raft_uv_transport *transport)
{
    struct uvTcp *t = transport->impl;
    uvTcpListenClose(t);
    uv_close((struct uv_handle_s *)&t->listener, listenerCloseCb);
    return 0;
}

/* Implementation of raft_uv_transport->close. */
static void uvTcpClose(struct raft_uv_transport *transport,
                       raft_uv_transport_close_cb cb)
{
    struct uvTcp *t = transport->impl;
    t->close_cb = cb;
    uvTcpConnectClose(t);

    /* If the listening handle has already been closed, invoke the close
     * callback immediately. */
    if (t->listener.data == NULL) {
        if (t->close_cb != NULL) {
            t->close_cb(t->transport);
        }
    }
}

int raft_uv_tcp_init(struct raft_uv_transport *transport,
                     struct uv_loop_s *loop)
{
    struct uvTcp *t;

    t = raft_malloc(sizeof *t);
    if (t == NULL) {
        /* UNTESTED: not interesting */
        return RAFT_NOMEM;
    }
    t->transport = transport;
    t->loop = loop;
    t->id = 0;
    t->address = NULL;
    t->listener.data = NULL;
    t->accept_cb = NULL;
    t->close_cb = NULL;
    QUEUE_INIT(&t->accept_conns);
    QUEUE_INIT(&t->connect_reqs);

    transport->impl = t;
    transport->config = uvTcpConfig;
    transport->listen = uvTcpListen;
    transport->stop = uvTcpStop;
    transport->connect = uvTcpConnect;
    transport->close = uvTcpClose;

    return 0;
}

void raft_uv_tcp_close(struct raft_uv_transport *transport)
{
    raft_free(transport->impl);
}
