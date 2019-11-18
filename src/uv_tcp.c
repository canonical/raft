#include "uv_tcp.h"

#include <string.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"
#include "assert.h"
#include "err.h"

/* Implementation of raft_uv_transport->init. */
static int uvTcpInit(struct raft_uv_transport *transport,
                     unsigned id,
                     const char *address)
{
    struct UvTcp *t = transport->impl;
    int rv;
    assert(id > 0);
    assert(address != NULL);
    t->id = id;
    t->address = address;
    rv = uv_tcp_init(t->loop, &t->listener);
    if (rv != 0) {
        return rv;
    }
    t->listener.data = t;
    return 0;
}

/* Implementation of raft_uv_transport->close. */
static void uvTcpClose(struct raft_uv_transport *transport,
                       raft_uv_transport_close_cb cb)
{
    struct UvTcp *t = transport->impl;
    assert(!t->closing);
    t->closing = true;
    t->close_cb = cb;
    UvTcpListenClose(t);
    UvTcpConnectClose(t);
}

void UvTcpMaybeFireCloseCb(struct UvTcp *t)
{
    if (!t->closing) {
        return;
    }
    if (t->listener.data != NULL) {
        return;
    }
    if (!QUEUE_IS_EMPTY(&t->connect_reqs)) {
        return;
    }
    if (!QUEUE_IS_EMPTY(&t->accept_conns)) {
        return;
    }
    if (t->close_cb != NULL) {
        t->close_cb(t->transport);
    }
}

int raft_uv_tcp_init(struct raft_uv_transport *transport,
                     struct uv_loop_s *loop)
{
    struct UvTcp *t;
    memset(transport, 0, sizeof *transport);
    t = raft_malloc(sizeof *t);
    if (t == NULL) {
        ErrMsgPrintf(transport->errmsg, "out of memory");
        return RAFT_NOMEM;
    }
    t->transport = transport;
    t->loop = loop;
    t->id = 0;
    t->address = NULL;
    t->listener.data = NULL;
    t->accept_cb = NULL;
    QUEUE_INIT(&t->accept_conns);
    QUEUE_INIT(&t->connect_reqs);
    t->closing = false;
    t->close_cb = NULL;

    transport->impl = t;
    transport->init = uvTcpInit;
    transport->close = uvTcpClose;
    transport->listen = UvTcpListen;
    transport->connect = UvTcpConnect;

    return 0;
}

void raft_uv_tcp_close(struct raft_uv_transport *transport)
{
    struct UvTcp *t = transport->impl;
    raft_free(t);
}
