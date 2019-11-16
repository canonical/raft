#include <string.h>

#include "assert.h"
#include "byte.h"
#include "err.h"
#include "heap.h"
#include "uv_ip.h"
#include "uv_tcp.h"

/* The happy path of a connection request is:
 *
 * - Create a TCP handle and submit a connect request.
 *
 * - Once connected, submit a write request for the handshake.
 *
 * - Once the write completes, fire the request callback.
 *
 * Possible failure modes are:
 *
 * - The transport get closed, fire RAFT_CANCELED.
 *
 * - The either the TCP connect() or the write() request fails: the connect or
 *   write callback sets the status attribute accordingly and closes the TCP
 *   handle.
 *
 * In the TCP handle close callback will fire the request callback with the
 * request error status.
 */

/* Hold state for a single connection request. */
struct uvTcpConnect
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

/* Encode an handshake message into the given buffer. */
static int uvTcpConnectEncodeHandshake(unsigned id,
                                       const char *address,
                                       uv_buf_t *buf)
{
    void *cursor;
    size_t address_len = bytePad64(strlen(address) + 1);
    buf->len = sizeof(uint64_t) + /* Protocol version. */
               sizeof(uint64_t) + /* Server ID. */
               sizeof(uint64_t) /* Size of the address buffer */;
    buf->len += address_len;
    buf->base = HeapMalloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_NOMEM;
    }
    cursor = buf->base;
    bytePut64(&cursor, UV__TCP_HANDSHAKE_PROTOCOL);
    bytePut64(&cursor, id);
    bytePut64(&cursor, address_len);
    strcpy(cursor, address);
    return 0;
}

static void uvTcpConnectFinish(struct uvTcpConnect *r)
{
    struct UvTcp *t = r->t;
    QUEUE_REMOVE(&r->queue);
    r->req->cb(r->req, (struct uv_stream_s *)r->tcp, r->status);
    raft_free(r);
    UvTcpMaybeFireCloseCb(t);
}

/* The TCP connection handle has been closed in consequence of an error or
 * because the transport is closing. */
static void uvTcpConnectCloseCb(struct uv_handle_s *handle)
{
    struct uvTcpConnect *r = handle->data;
    /* We must be careful to not reference the r->t field of the connect request
     * object, since that uvTcp object might have been released in the
     * meantime. */
    assert((struct uv_tcp_s *)handle == r->tcp);
    assert(r->status != 0);
    HeapFree(handle);
    r->tcp = NULL;
    uvTcpConnectFinish(r);
}

/* Abort a connection request. */
void uvTcpConnectClose(struct uvTcpConnect *r)
{
    uv_close((struct uv_handle_s *)r->tcp, uvTcpConnectCloseCb);
}

/* The handshake TCP write completes. Fire the connect callback. */
static void uvTcpConnectHandshakeWriteCb(struct uv_write_s *write, int status)
{
    struct uvTcpConnect *r = write->data;
    struct UvTcp *t = r->t;

    /* We don't need the handshake buffer anymore. */
    HeapFree(r->handshake.base);

    if (t->closing || status == UV_ECANCELED) {
        r->status = RAFT_CANCELED;
        return;
    }

    if (status != 0) {
        r->status = RAFT_NOCONNECTION;
        uvTcpConnectClose(r);
        return;
    }

    uvTcpConnectFinish(r);
}

/* The TCP connection is established. Write the handshake data. */
static void uvTcpConnectCb(struct uv_connect_s *connect, int status)
{
    struct uvTcpConnect *r = connect->data;
    struct UvTcp *t = r->t;
    int rv;

    if (t->closing || status == UV_ECANCELED) {
        r->status = RAFT_CANCELED;
        return;
    }

    if (status != 0) {
        rv = RAFT_NOCONNECTION;
        ErrMsgPrintf(t->transport->errmsg, "uv_tcp_connect(): %s",
                     uv_strerror(status));
        goto err;
    }

    /* Initialize the handshake buffer and write it out. */
    rv = uvTcpConnectEncodeHandshake(r->t->id, r->t->address, &r->handshake);
    if (rv != 0) {
        assert(rv == RAFT_NOMEM);
        ErrMsgPrintf(r->t->transport->errmsg, "out of memory");
        goto err;
    }
    rv = uv_write(&r->write, (struct uv_stream_s *)r->tcp, &r->handshake, 1,
                  uvTcpConnectHandshakeWriteCb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        rv = RAFT_IOERR;
        goto err_after_encodeHandshake;
    }
    r->write.data = r;

    return;

err_after_encodeHandshake:
    HeapFree(r->handshake.base);
err:
    r->status = rv;
    uvTcpConnectClose(r);
}

/* Create a new TCP handle and submit a connection request to the event loop. */
static int uvTcpStartConnecting(struct uvTcpConnect *r, const char *address)
{
    struct UvTcp *t = r->t;
    struct sockaddr_in addr;
    int rv;

    r->tcp = HeapMalloc(sizeof *r->tcp);
    if (r->tcp == NULL) {
        ErrMsgPrintf(t->transport->errmsg, "out of memory");
        rv = RAFT_NOMEM;
        goto err;
    }
    r->handshake.base = NULL;

    rv = uv_tcp_init(r->t->loop, r->tcp);
    assert(rv == 0);
    r->tcp->data = r;

    rv = uvIpParse(address, &addr);
    if (rv != 0) {
        goto err_after_tcp_init;
    }

    rv = uv_tcp_connect(&r->connect, r->tcp, (struct sockaddr *)&addr,
                        uvTcpConnectCb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        ErrMsgPrintf(t->transport->errmsg, "uv_tcp_connect(): %s",
                     uv_strerror(rv));
        rv = RAFT_NOCONNECTION;
        goto err_after_tcp_init;
    }
    r->connect.data = r;

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)r->tcp, (uv_close_cb)HeapFree);
err:
    return rv;
}

int UvTcpConnect(struct raft_uv_transport *transport,
                 struct raft_uv_connect *req,
                 unsigned id,
                 const char *address,
                 raft_uv_connect_cb cb)
{
    struct UvTcp *t = transport->impl;
    struct uvTcpConnect *r;
    int rv;
    (void)id;
    assert(!t->closing);

    /* Create and initialize a new TCP connection request object */
    r = HeapMalloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_NOMEM;
        ErrMsgPrintf(transport->errmsg, "out of memory");
        goto err;
    }
    r->t = t;
    r->req = req;
    r->status = 0;

    req->cb = cb;

    /* Keep track of the pending request */
    QUEUE_PUSH(&t->connect_reqs, &r->queue);

    /* Start connecting */
    rv = uvTcpStartConnecting(r, address);
    if (rv != 0) {
        goto err_after_alloc;
    }

    return 0;

err_after_alloc:
    QUEUE_REMOVE(&r->queue);
    HeapFree(r);
err:
    return rv;
}

void UvTcpConnectStop(struct UvTcp *t)
{
    queue *head;
    QUEUE_FOREACH(head, &t->connect_reqs)
    {
        struct uvTcpConnect *req;
        head = QUEUE_HEAD(&t->connect_reqs);
        req = QUEUE_DATA(head, struct uvTcpConnect, queue);
        uvTcpConnectClose(req);
    }
}
