#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
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
 * - The request gets canceled in the transport->close() implementation, by
 *   calling tcp_connect_stop(): the status attribute gets set to
 *   RAFT_CANCELED and the TCP handle gets closed.
 *
 * - The either the TCP connect() or the write() request fails: the connect or
 *   write callback sets the status attribute accordingly and closes the TCP
 *   handle.
 *
 * In the TCP handle close callback will fire the request callback with the
 * request error status.
 */

/* Encode an handshake message into the given buffer. */
static int encodeHandshake(unsigned id, const char *address, uv_buf_t *buf)
{
    void *cursor;
    size_t address_len = bytePad64(strlen(address) + 1);
    buf->len = sizeof(uint64_t) + /* Protocol version. */
               sizeof(uint64_t) + /* Server ID. */
               sizeof(uint64_t) /* Size of the address buffer */;
    buf->len += address_len;
    buf->base = raft_malloc(buf->len);
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

/* The TCP connection handle has been closed in consequence of an error or
 * because the transport is closing. Fire the connect callback. */
static void closeCb(struct uv_handle_s *handle)
{
    struct UvTcpConnect *r = handle->data;
    /* We must be careful to not reference the r->t field of the connect request
     * object, since that uvTcp object might have been released in the
     * meantime. */
    assert((struct uv_tcp_s *)handle == r->tcp);
    assert(r->status != 0);
    r->req->cb(r->req, NULL, r->status);
    raft_free(handle);
    raft_free(r);
}

/* The handshake TCP write completes. Fire the connect callback. */
static void writeCb(struct uv_write_s *write, int status)
{
    struct UvTcpConnect *r = write->data;
    int rv;

    /* We don't need the handshake buffer anymore. */
    raft_free(r->handshake.base);

    /* If we were canceled via transport->close(), let's bail out, the handle
     * close callback will eventually fire and we'll invoke the callback. */
    if (r->status == RAFT_CANCELED) {
        return;
    }

    /* Regardless of whether we succeeded or not, the request has completed, so
     * we can remove it from the queue. */
    QUEUE_REMOVE(&r->queue);

    if (status != 0) {
        rv = RAFT_NOCONNECTION;
        goto err;
    }

    r->req->cb(r->req, (struct uv_stream_s *)r->tcp, 0);
    raft_free(r);

    return;

err:
    r->status = rv;
    uv_close((struct uv_handle_s *)r->tcp, closeCb);
}

/* The TCP connection is established. Write the handshake data. */
static void connectCb(struct uv_connect_s *connect, int status)
{
    struct UvTcpConnect *r = connect->data;
    int rv;

    /* If we were canceled via tcp_close(), let's bail out, the close callback
     * will eventually fire. */
    if (r->status == RAFT_CANCELED) {
        return;
    }

    if (status != 0) {
        rv = RAFT_NOCONNECTION;
        goto err;
    }

    /* Initialize the handshake buffer and write it out. */
    rv = encodeHandshake(r->t->id, r->t->address, &r->handshake);
    if (rv != 0) {
        goto err;
    }
    rv = uv_write(&r->write, (struct uv_stream_s *)r->tcp, &r->handshake, 1,
                  writeCb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        rv = RAFT_IOERR;
        goto err_after_encodeHandshake;
    }
    r->write.data = r;

    return;

err_after_encodeHandshake:
    raft_free(r->handshake.base);

err:
    /* Remove the request from the queue, since we're aborting it */
    QUEUE_REMOVE(&r->queue);
    r->status = rv;
    uv_close((struct uv_handle_s *)r->tcp, closeCb);
}

/* Create a new TCP handle and submit a connection request to the event loop. */
static int startConnecting(struct UvTcpConnect *r, const char *address)
{
    struct sockaddr_in addr;
    int rv;

    r->tcp = raft_malloc(sizeof *r->tcp);
    if (r->tcp == NULL) {
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
                        connectCb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        rv = RAFT_NOCONNECTION;
        goto err_after_tcp_init;
    }
    r->connect.data = r;

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)r->tcp, (uv_close_cb)raft_free);
err:
    return rv;
}

int UvTcpConnect(struct raft_uv_transport *transport,
                 struct raft_uv_connect *req,
                 unsigned id,
                 const char *address,
                 raft_uv_connect_cb cb)
{
    struct UvTcp *t;
    struct UvTcpConnect *r;
    int rv;

    (void)id;

    t = transport->impl;

    /* Create and initialize a new TCP connection request object */
    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        return RAFT_NOMEM;
    }
    r->t = t;
    r->req = req;
    r->status = 0;

    req->cb = cb;

    /* Start connecting */
    rv = startConnecting(r, address);
    if (rv != 0) {
        raft_free(r);
        return rv;
    }

    /* Keep track of the pending request */
    QUEUE_PUSH(&t->connect_reqs, &r->queue);

    return 0;
}

/* Abort a connection request. */
static void abortConnection(struct UvTcpConnect *r)
{
    QUEUE_REMOVE(&r->queue);
    r->status = RAFT_CANCELED;
    uv_close((struct uv_handle_s *)r->tcp, closeCb);
}

void UvTcpConnectClose(struct UvTcp *t)
{
    while (!QUEUE_IS_EMPTY(&t->connect_reqs)) {
        queue *head;
        struct UvTcpConnect *r;
        head = QUEUE_HEAD(&t->connect_reqs);
        r = QUEUE_DATA(head, struct UvTcpConnect, queue);
        abortConnection(r);
    }
}
