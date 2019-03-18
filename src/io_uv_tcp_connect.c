#include <string.h>

#include "../include/raft/io_uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv_ip.h"
#include "io_uv_tcp.h"

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
 *   RAFT_ERR_IO_CANCELED and the TCP handle gets closed.
 *
 * - The either the TCP connect() or the write() request fails: the connect or
 *   write callback sets the status attribute accordingly and closes the TCP
 *   handle.
 *
 * In the TCP handle close callback will fire the request callback with the
 * request error status.
 */

/* Hold state for a single connection request. */
struct connect
{
    struct io_uv__tcp *t;           /* Transport implementation */
    struct raft_io_uv_connect *req; /* User request */
    uv_buf_t handshake;             /* Handshake data */
    struct uv_tcp_s *tcp;           /* TCP connection socket handle */
    struct uv_connect_s connect;    /* TCP connectionr request */
    struct uv_write_s write;        /* TCP handshake request */
    int status;                     /* Returned to the request callback */
    raft__queue queue;              /* Pending connect queue */
};

/* Encode an handshake message into the given buffer. */
static int encode_handshake(unsigned id, const char *address, uv_buf_t *buf)
{
    void *cursor;
    size_t address_len = byte__pad64(strlen(address) + 1);

    buf->len = sizeof(uint64_t) + /* Protocol version. */
               sizeof(uint64_t) + /* Server ID. */
               sizeof(uint64_t) /* Size of the address buffer */;
    buf->len += address_len;
    buf->base = raft_malloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_ENOMEM;
    }

    cursor = buf->base;

    byte__put64(&cursor, TCP_TRANSPORT__HANDSHAKE_PROTOCOL);
    byte__put64(&cursor, id);
    byte__put64(&cursor, address_len);
    strcpy(cursor, address);

    return 0;
}

/* The TCP connection handle has been closed in consequence of an error or
 * because the transport is closing. Fire the connect callback. */
static void close_cb(struct uv_handle_s *handle)
{
    struct connect *r = handle->data;
    /* We must be careful to not reference the r->t field of the connect request
     * object, since that io_uv__tcp object might have been released in the
     * meantime. */
    assert((struct uv_tcp_s *)handle == r->tcp);
    assert(r->status != 0);

    r->req->cb(r->req, NULL, r->status);

    raft_free(handle);
    raft_free(r);
}

/* The handshake TCP write completes. Fire the connect callback. */
static void write_cb(struct uv_write_s *write, int status)
{
    struct connect *r = write->data;
    int rv;

    /* We don't need the handshake buffer anymore. */
    raft_free(r->handshake.base);

    /* If we were canceled via transport->close(), let's bail out, the handle
     * close callback will eventually fire and we'll invoke the callback. */
    if (r->status == RAFT_ERR_IO_CANCELED) {
        return;
    }

    /* Regardless of whether we succeeded or not, the request has completed, so
     * we can remove it from the queue. */
    RAFT__QUEUE_REMOVE(&r->queue);

    if (status != 0) {
        rv = RAFT_ERR_IO_CONNECT;
        goto err;
    }

    r->req->cb(r->req, (struct uv_stream_s *)r->tcp, 0);
    raft_free(r);

    return;

err:
    r->status = rv;
    uv_close((struct uv_handle_s *)r->tcp, close_cb);
}

/* The TCP connection is established. Write the handshake data. */
static void connect_cb(struct uv_connect_s *connect, int status)
{
    struct connect *r = connect->data;
    int rv;

    /* If we were canceled via tcp_close(), let's bail out, the close callback
     * will eventually fire. */
    if (r->status == RAFT_ERR_IO_CANCELED) {
        return;
    }

    if (status != 0) {
        rv = RAFT_ERR_IO_CONNECT;
        goto err;
    }

    /* Initialize the handshake buffer and write it out. */
    rv = encode_handshake(r->t->id, r->t->address, &r->handshake);
    if (rv != 0) {
        goto err;
    }
    rv = uv_write(&r->write, (struct uv_stream_s *)r->tcp, &r->handshake, 1,
                  write_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        rv = RAFT_ERR_IO;
        goto err_after_encode_handshake;
    }
    r->write.data = r;

    return;

err_after_encode_handshake:
    raft_free(r->handshake.base);

err:
    /* Remove the request from the queue, since we're aborting it */
    RAFT__QUEUE_REMOVE(&r->queue);
    r->status = rv;
    uv_close((struct uv_handle_s *)r->tcp, close_cb);
}

/* Create a new TCP handle and submit a connection request to the event loop. */
static int tcp_connect__start(struct connect *r, const char *address)
{
    struct sockaddr_in addr;
    int rv;

    r->tcp = raft_malloc(sizeof *r->tcp);
    if (r->tcp == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }
    r->handshake.base = NULL;

    rv = uv_tcp_init(r->t->loop, r->tcp);
    assert(rv == 0);
    r->tcp->data = r;

    rv = raft__io_uv_ip_parse(address, &addr);
    if (rv != 0) {
        goto err_after_tcp_init;
    }

    rv = uv_tcp_connect(&r->connect, r->tcp, (struct sockaddr *)&addr,
                        connect_cb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        rv = RAFT_ERR_IO_CONNECT;
        goto err_after_tcp_init;
    }
    r->connect.data = r;

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)r->tcp, (uv_close_cb)raft_free);
err:
    return rv;
}

int io_uv__tcp_connect(struct raft_io_uv_transport *transport,
                       struct raft_io_uv_connect *req,
                       unsigned id,
                       const char *address,
                       raft_io_uv_connect_cb cb)
{
    struct io_uv__tcp *t;
    struct connect *r;
    int rv;

    (void)id;

    t = transport->impl;

    /* Create and initialize a new TCP connection request object */
    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        return RAFT_ENOMEM;
    }
    r->t = t;
    r->req = req;
    r->status = 0;

    req->cb = cb;

    /* Start connecting */
    rv = tcp_connect__start(r, address);
    if (rv != 0) {
        raft_free(r);
        return rv;
    }

    /* Keep track of the pending request */
    RAFT__QUEUE_PUSH(&t->connect_reqs, &r->queue);

    return 0;
}

static void tcp_connect__cancel(struct connect *r)
{
    RAFT__QUEUE_REMOVE(&r->queue);
    r->status = RAFT_ERR_IO_CANCELED;
    uv_close((struct uv_handle_s *)r->tcp, close_cb);
}

void io_uv__tcp_connect_stop(struct io_uv__tcp *t)
{
    while (!RAFT__QUEUE_IS_EMPTY(&t->connect_reqs)) {
        raft__queue *head;
        struct connect *r;
        head = RAFT__QUEUE_HEAD(&t->connect_reqs);
        r = RAFT__QUEUE_DATA(head, struct connect, queue);
        tcp_connect__cancel(r);
    }
}
