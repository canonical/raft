#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
#include "uv_ip.h"
#include "uv_tcp.h"

/* The happy path of an incoming connection is:
 *
 * - The connection callback is fired on the listener TCP handle, and the
 *   incoming connection is uv_accept()'ed. We call uv_read_start() to get
 *   notified about received handshake data.
 *
 * - Once the preamble is received, we start waiting for the server address.
 *
 * - Once the server address is received, we fire the receive callback.
 *
 * Possible failure modes are:
 *
 * - The accept process gets canceled in the transport->close() implementation,
 *   by calling tcp_accept_stop(): the incoming TCP connection handle gets
 *   closed, preventing any further handshake data notification, and all
 *   allocated memory gets released in the handle close callack.
 */

/* Hold state for a connection being accepted. */
struct handshake
{
    uint64_t preamble[3]; /* Preamble buffer */
    uv_buf_t address;     /* Address buffer */
    size_t nread;         /* Number of bytes read */
};

/* Hold handshake data for a new connection being established. */
struct conn
{
    struct uvTcp *t;            /* Transport implementation */
    struct uv_tcp_s *tcp;       /* TCP connection socket handle */
    struct handshake handshake; /* Handshake data */
    queue queue;                /* Pending accept queue */
};

/* Read the preamble of the handshake. */
static void preambleAllocCb(struct uv_handle_s *handle,
                            size_t suggested_size,
                            uv_buf_t *buf)
{
    struct conn *c = handle->data;
    (void)suggested_size;
    buf->base = (void *)c->handshake.preamble + c->handshake.nread;
    buf->len = sizeof c->handshake.preamble - c->handshake.nread;
}

/* Decode the handshake preamble, containing the protocol version, the ID of the
 * connecting server and the length of its address. Also, allocate the buffer to
 * start reading the server address. */
static int decodePreamble(struct handshake *h)
{
    uint64_t protocol;
    protocol = byteFlip64(h->preamble[0]);
    if (protocol != UV__TCP_HANDSHAKE_PROTOCOL) {
        return RAFT_MALFORMED;
    }
    h->address.len = byteFlip64(h->preamble[2]);
    h->address.base = raft_malloc(h->address.len);
    if (h->address.base == NULL) {
        return RAFT_NOMEM;
    }
    h->nread = 0;
    return 0;
}

/* The accepted TCP client connection has been closed, release all memory
 * associated with accept object. We can get here only if an error occurrent
 * during the handshake or if raft_io_transport->close() has been invoked. */
static void closeCb(struct uv_handle_s *handle)
{
    struct conn *c = handle->data;
    /* We have to be careful to not use the c->t reference, since that
     * io_uv__tcp transport object might have been released at this point. */
    if (c->handshake.address.base != NULL) {
        raft_free(c->handshake.address.base);
    }
    raft_free(c->tcp);
    raft_free(c);
}

/* Close an incoming TCP connection. Can be called at any time after starting
 * the handshake. */
static void closeConn(struct conn *c)
{
    QUEUE_REMOVE(&c->queue);
    /* After uv_close() returns we are guaranteed that no more alloc_cb or
     * read_cb will be called. */
    uv_close((struct uv_handle_s *)c->tcp, closeCb);
}

/* Read the address part of the handshake. */
static void addressAllocCb(struct uv_handle_s *handle,
                           size_t suggested_size,
                           uv_buf_t *buf)
{
    struct conn *c = handle->data;
    (void)suggested_size;
    buf->base = c->handshake.address.base + c->handshake.nread;
    buf->len = c->handshake.address.len - c->handshake.nread;
}

static void addressReadCb(uv_stream_t *stream,
                          ssize_t nread,
                          const uv_buf_t *buf)
{
    struct conn *c = stream->data;
    char *address;
    unsigned id;
    size_t n;
    int rv;

    (void)buf;

    if (nread == 0) {
        /* Empty read just ignore it. */
        return;
    }
    if (nread < 0) {
        closeConn(c);
        return;
    }

    /* We shouldn't have read more data than the pending amount. */
    n = nread;
    assert(n <= c->handshake.address.len - c->handshake.nread);

    /* Advance the read window */
    c->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (c->handshake.nread < c->handshake.address.len) {
        return;
    }

    /* If we have completed reading the address, let's fire the callback. */
    rv = uv_read_stop(stream);
    assert(rv == 0);
    id = byteFlip64(c->handshake.preamble[1]);
    address = c->handshake.address.base;
    QUEUE_REMOVE(&c->queue);
    c->t->accept_cb(c->t->transport, id, address, (struct uv_stream_s *)c->tcp);
    raft_free(c->handshake.address.base);
    raft_free(c);
}

static void preambleReadCb(uv_stream_t *stream,
                           ssize_t nread,
                           const uv_buf_t *buf)
{
    struct conn *c = stream->data;
    size_t n;
    int rv;

    (void)buf;

    if (nread == 0) {
        /* Empty read just ignore it. */
        return;
    }
    if (nread < 0) {
        closeConn(c);
        return;
    }

    /* We shouldn't have read more data than the pending amount. */
    n = nread;
    assert(n <= sizeof c->handshake.preamble - c->handshake.nread);

    /* Advance the read window */
    c->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (c->handshake.nread < sizeof c->handshake.preamble) {
        return;
    }

    /* If we have completed reading the preamble, let's parse it. */
    rv = decodePreamble(&c->handshake);
    if (rv != 0) {
        closeConn(c);
        return;
    }

    rv = uv_read_stop(stream);
    assert(rv == 0);
    rv = uv_read_start((uv_stream_t *)c->tcp, addressAllocCb, addressReadCb);
    assert(rv == 0);
}

/* Start reading handshake data for a new incoming connection. */
static int readHandshake(struct conn *c)
{
    int rv;
    memset(&c->handshake, 0, sizeof c->handshake);

    c->tcp = raft_malloc(sizeof *c->tcp);
    if (c->tcp == NULL) {
        return RAFT_NOMEM;
    }
    c->tcp->data = c;
    rv = uv_tcp_init(c->t->loop, c->tcp);
    assert(rv == 0);

    rv = uv_accept((struct uv_stream_s *)&c->t->listener,
                   (struct uv_stream_s *)c->tcp);
    if (rv != 0) {
        rv = RAFT_IOERR;
        goto err_after_tcp_init;
    }
    rv = uv_read_start((uv_stream_t *)c->tcp, preambleAllocCb, preambleReadCb);
    assert(rv == 0);

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)c->tcp, (uv_close_cb)raft_free);
    return rv;
}

/* Called when there's a new incoming connection: create a new tcp_accept object
 * and start receiving handshake data. */
static void listenCb(struct uv_stream_s *stream, int status)
{
    struct uvTcp *t = stream->data;
    struct conn *c;
    int rv;
    assert(stream == (struct uv_stream_s *)&t->listener);

    if (status < 0) {
        rv = RAFT_IOERR;
        goto err;
    }

    c = raft_malloc(sizeof *c);
    if (c == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    c->t = t;

    rv = readHandshake(c);
    if (rv != 0) {
        goto err_after_accept_alloc;
    }

    QUEUE_PUSH(&t->accept_conns, &c->queue);
    return;

err_after_accept_alloc:
    raft_free(c);
err:
    assert(rv != 0);
}

int uvTcpListen(struct raft_uv_transport *transport, raft_uv_accept_cb cb)
{
    struct uvTcp *t;
    struct sockaddr_in addr;
    int rv;

    t = transport->impl;
    t->accept_cb = cb;

    rv = uvIpParse(t->address, &addr);
    if (rv != 0) {
        return rv;
    }
    rv = uv_tcp_bind(&t->listener, (const struct sockaddr *)&addr, 0);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        return RAFT_IOERR;
    }
    rv = uv_listen((uv_stream_t *)&t->listener, 1, listenCb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        return RAFT_IOERR;
    }

    return 0;
}

void uvTcpListenClose(struct uvTcp *t)
{
    /* Abort all connections currently being accepted */
    while (!QUEUE_IS_EMPTY(&t->accept_conns)) {
        queue *head;
        struct conn *r;
        head = QUEUE_HEAD(&t->accept_conns);
        r = QUEUE_DATA(head, struct conn, queue);
        closeConn(r);
    }
}
