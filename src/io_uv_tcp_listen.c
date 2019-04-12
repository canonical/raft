#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv_ip.h"
#include "io_uv_tcp.h"

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
    struct io_uv__tcp *t;       /* Transport implementation */
    struct uv_tcp_s *tcp;       /* TCP connection socket handle */
    struct handshake handshake; /* Handshake data */
    raft__queue queue;          /* Pending accept queue */
};

/* Read the preamble of the handshake. */
static void preamble_alloc_cb(struct uv_handle_s *handle,
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
static int decode_preamble(struct handshake *h)
{
    uint64_t protocol;
    protocol = byte__flip64(h->preamble[0]);
    if (protocol != TCP_TRANSPORT__HANDSHAKE_PROTOCOL) {
        return RAFT_ERR_IO_MALFORMED;
    }
    h->address.len = byte__flip64(h->preamble[2]);
    h->address.base = raft_malloc(h->address.len);
    if (h->address.base == NULL) {
        return RAFT_ENOMEM;
    }
    h->nread = 0;
    return 0;
}

/* The accepted TCP client connection has been closed, release all memory
 * associated with accept object. We can get here only if an error occurrent
 * during the handshake or if raft_io_transport->close() has been invoked. */
static void close_cb(struct uv_handle_s *handle)
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
static void conn_stop(struct conn *c)
{
    RAFT__QUEUE_REMOVE(&c->queue);
    /* After uv_close() returns we are guaranteed that no more alloc_cb or
     * read_cb will be called. */
    uv_close((struct uv_handle_s *)c->tcp, close_cb);
}

/* Read the address part of the handshake. */
static void address_alloc_cb(struct uv_handle_s *handle,
                             size_t suggested_size,
                             uv_buf_t *buf)
{
    struct conn *c = handle->data;
    (void)suggested_size;
    buf->base = c->handshake.address.base + c->handshake.nread;
    buf->len = c->handshake.address.len - c->handshake.nread;
}

static void address_read_cb(uv_stream_t *stream,
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
        conn_stop(c);
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
    id = byte__flip64(c->handshake.preamble[1]);
    address = c->handshake.address.base;
    RAFT__QUEUE_REMOVE(&c->queue);
    c->t->accept_cb(c->t->transport, id, address, (struct uv_stream_s *)c->tcp);
    raft_free(c->handshake.address.base);
    raft_free(c);
}

static void preamble_read_cb(uv_stream_t *stream,
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
        conn_stop(c);
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
    rv = decode_preamble(&c->handshake);
    if (rv != 0) {
        conn_stop(c);
        return;
    }

    rv = uv_read_stop(stream);
    assert(rv == 0);
    rv =
        uv_read_start((uv_stream_t *)c->tcp, address_alloc_cb, address_read_cb);
    assert(rv == 0);
}

/* Start reading handshake data for a new incoming connection. */
static int conn_start_handshake(struct conn *c)
{
    int rv;

    memset(&c->handshake, 0, sizeof c->handshake);

    c->tcp = raft_malloc(sizeof *c->tcp);
    if (c->tcp == NULL) {
        return RAFT_ENOMEM;
    }
    c->tcp->data = c;
    rv = uv_tcp_init(c->t->loop, c->tcp);
    assert(rv == 0);

    rv = uv_accept((struct uv_stream_s *)&c->t->listener,
                   (struct uv_stream_s *)c->tcp);
    if (rv != 0) {
        rv = RAFT_ERR_IO;
        goto err_after_client_init;
    }

    rv = uv_read_start((uv_stream_t *)c->tcp, preamble_alloc_cb,
                       preamble_read_cb);
    assert(rv == 0);

    return 0;

err_after_client_init:
    uv_close((uv_handle_t *)c->tcp, (uv_close_cb)raft_free);

    return rv;
}

/* Called when there's a new incoming connection: create a new tcp_accept object
 * and start receiving handshake data. */
static void listen_cb(struct uv_stream_s *stream, int status)
{
    struct io_uv__tcp *t = stream->data;
    struct conn *c;
    int rv;

    assert(stream == (struct uv_stream_s *)&t->listener);

    if (status < 0) {
        rv = RAFT_ERR_IO;
        goto err;
    }

    c = raft_malloc(sizeof *c);
    if (c == NULL) {
        rv = RAFT_ENOMEM;
        goto err;
    }
    c->t = t;

    rv = conn_start_handshake(c);
    if (rv != 0) {
        goto err_after_accept_alloc;
    }

    RAFT__QUEUE_PUSH(&t->accept_conns, &c->queue);

    return;

err_after_accept_alloc:
    raft_free(c);

err:
    assert(rv != 0);
}

int io_uv__tcp_listen(struct raft_uv_transport *transport,
                      raft_uv_accept_cb cb)
{
    struct io_uv__tcp *t;
    struct sockaddr_in addr;
    int rv;

    t = transport->impl;
    t->accept_cb = cb;

    rv = raft__io_uv_ip_parse(t->address, &addr);
    if (rv != 0) {
        return rv;
    }
    rv = uv_tcp_bind(&t->listener, (const struct sockaddr *)&addr, 0);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        return RAFT_ERR_IO;
    }
    rv = uv_listen((uv_stream_t *)&t->listener, 1, listen_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        return RAFT_ERR_IO;
    }

    return 0;
}

void io_uv__tcp_listen_stop(struct io_uv__tcp *t)
{
    /* Abort all connections currently being accepted */
    while (!RAFT__QUEUE_IS_EMPTY(&t->accept_conns)) {
        raft__queue *head;
        struct conn *r;
        head = RAFT__QUEUE_HEAD(&t->accept_conns);
        r = RAFT__QUEUE_DATA(head, struct conn, queue);
        conn_stop(r);
    }
}
