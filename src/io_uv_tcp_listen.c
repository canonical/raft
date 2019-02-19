#include <string.h>

#include "assert.h"
#include "binary.h"
#include "io_uv_ip.h"
#include "io_uv_tcp_listen.h"

/* Hold state for a connection being accepted. */
struct raft__io_uv_tcp_accept
{
    struct raft__io_uv_tcp *tcp;

    struct
    {
        uint64_t preamble[3]; /* Preamble buffer */
        uv_buf_t address;     /* Address buffer */
        size_t nread;         /* Number of bytes read */
    } handshake;

    struct uv_tcp_s *stream; /* Connection */
    raft__queue queue;
};

/* Called when there's a new incoming connection. */
static void raft__io_uv_tcp_listen_cb(uv_stream_t *server, int status);

/* Called when the listener handle has been closed */
static void raft__io_uv_tcp_listen_close_cb(uv_handle_t *handle);

/* Start accepting a new incoming connection. */
int raft__io_uv_tcp_accept_start(struct raft__io_uv_tcp_accept *req,
                                 struct raft__io_uv_tcp *tcp,
                                 struct uv_stream_s *server);

/* Read the preamble of the handshake. */
static void raft__io_uv_tcp_accept_preamble_alloc_cb(uv_handle_t *handle,
                                                     size_t suggested_size,
                                                     uv_buf_t *buf);
static void raft__io_uv_tcp_accept_preamble_read_cb(uv_stream_t *stream,
                                                    ssize_t nread,
                                                    const uv_buf_t *buf);

/* Read the address part of the handshake. */
static void raft__io_uv_tcp_accept_address_alloc_cb(uv_handle_t *handle,
                                                    size_t suggested_size,
                                                    uv_buf_t *buf);
static void raft__io_uv_tcp_accept_address_read_cb(uv_stream_t *stream,
                                                   ssize_t nread,
                                                   const uv_buf_t *buf);

/* Close the accepted TCP connection. Can be called at any time after start. */
static void raft__io_uv_tcp_accept_cancel(struct raft__io_uv_tcp_accept *req);

/* The accepted TCP client connection gets closed. Fire the accept callback. */
static void raft__io_uv_tcp_accept_close_cb(struct uv_handle_s *handle);

int raft__io_uv_tcp_listen(struct raft_io_uv_transport *t,
                           raft_io_uv_accept_cb cb)
{
    struct raft__io_uv_tcp *tcp;
    struct sockaddr_in addr;
    int rv;

    tcp = t->impl;

    tcp->accept_cb = cb;

    rv = raft__io_uv_ip_parse(tcp->address, &addr);
    if (rv != 0) {
        return rv;
    }

    rv = uv_tcp_init(tcp->loop, &tcp->listener);
    assert(rv == 0);

    rv = uv_tcp_bind(&tcp->listener, (const struct sockaddr *)&addr, 0);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        raft_warnf(tcp->logger, "uv_tcp_bind: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    rv = uv_listen((uv_stream_t *)&tcp->listener, 1, raft__io_uv_tcp_listen_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        raft_warnf(tcp->logger, "uv_tcp_listen: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }
    tcp->listening = true;

    return 0;
}

void raft__io_uv_tcp_stop(struct raft_io_uv_transport *t)
{
    struct raft__io_uv_tcp *tcp;

    tcp = t->impl;

    /* Abort all connections currently being accepted */
    while (!RAFT__QUEUE_IS_EMPTY(&tcp->accept_queue)) {
        raft__queue *head;
        struct raft__io_uv_tcp_accept *req;

        head = RAFT__QUEUE_HEAD(&tcp->accept_queue);

        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_tcp_accept, queue);
        raft__io_uv_tcp_accept_cancel(req);
    }

    uv_close((struct uv_handle_s *)&tcp->listener,
             raft__io_uv_tcp_listen_close_cb);
}

static void raft__io_uv_tcp_listen_cb(uv_stream_t *server, int status)
{
    struct raft__io_uv_tcp *tcp = server->data;
    struct raft__io_uv_tcp_accept *req;
    int rv;

    assert(tcp->state == RAFT__IO_UV_TCP_ACTIVE);

    if (status < 0) {
        raft_warnf(tcp->logger, "uv_connection_cb: %s", uv_strerror(status));
        rv = RAFT_ERR_IO;
        goto err;
    }

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    rv = raft__io_uv_tcp_accept_start(req, tcp, server);
    if (rv != 0) {
        goto err_after_accept_alloc;
    }

    RAFT__QUEUE_PUSH(&tcp->accept_queue, &req->queue);

    return;

err_after_accept_alloc:
    raft_free(req);

err:
    assert(rv != 0);
}

static void raft__io_uv_tcp_listen_close_cb(uv_handle_t *handle)
{
    struct raft__io_uv_tcp *tcp = handle->data;

    tcp->listening = false;

    raft__io_uv_tcp_continue(tcp);
}

int raft__io_uv_tcp_accept_start(struct raft__io_uv_tcp_accept *req,
                                 struct raft__io_uv_tcp *tcp,
                                 struct uv_stream_s *server)
{
    int rv;

    req->tcp = tcp;

    memset(&req->handshake, 0, sizeof req->handshake);

    req->stream = raft_malloc(sizeof *req->stream);
    if (req->stream == NULL) {
        return RAFT_ERR_NOMEM;
    }

    req->stream->data = req;

    rv = uv_tcp_init(tcp->loop, req->stream);
    assert(rv == 0);

    rv = uv_accept(server, (uv_stream_t *)req->stream);
    if (rv != 0) {
        raft_warnf(req->tcp->logger, "uv_accept: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_client_init;
    }

    rv = uv_read_start((uv_stream_t *)req->stream,
                       raft__io_uv_tcp_accept_preamble_alloc_cb,
                       raft__io_uv_tcp_accept_preamble_read_cb);
    assert(rv == 0);

    return 0;

err_after_client_init:
    uv_close((uv_handle_t *)req->stream, (uv_close_cb)raft_free);

    return rv;
}

static void raft__io_uv_tcp_accept_preamble_alloc_cb(uv_handle_t *handle,
                                                     size_t suggested_size,
                                                     uv_buf_t *buf)
{
    struct raft__io_uv_tcp_accept *req = handle->data;

    (void)suggested_size;

    buf->base = (void *)req->handshake.preamble + req->handshake.nread;
    buf->len = sizeof req->handshake.preamble - req->handshake.nread;
}

static void raft__io_uv_tcp_accept_preamble_read_cb(uv_stream_t *stream,
                                                    ssize_t nread,
                                                    const uv_buf_t *buf)
{
    struct raft__io_uv_tcp_accept *req = stream->data;
    uint64_t protocol;
    size_t n;
    int rv;

    (void)buf;

    if (nread == 0) {
        /* Empty read just ignore it. */
        return;
    }

    if (nread < 0) {
        raft_warnf(req->tcp->logger, "tcp read: %s", uv_strerror(nread));
        raft__io_uv_tcp_accept_cancel(req);
        return;
    }

    n = nread;

    /* We shouldn't have read more data than the pending amount. */
    assert(n <= sizeof req->handshake.preamble - req->handshake.nread);

    /* Advance the read window */
    req->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (req->handshake.nread < sizeof req->handshake.preamble) {
        return;
    }

    /* If we have completed reading the preamble, let's parse it. */
    protocol = raft__flip64(req->handshake.preamble[0]);
    if (protocol != RAFT__IO_UV_TCP_PROTOCOL) {
        raft__io_uv_tcp_accept_cancel(req);
        return;
    }

    req->handshake.address.len = raft__flip64(req->handshake.preamble[2]);
    req->handshake.address.base = raft_malloc(req->handshake.address.len);

    if (req->handshake.address.base == NULL) {
        raft__io_uv_tcp_accept_cancel(req);
        return;
    }
    req->handshake.nread = 0;

    rv = uv_read_stop(stream);
    assert(rv == 0);

    rv = uv_read_start((uv_stream_t *)req->stream,
                       raft__io_uv_tcp_accept_address_alloc_cb,
                       raft__io_uv_tcp_accept_address_read_cb);
    assert(rv == 0);
}

static void raft__io_uv_tcp_accept_address_alloc_cb(uv_handle_t *handle,
                                                    size_t suggested_size,
                                                    uv_buf_t *buf)
{
    struct raft__io_uv_tcp_accept *req = handle->data;

    (void)suggested_size;

    buf->base = req->handshake.address.base + req->handshake.nread;
    buf->len = req->handshake.address.len - req->handshake.nread;
}

static void raft__io_uv_tcp_accept_address_read_cb(uv_stream_t *stream,
                                                   ssize_t nread,
                                                   const uv_buf_t *buf)
{
    struct raft__io_uv_tcp_accept *req = stream->data;
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
        raft_warnf(req->tcp->logger, "tcp read: %s", uv_strerror(nread));
        raft__io_uv_tcp_accept_cancel(req);
        return;
    }

    n = nread;

    /* We shouldn't have read more data than the pending amount. */
    assert(n <= req->handshake.address.len - req->handshake.nread);

    /* Advance the read window */
    req->handshake.nread += n;

    /* If there's more data to read in order to fill the current
     * read buffer, just return, we'll be invoked again. */
    if (req->handshake.nread < req->handshake.address.len) {
        return;
    }

    /* If we have completed reading the address, let's fire the callback. */
    rv = uv_read_stop(stream);
    assert(rv == 0);

    id = raft__flip64(req->handshake.preamble[1]);
    address = req->handshake.address.base;

    assert(req->tcp->state == RAFT__IO_UV_TCP_ACTIVE);

    RAFT__QUEUE_REMOVE(&req->queue);

    req->tcp->accept_cb(req->tcp->transport, id, address,
                        (struct uv_stream_s *)req->stream);
    raft_free(req->handshake.address.base);
    raft_free(req);
}

static void raft__io_uv_tcp_accept_cancel(struct raft__io_uv_tcp_accept *req)
{
    RAFT__QUEUE_REMOVE(&req->queue);

    uv_close((uv_handle_t *)req->stream, raft__io_uv_tcp_accept_close_cb);
}

static void raft__io_uv_tcp_accept_close_cb(struct uv_handle_s *handle)
{
    struct raft__io_uv_tcp_accept *req = handle->data;

    if (req->handshake.address.base != NULL) {
        raft_free(req->handshake.address.base);
    }

    raft_free(req->stream);
    raft_free(req);
}
