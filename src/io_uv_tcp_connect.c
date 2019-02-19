#include <string.h>

#include "assert.h"
#include "binary.h"
#include "io_uv_ip.h"
#include "io_uv_tcp_connect.h"

/* Hold state for a single connection attempt. */
struct raft__io_uv_tcp_connect
{
    struct raft__io_uv_tcp *tcp;
    struct raft_io_uv_connect *req;

    int status;         /* Current status code */
    uv_buf_t handshake; /* Handshake data */

    struct uv_tcp_s *stream;
    struct uv_connect_s connect;
    struct uv_write_s write;
};

/* Start a connection attempt */
static int raft__io_uv_tcp_connect_start(struct raft__io_uv_tcp *tcp,
                                         struct raft__io_uv_tcp_connect *req,
                                         const char *address);

/* The TCP connection is established. Write the handshake data. */
static void raft__io_uv_tcp_connect_connect_cb(struct uv_connect_s *req,
                                               int status);

/* The handshake TCP write completes. Fire the connect callback. */
static void raft__io_uv_tcp_connect_write_cb(struct uv_write_s *req,
                                             int status);

/* Implementation of raft_io_uv_connect->cancel(). */
static void raft__io_uv_tcp_connect_cancel(struct raft_io_uv_connect *req);

/* The TCP connection gets closed after cancel(). Fire the connect callback. */
static void raft__io_uv_tcp_connect_close_cb(struct uv_handle_s *handle);

/* Encode an handshake into the given buffer. */
static int raft__io_uv_tcp_connect_encode_handshake(unsigned id,
                                                    const char *address,
                                                    uv_buf_t *buf);

int raft__io_uv_tcp_connect(struct raft_io_uv_transport *t,
                            struct raft_io_uv_connect *connect,
                            unsigned id,
                            const char *address,
                            raft_io_uv_connect_cb cb)
{
    struct raft__io_uv_tcp *tcp;
    struct raft__io_uv_tcp_connect *req;
    int rv;

    (void)id;

    tcp = t->impl;

    assert(tcp->state == RAFT__IO_UV_TCP_ACTIVE);

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        return RAFT_ERR_NOMEM;
    }
    req->req = connect;

    connect->cb = cb;
    connect->impl = req;
    connect->cancel = raft__io_uv_tcp_connect_cancel;

    rv = raft__io_uv_tcp_connect_start(tcp, req, address);
    if (rv != 0) {
        raft_free(req);
        return rv;
    }

    return 0;
}

static int raft__io_uv_tcp_connect_start(struct raft__io_uv_tcp *tcp,
                                         struct raft__io_uv_tcp_connect *req,
                                         const char *address)
{
    struct sockaddr_in addr;
    int rv;

    req->tcp = tcp;
    req->stream = raft_malloc(sizeof *req->stream);
    if (req->stream == NULL) {
        return RAFT_ERR_NOMEM;
    }

    req->handshake.base = NULL;

    rv = uv_tcp_init(tcp->loop, req->stream);
    assert(rv == 0);
    req->stream->data = req;

    rv = raft__io_uv_ip_parse(address, &addr);
    if (rv != 0) {
        goto err_after_tcp_init;
    }

    rv = uv_tcp_connect(&req->connect, req->stream, (struct sockaddr *)&addr,
                        raft__io_uv_tcp_connect_connect_cb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        raft_warnf(req->tcp->logger, "connect: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO_CONNECT;
        goto err_after_tcp_init;
    }
    req->connect.data = req;

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)req->stream, (uv_close_cb)raft_free);

    return rv;
}

static void raft__io_uv_tcp_connect_cancel(struct raft_io_uv_connect *connect)
{
    struct raft__io_uv_tcp_connect *req = connect->impl;

    req->status = RAFT_ERR_IO_CANCELED;

    uv_close((struct uv_handle_s *)req->stream,
             raft__io_uv_tcp_connect_close_cb);
}

static void raft__io_uv_tcp_connect_connect_cb(struct uv_connect_s *connect,
                                               int status)
{
    struct raft__io_uv_tcp_connect *req = connect->data;
    int rv;

    /* If we were canceled via raft__io_uv_tcp_connect_cancel(), let's bail out,
     * the close callback will eventually fire. */
    if (req->status == RAFT_ERR_IO_CANCELED) {
        return;
    }

    if (status != 0) {
        raft_warnf(req->tcp->logger, "connect: %s", uv_strerror(status));
        rv = RAFT_ERR_IO_CONNECT;
        goto err;
    }

    /* Initialize the handshake buffer. */
    rv = raft__io_uv_tcp_connect_encode_handshake(
        req->tcp->id, req->tcp->address, &req->handshake);
    if (rv != 0) {
        goto err;
    }

    rv = uv_write(&req->write, (struct uv_stream_s *)req->stream,
                  &req->handshake, 1, raft__io_uv_tcp_connect_write_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        raft_warnf(req->tcp->logger, "write: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_encode_handshake;
    }
    req->write.data = req;

    return;

err_after_encode_handshake:
    raft_free(req->handshake.base);

err:
    req->status = rv;

    uv_close((struct uv_handle_s *)req->stream,
             raft__io_uv_tcp_connect_close_cb);
}

static void raft__io_uv_tcp_connect_write_cb(struct uv_write_s *write,
                                             int status)
{
    struct raft__io_uv_tcp_connect *req = write->data;
    int rv;

    raft_free(req->handshake.base);

    /* If we were canceled via raft__io_uv_tcp_connect_cancel(), let's bail out,
     * the close callback will eventually fire. */
    if (req->status == RAFT_ERR_IO_CANCELED) {
        return;
    }

    if (status != 0) {
        raft_warnf(req->tcp->logger, "write: %s", uv_strerror(status));
        rv = RAFT_ERR_IO_CONNECT;
        goto err;
    }

    assert(req->tcp->state == RAFT__IO_UV_TCP_ACTIVE);

    req->req->cb(req->req, (struct uv_stream_s *)req->stream, 0);

    raft_free(req);

    return;

err:
    req->status = rv;

    uv_close((struct uv_handle_s *)req->stream,
             raft__io_uv_tcp_connect_close_cb);
}

static void raft__io_uv_tcp_connect_close_cb(struct uv_handle_s *handle)
{
    struct raft__io_uv_tcp_connect *req = handle->data;

    assert((struct uv_tcp_s *)handle == req->stream);

    req->req->cb(req->req, NULL, req->status);

    raft_free(req->stream);
    raft_free(req);
}

static int raft__io_uv_tcp_connect_encode_handshake(unsigned id,
                                                    const char *address,
                                                    uv_buf_t *buf)
{
    void *cursor;
    size_t address_len = raft__pad64(strlen(address) + 1);

    buf->len = sizeof(uint64_t) + /* Protocol version. */
               sizeof(uint64_t) + /* Server ID. */
               sizeof(uint64_t) /* Size of the address buffer */;

    buf->len += address_len;

    buf->base = raft_malloc(buf->len);
    if (buf->base == NULL) {
        return RAFT_ERR_NOMEM;
    }

    cursor = buf->base;

    raft__put64(&cursor, RAFT__IO_UV_TCP_PROTOCOL);
    raft__put64(&cursor, id);
    raft__put64(&cursor, address_len);

    strcpy(cursor, address);

    return 0;
}
