#include <string.h>

#include "assert.h"
#include "binary.h"
#include "io_uv_encoding.h"
#include "io_uv_rpc_server.h"

/* Initialize a new server object for reading requests from an incoming
 * connection. */
static int raft__io_uv_rpc_server_init(struct raft__io_uv_rpc_server *s,
                                       struct raft__io_uv_rpc *rpc,
                                       const unsigned id,
                                       const char *address,
                                       struct uv_stream_s *stream);
static void raft__io_uv_rpc_server_close(struct raft__io_uv_rpc_server *s);

/* Start reading incoming requests. */
static int raft__io_uv_rpc_server_start(struct raft__io_uv_rpc_server *s);

/* Invoked to initialize the read buffer for the next asynchronous read on the
 * socket. */
static void raft__io_uv_rpc_server_alloc_cb(uv_handle_t *handle,
                                            size_t suggested_size,
                                            uv_buf_t *buf);

/* Callback invoked when data has been read from the socket. */
static void raft__io_uv_rpc_server_read_cb(uv_stream_t *stream,
                                           ssize_t nread,
                                           const uv_buf_t *buf);

/* Invoke the receive callback. */
static void raft__io_uv_rpc_server_recv(struct raft__io_uv_rpc_server *s);

/* Abort an inbound connection, removing this server from the active ones. */
static void raft__io_uv_rpc_server_abort(struct raft__io_uv_rpc_server *s);

/* Callback invoked afer the stream handle of this server connection has been
 * closed. */
static void raft__io_uv_rpc_server_stream_close_cb(uv_handle_t *handle);

/* Remove the given server connection */
static void raft__io_uv_rpc_remove_server(
    struct raft__io_uv_rpc *r,
    struct raft__io_uv_rpc_server *server);

int raft__io_uv_rpc_add_server(struct raft__io_uv_rpc *r,
                               unsigned id,
                               const char *address,
                               struct uv_stream_s *stream)
{
    struct raft__io_uv_rpc_server **servers;
    struct raft__io_uv_rpc_server *server;
    unsigned n_servers;
    int rv;

    /* Grow the servers array */
    n_servers = r->n_servers + 1;
    servers = raft_realloc(r->servers, n_servers * sizeof *servers);
    if (servers == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    r->servers = servers;
    r->n_servers = n_servers;

    /* Initialize the new connection */
    server = raft_malloc(sizeof *server);
    if (server == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_servers_realloc;
    }
    servers[n_servers - 1] = server;

    rv = raft__io_uv_rpc_server_init(server, r, id, address, stream);
    if (rv != 0) {
        goto err_after_server_alloc;
    }

    /* This will start reading requests. */
    rv = raft__io_uv_rpc_server_start(server);
    if (rv != 0) {
        goto err_after_server_init;
    }

    return 0;

err_after_server_init:
    raft__io_uv_rpc_server_close(server);

err_after_server_alloc:
    raft_free(server);

err_after_servers_realloc:
    /* Simply pretend that the connection was not inserted at all */
    r->n_servers--;

err:
    assert(rv != 0);

    return rv;
}

void raft__io_uv_rpc_server_stop(struct raft__io_uv_rpc_server *s)
{
    uv_close((struct uv_handle_s *)s->stream,
             raft__io_uv_rpc_server_stream_close_cb);
}

static int raft__io_uv_rpc_server_init(struct raft__io_uv_rpc_server *s,
                                       struct raft__io_uv_rpc *rpc,
                                       const unsigned id,
                                       const char *address,
                                       struct uv_stream_s *stream)
{
    s->rpc = rpc;
    s->id = id;

    /* Make a copy of the address. */
    s->address = raft_malloc(strlen(address) + 1);
    if (s->address == NULL) {
        return RAFT_ERR_NOMEM;
    }
    strcpy(s->address, address);

    s->stream = stream;

    s->buf.base = NULL;
    s->buf.len = 0;

    s->preamble[0] = 0;
    s->preamble[1] = 0;

    s->header.base = NULL;
    s->header.len = 0;

    s->payload.base = NULL;
    s->payload.len = 0;

    s->aborted = false;

    stream->data = s;

    return 0;
}

static void raft__io_uv_rpc_server_close(struct raft__io_uv_rpc_server *s)
{
    raft_free(s->address);

    if (s->header.base != NULL) {
        raft_free(s->header.base);
    }
}

static int raft__io_uv_rpc_server_start(struct raft__io_uv_rpc_server *s)
{
    int rv;

    rv = uv_read_start(s->stream, raft__io_uv_rpc_server_alloc_cb,
                       raft__io_uv_rpc_server_read_cb);
    if (rv != 0) {
        raft_warnf(s->rpc->logger, "start reading: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

static void raft__io_uv_rpc_server_alloc_cb(uv_handle_t *handle,
                                            size_t suggested_size,
                                            uv_buf_t *buf)
{
    struct raft__io_uv_rpc_server *s = handle->data;

    (void)suggested_size;

    /* If this is the first read of the preamble, or of the header, or of the
     * payload, then initialize the read buffer, according to the chunk of data
     * that we expect next. */
    if (s->buf.len == 0) {
        assert(s->buf.base == NULL);

        /* Check if we expect the preamble. */
        if (s->header.len == 0) {
            assert(s->preamble[0] == 0);
            assert(s->preamble[1] == 0);

            s->buf.base = (char *)s->preamble;
            s->buf.len = sizeof s->preamble;

            goto out;
        }

        /* Check if we expect the header. */
        if (s->payload.len == 0) {
            assert(s->header.len > 0);
            assert(s->header.base == NULL);

            s->header.base = raft_malloc(s->header.len);
            if (s->header.base == NULL) {
                /* Setting all buffer fields to 0 will make read_cb fail with
                 * ENOBUFS. */
                memset(buf, 0, sizeof *buf);
                return;
            }

            s->buf = s->header;
            goto out;
        }

        /* We should be expecting the payload. */
        assert(s->payload.len > 0);

        s->payload.base = raft_malloc(s->payload.len);
        if (s->payload.base == NULL) {
            /* Setting all buffer fields to 0 will make read_cb fail with
             * ENOBUFS. */
            memset(buf, 0, sizeof *buf);
            return;
        }

        s->buf = s->payload;
    }

out:
    *buf = s->buf;
}

static void raft__io_uv_rpc_server_recv(struct raft__io_uv_rpc_server *s)
{
    s->rpc->recv_cb(s->rpc, &s->message);

    memset(s->preamble, 0, sizeof s->preamble);

    raft_free(s->header.base);

    s->header.base = NULL;
    s->header.len = 0;

    s->payload.base = NULL;
    s->payload.len = 0;
}

static void raft__io_uv_rpc_server_read_cb(uv_stream_t *stream,
                                           ssize_t nread,
                                           const uv_buf_t *buf)
{
    struct raft__io_uv_rpc_server *s = stream->data;
    int rv;

    (void)buf;

    /* If the read was successful, let's check if we have received all the data
     * we expected. */
    if (nread > 0) {
        size_t n = (size_t)nread;

        /* We shouldn't have read more data than the pending amount. */
        assert(n <= s->buf.len);

        /* Advance the read window */
        s->buf.base += n;
        s->buf.len -= n;

        /* If there's more data to read in order to fill the current
         * read buffer, just return, we'll be invoked again. */
        if (s->buf.len > 0) {
            goto out;
        }

        if (s->header.len == 0) {
            /* If the header buffer is not set, it means that we've just
             * completed reading the preamble. */
            assert(s->header.base == NULL);

            s->header.len = raft__flip64(s->preamble[1]);

            /* The length of the header must be greater than zero. */
            if (s->header.len == 0) {
                raft_warnf(s->rpc->logger, "message has zero length");
                goto abort;
            }
        } else if (s->payload.len == 0) {
            /* If the payload buffer is not set, it means we just completed
             * reading the message header. */
            unsigned type;

            assert(s->header.base != NULL);

            type = raft__flip64(s->preamble[0]);
            assert(type > 0);

            rv = raft_io_uv_decode__message(type, &s->header, &s->message,
                                            &s->payload.len);
            if (rv != 0) {
                raft_warnf(s->rpc->logger, "decode message: %s",
                           raft_strerror(rv));
                goto abort;
            }

            s->message.server_id = s->id;
            s->message.server_address = s->address;

            /* If the message has no payload, we're done. */
            if (s->payload.len == 0) {
                raft__io_uv_rpc_server_recv(s);
            }
        } else {
            /* If we get here it means that we've just completed reading the
             * payload. */
            struct raft_buffer buf; /* TODO: avoid converting from uv_buf_t */
            assert(s->payload.base != NULL);
            assert(s->payload.len > 0);

            switch (s->message.type) {
                case RAFT_IO_APPEND_ENTRIES:
                    buf.base = s->payload.base;
                    buf.len = s->payload.len;
                    raft_io_uv_decode__entries_batch(
                        &buf, s->message.append_entries.entries,
                        s->message.append_entries.n_entries);
                    break;
                default:
                    /* We should never have read a payload in the first place */
                    assert(0);
            }

            raft__io_uv_rpc_server_recv(s);
        }

        /* Mark that we're done with this chunk. When the alloc callback will
         * trigger again it will notice that it needs to change the read
         * buffer. */
        assert(s->buf.len == 0);
        s->buf.base = NULL;

        goto out;
    }

    /* The if nread>0 condition above should always exit the function with a
     * goto. */
    assert(nread <= 0);

    if (nread == 0) {
        /* Empty read */
        goto out;
    }

    /* The "if nread==0" condition above should always exit the function
     * with a goto and never reach this point. */
    assert(nread < 0);

    raft_warnf(s->rpc->logger, "receive data: %s", uv_strerror(nread));

abort:
    raft__io_uv_rpc_server_abort(s);

out:
    return;
}

static void raft__io_uv_rpc_server_stream_close_cb(uv_handle_t *handle)
{
    struct raft__io_uv_rpc_server *s = handle->data;

    raft_free(handle);

    if (s->aborted) {
        raft__io_uv_rpc_remove_server(s->rpc, s);
    }

    raft__io_uv_rpc_server_close(s);
    raft_free(s);
}

static void raft__io_uv_rpc_server_abort(struct raft__io_uv_rpc_server *s)
{
    s->aborted = true;
    raft__io_uv_rpc_server_stop(s);
}

static void raft__io_uv_rpc_remove_server(struct raft__io_uv_rpc *r,
                                          struct raft__io_uv_rpc_server *server)
{
    unsigned i;
    unsigned j;

    for (i = 0; i < r->n_servers; i++) {
        if (r->servers[i] == server) {
            break;
        }
    }

    assert(i < r->n_servers);

    for (j = i + 1; j < r->n_servers; j++) {
        r->servers[j - 1] = r->servers[j];
    }

    r->n_servers--;
}
