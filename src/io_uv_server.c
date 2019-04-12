#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "byte.h"
#include "io_uv_encoding.h"
#include "logging.h"
#include "uv.h"

/* The happy path for a receiving an RPC message is:
 *
 * - When a peer server successfully establishes a new connection with us, the
 *   transport invokes our accept callback.
 *
 * - A new server object is created and added to the servers array. It starts
 *   reading from the stream handle of the new connection.
 *
 * - The RPC message preamble is read, which contains the message type and the
 *   message length.
 *
 * - The RPC message header is read, whose content depends on the message type.
 *
 * - Optionally, the RPC message payload is read (for AppendEntries requests).
 *
 * - The recv callback passed to raft_io->start() gets fired with the received
 *   message.
 *
 * Possible failure modes are:
 *
 * - The peer server disconnects. In this case the read callback will fire with
 *   UV_EOF, we'll close the stream handle and then release all memory
 *   associated with the server object.
 *
 * - The peer server sends us invalid data. In this case we close the stream
 *   handle and act like above.
 */

struct uv__server
{
    struct uv *uv;               /* libuv I/O implementation object */
    unsigned id;                 /* ID of the remote server */
    char *address;               /* Address of the other server */
    struct uv_stream_s *stream;  /* Connection handle */
    uv_buf_t buf;                /* Sliding buffer for reading incoming data */
    uint64_t preamble[2];        /* Static buffer with the request preamble */
    uv_buf_t header;             /* Dynamic buffer with the request header */
    uv_buf_t payload;            /* Dynamic buffer with the request payload */
    struct raft_message message; /* The message being received */
};

static void copy_address(const char *address1, char **address2)
{
    *address2 = raft_malloc(strlen(address1) + 1);
    if (*address2 == NULL) {
        return;
    }
    strcpy(*address2, address1);
}

/* Initialize a new server object for reading requests from an incoming
 * connection. */
static int server_init(struct uv__server *s,
                       struct uv *uv,
                       const unsigned id,
                       const char *address,
                       struct uv_stream_s *stream)
{
    s->uv = uv;
    s->id = id;
    copy_address(address, &s->address); /* Make a copy of the address string. */
    if (s->address == NULL) {
        return RAFT_NOMEM;
    }
    s->stream = stream;
    s->stream->data = s;
    s->buf.base = NULL;
    s->buf.len = 0;
    s->preamble[0] = 0;
    s->preamble[1] = 0;
    s->header.base = NULL;
    s->header.len = 0;
    s->payload.base = NULL;
    s->payload.len = 0;
    return 0;
}

static void server_close(struct uv__server *s)
{
    if (s->header.base != NULL) {
        /* This means we were interrupted while reading the header. */
        raft_free(s->header.base);
    }
    if (s->payload.base != NULL) {
        /* This means we were interrupted while reading the payload. */
        switch (s->message.type) {
            case RAFT_IO_APPEND_ENTRIES:
                raft_free(s->message.append_entries.entries);
                break;
            case RAFT_IO_INSTALL_SNAPSHOT:
                raft_configuration_close(&s->message.install_snapshot.conf);
                break;
        }
        raft_free(s->payload.base);
    }
    raft_free(s->address);
    raft_free(s->stream);
}

/* Invoked to initialize the read buffer for the next asynchronous read on the
 * socket. */
static void alloc_cb(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf)
{
    struct uv__server *s = handle->data;
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

        /* If we get here we should be expecting the payload. */
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

/* Remove the given server connection */
static void server_remove(struct uv__server *s)
{
    struct uv *uv = s->uv;
    unsigned i;
    unsigned j;

    for (i = 0; i < uv->n_servers; i++) {
        if (uv->servers[i] == s) {
            break;
        }
    }
    assert(i < uv->n_servers);

    /* Left-shift the pointers of the rest of the servers. */
    for (j = i + 1; j < uv->n_servers; j++) {
        uv->servers[j - 1] = uv->servers[j];
    }

    uv->n_servers--;
}

/* Callback invoked afer the stream handle of this server connection has been
 * closed. We can release all resources associated with the server object. */
static void stream_close_cb(uv_handle_t *handle)
{
    struct uv__server *s = handle->data;
    server_close(s);
    raft_free(s);
}

static void server_stop(struct uv__server *s)
{
    uv_close((struct uv_handle_s *)s->stream, stream_close_cb);
}

/* Invoke the receive callback. */
static void server_recv(struct uv__server *s)
{
    s->uv->recv_cb(s->uv->io, &s->message);

    /* Reset our state as we'll start reading a new message. We don't need to
     * release the payload buffer, since ownership was transfered to the
     * user. */
    memset(s->preamble, 0, sizeof s->preamble);
    raft_free(s->header.base);
    s->header.base = NULL;
    s->header.len = 0;
    s->payload.base = NULL;
    s->payload.len = 0;
}

/* Callback invoked when data has been read from the socket. */
static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
    struct uv__server *s = stream->data;
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
            return;
        }

        if (s->header.len == 0) {
            /* If the header buffer is not set, it means that we've just
             * completed reading the preamble. */
            assert(s->header.base == NULL);

            s->header.len = byte__flip64(s->preamble[1]);

            /* The length of the header must be greater than zero. */
            if (s->header.len == 0) {
                warnf(s->uv->io, "message has zero length");
                goto abort;
            }
        } else if (s->payload.len == 0) {
            /* If the payload buffer is not set, it means we just completed
             * reading the message header. */
            unsigned type;

            assert(s->header.base != NULL);

            type = byte__flip64(s->preamble[0]);
            assert(type > 0);

            rv = io_uv__decode_message(type, &s->header, &s->message,
                                       &s->payload.len);
            if (rv != 0) {
                warnf(s->uv->io, "decode message: %s", raft_strerror(rv));
                goto abort;
            }

            s->message.server_id = s->id;
            s->message.server_address = s->address;

            /* If the message has no payload, we're done. */
            if (s->payload.len == 0) {
                server_recv(s);
            }
        } else {
            /* If we get here it means that we've just completed reading the
             * payload. TODO: avoid converting from uv_buf_t */
            struct raft_buffer payload;
            assert(s->payload.base != NULL);
            assert(s->payload.len > 0);

            switch (s->message.type) {
                case RAFT_IO_APPEND_ENTRIES:
                    payload.base = s->payload.base;
                    payload.len = s->payload.len;
                    io_uv__decode_entries_batch(
                        &payload, s->message.append_entries.entries,
                        s->message.append_entries.n_entries);
                    break;
                case RAFT_IO_INSTALL_SNAPSHOT:
                    s->message.install_snapshot.data.base = s->payload.base;
                    break;
                default:
                    /* We should never have read a payload in the first place */
                    assert(0);
            }

            server_recv(s);
        }

        /* Mark that we're done with this chunk. When the alloc callback will
         * trigger again it will notice that it needs to change the read
         * buffer. */
        assert(s->buf.len == 0);
        s->buf.base = NULL;

        return;
    }

    /* The if nread>0 condition above should always exit the function with a
     * goto. */
    assert(nread <= 0);

    if (nread == 0) {
        /* Empty read */
        return;
    }

    /* The "if nread==0" condition above should always exit the function
     * with a goto and never reach this point. */
    assert(nread < 0);

    warnf(s->uv->io, "receive data: %s", uv_strerror(nread));

abort:
    server_remove(s);
    server_stop(s);
}

/* Start reading incoming requests. */
static int server_start(struct uv__server *s)
{
    int rv;

    rv = uv_read_start(s->stream, alloc_cb, read_cb);
    if (rv != 0) {
        warnf(s->uv->io, "start reading: %s", uv_strerror(rv));
        return RAFT_IOERR;
    }

    return 0;
}

static int server_add(struct uv *uv,
                      unsigned id,
                      const char *address,
                      struct uv_stream_s *stream)
{
    struct uv__server **servers;
    struct uv__server *s;
    unsigned n_servers;
    int rv;

    /* Grow the servers array */
    n_servers = uv->n_servers + 1;
    servers = raft_realloc(uv->servers, n_servers * sizeof *servers);
    if (servers == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    uv->servers = servers;
    uv->n_servers = n_servers;

    /* Initialize the new connection */
    s = raft_malloc(sizeof *s);
    if (s == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_servers_realloc;
    }
    servers[n_servers - 1] = s;

    rv = server_init(s, uv, id, address, stream);
    if (rv != 0) {
        goto err_after_server_alloc;
    }

    /* This will start reading requests. */
    rv = server_start(s);
    if (rv != 0) {
        goto err_after_server_init;
    }

    return 0;

err_after_server_init:
    server_close(s);

err_after_server_alloc:
    raft_free(s);

err_after_servers_realloc:
    /* Simply pretend that the connection was not inserted at all */
    uv->n_servers--;

err:
    assert(rv != 0);

    return rv;
}

static void accept_cb(struct raft_uv_transport *transport,
                      unsigned id,
                      const char *address,
                      struct uv_stream_s *stream)
{
    struct uv *uv = transport->data;
    int rv;

    assert(uv->state == UV__ACTIVE || uv->state == UV__CLOSING);

    if (uv->state == UV__CLOSING) {
        goto abort;
    }

    rv = server_add(uv, id, address, stream);
    if (rv != 0) {
        warnf(uv->io, "add server: %s", raft_strerror(rv));
        goto abort;
    }

    return;

abort:
    uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
}

int io_uv__listen(struct uv *uv)
{
    int rv;
    rv = uv->transport->listen(uv->transport, accept_cb);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

void io_uv__servers_stop(struct uv *uv)
{
    unsigned i;
    for (i = 0; i < uv->n_servers; i++) {
        server_stop(uv->servers[i]);
    }
}
