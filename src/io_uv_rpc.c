#include <string.h>

#include "assert.h"
#include "binary.h"
#include "io_uv_encoding.h"
#include "io_uv_rpc.h"

/**
 * Protocol version.
 */
#define RAFT_IO_UV_RPC__PROTOCOL 1

/**
 * Retry to connect every second.
 *
 * TODO: implement an exponential backoff instead.
 */
#define RAFT_IO_UV_RPC_CLIENT__CONNECT_RETRY_DELAY 1000

/**
 * Initialize the given request object, encoding the given @message.
 *
 * The @cb callback must be invoked once the request completes (either
 * successfully or not).
 */
static int raft_io_uv_rpc_request__init(struct raft_io_uv_rpc_request *r,
                                        const struct raft_message *message,
                                        void *data,
                                        void (*cb)(void *data,
                                                   const int status))
{
    int rv;

    r->req.data = r;
    r->data = data;
    r->cb = cb;

    rv = raft_io_uv_encode__message(message, &r->bufs, &r->n_bufs);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

/**
 * Release all memory associated with the given request object.
 */
static void raft_io_uv_rpc_request__close(struct raft_io_uv_rpc_request *r)
{
    assert(r->bufs != NULL);
    assert(r->bufs[0].base != NULL);

    /* Just release the first buffer. Further buffers are entry payloads, which
     * we were passed but we don't own. */
    raft_free(r->bufs[0].base);

    /* Release the buffers array. */
    raft_free(r->bufs);
}

/**
 * Initialize a new outgoing connection, making a copy of the address of the
 * remote Raft server.
 */
static int raft_io_uv_rpc_client__init(struct raft_io_uv_rpc_client *c,
                                       struct raft_io_uv_rpc *rpc,
                                       unsigned id,
                                       const char *address)
{
    int rv;

    c->rpc = rpc;
    c->timer.data = c;
    c->req.data = c;
    c->stream = NULL;
    c->n_connect_errors = 0;
    c->id = id;

    /* Make a copy of the address string */
    c->address = raft_malloc(strlen(address) + 1);
    if (c->address == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    strcpy(c->address, address);

    return 0;

err:
    assert(rv != 0);

    return rv;
}

/**
 * Release all resources associated with an outgoing connection.
 */
static void raft_io_uv_rpc_client__close(struct raft_io_uv_rpc_client *c)
{
    assert(c->address != NULL);
    raft_free(c->address);
}

/**
 * Callback invoked after the stream handle of a connection is closed because
 * the connection attempt failed.
 */
static void raft_io_uv_rpc_client__connect_stream_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv_rpc_client *c = handle->data;

    c->rpc->n_active--;
    raft_free(handle);
}

/* Forward declaration */
static void raft_io_uv_rpc_client__timer_cb(uv_timer_t *timer);

/**
 * Callback invoked after a connection attempt completed (either successfully or
 * not).
 */
static void raft_io_uv_rpc_client__connect_cb(void *data, int status)
{
    struct raft_io_uv_rpc_client *c = data;
    void (*log)(struct raft_logger * logger, const char *format, ...);
    int rv;

    /* If there's no stream handle set, it means we were stopped before the
     * connection was fully setup. Let's just bail out. */
    if (c->stream == NULL) {
        return;
    }

    /* The connection attempt was successful. We're good. */
    if (status == 0) {
        c->n_connect_errors = 0;
        return;
    }

    c->n_connect_errors++;

    /* Use debug level for logging the first few attempts, then switch to
     * warn. */
    if (c->n_connect_errors < 10) {
        log = raft_debugf;
    } else {
        log = raft_warnf;
    }

    log(c->rpc->logger, "connect to %d (%s): %s", c->id, c->address,
        uv_strerror(status));

    /* Let's close this stream handle and schedule another connection
     * attempt.  */
    uv_close((uv_handle_t *)c->stream,
             raft_io_uv_rpc_client__connect_stream_close_cb);

    c->stream = NULL;

    rv = uv_timer_start(&c->timer, raft_io_uv_rpc_client__timer_cb,
                        c->rpc->connect_retry_delay, 0);
    assert(rv == 0);
}

/**
 * Perform a single connection attempt, scheduling a retry if it fails.
 */
static void raft_io_uv_rpc_client__connect(struct raft_io_uv_rpc_client *c)
{
    int rv;

    assert(c->stream == NULL);

    /* Trigger a connection attempt. */
    rv = c->rpc->transport->connect(c->rpc->transport, c->id, c->address,
                                    &c->stream, c,
                                    raft_io_uv_rpc_client__connect_cb);
    if (rv != 0) {
        assert(c->stream == NULL);

        /* Restart the timer, so we can retry. */
        rv = uv_timer_start(&c->timer, raft_io_uv_rpc_client__timer_cb,
                            c->rpc->connect_retry_delay, 0);
        assert(rv == 0);

        return;
    }

    assert(c->stream != NULL);

    c->stream->data = c;
    c->rpc->n_active++;
}

/**
 * Callback invoked once the connection retry timer expires.
 */
static void raft_io_uv_rpc_client__timer_cb(uv_timer_t *timer)
{
    struct raft_io_uv_rpc_client *c = timer->data;

    assert(c->stream == NULL);

    /* Retry to connect. */
    raft_io_uv_rpc_client__connect(c);
}

/**
 * Start the client by making the first connection attempt.
 */
static void raft_io_uv_rpc_client__start(struct raft_io_uv_rpc_client *c)
{
    int rv;

    assert(c->stream == NULL);

    rv = uv_timer_init(c->rpc->loop, &c->timer);
    assert(rv == 0); /* This should never fail */

    c->rpc->n_active++;

    /* Make a first connection attempt right away. */
    raft_io_uv_rpc_client__connect(c);
}

/**
 * Invoke the stop callback if we were asked to be stopped and there are no more
 * pending asynchronous activities.
 */
static void raft_io_uv_rpc__maybe_stopped(struct raft_io_uv_rpc *r)
{
    if (r->stop.cb != NULL && r->n_active == 0) {
        r->stop.cb(r->stop.data);

        r->stop.data = NULL;
        r->stop.cb = NULL;
    }
}

/**
 * Callback invoked when the stream handle of an active outgoing connection has
 * been closed during the stop phase.
 */
static void raft_io_uv_rpc_client__stream_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv_rpc_client *c = handle->data;

    c->rpc->n_active--;
    raft_free(handle);
    raft_io_uv_rpc__maybe_stopped(c->rpc);
}

/**
 * Callback invoked when the timer handle of an active outgoing connection has
 * been closed, during the stop phase of the backend.
 */
static void raft_io_uv_rpc_client__timer_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv_rpc_client *c = handle->data;

    c->rpc->n_active--;
    raft_io_uv_rpc__maybe_stopped(c->rpc);
}

/**
 * Request to stop this outgoing connection, eventually releasing all associated
 * resources.
 */
static void raft_io_uv_rpc_client__stop(struct raft_io_uv_rpc_client *c)
{
    int rv;

    if (c->stream != NULL) {
        uv_close((uv_handle_t *)c->stream,
                 raft_io_uv_rpc_client__stream_close_cb);
        c->stream = NULL;
    }

    rv = uv_timer_stop(&c->timer);
    assert(rv == 0);

    uv_close((struct uv_handle_s *)&c->timer,
             raft_io_uv_rpc_client__timer_close_cb);
}

/**
 * Initialize a new server object for reading requests from an incoming
 * connection.
 */
static int raft_io_uv_rpc_server__init(struct raft_io_uv_rpc_server *s,
                                       struct raft_io_uv_rpc *rpc,
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

    rpc->n_active++;

    return 0;
}

/**
 * Release all resources associated with an incoming connection.
 */
static void raft_io_uv_rpc_server__close(struct raft_io_uv_rpc_server *s)
{
    raft_free(s->address);

    if (s->header.base != NULL) {
        raft_free(s->header.base);
    }
}

static void raft_io_uv_rpc__remove_server(struct raft_io_uv_rpc *r,
                                          struct raft_io_uv_rpc_server *server);

/**
 * Callback invoked afer the stream handle of this server connection has been
 * closed.
 */
static void raft_io_uv_rpc_server__stream_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv_rpc_server *s = handle->data;

    s->rpc->n_active--;
    raft_free(handle);

    if (s->aborted) {
        raft_io_uv_rpc__remove_server(s->rpc, s);
        raft_io_uv_rpc_server__close(s);
        raft_free(s);
    } else {
        raft_io_uv_rpc__maybe_stopped(s->rpc);
    }
}

/**
 * Request to stop this incoming connection, eventually releasing all associated
 * resources.
 */
static void raft_io_uv_rpc_server__stop(struct raft_io_uv_rpc_server *s)
{
    int rv;

    rv = uv_read_stop(s->stream);
    assert(rv == 0);

    uv_close((struct uv_handle_s *)s->stream,
             raft_io_uv_rpc_server__stream_close_cb);
}

/**
 * Abort an inbound connection, removing this server from the active ones.
 */
static void raft_io_uv_rpc_server__abort(struct raft_io_uv_rpc_server *s)
{
    s->aborted = true;
    raft_io_uv_rpc_server__stop(s);
}

/**
 * Callback invoked to initialy the read buffer for the next asynchronous read
 * on the socket.
 */
static void raft_io_uv_rpc_server__alloc_cb(uv_handle_t *handle,
                                            size_t suggested_size,
                                            uv_buf_t *buf)
{
    struct raft_io_uv_rpc_server *s = handle->data;

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

static void raft_io_uv_rpc_server__recv(struct raft_io_uv_rpc_server *s)
{
    s->rpc->recv.cb(s->rpc->recv.data, &s->message);

    memset(s->preamble, 0, sizeof s->preamble);

    raft_free(s->header.base);

    s->header.base = NULL;
    s->header.len = 0;

    s->payload.base = NULL;
    s->payload.len = 0;
}

/**
 * Callback invoked when data has been read from the socket.
 */
static void raft_io_uv_rpc_server__read_cb(uv_stream_t *stream,
                                           ssize_t nread,
                                           const uv_buf_t *buf)
{
    struct raft_io_uv_rpc_server *s = stream->data;
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
                raft_io_uv_rpc_server__recv(s);
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

            raft_io_uv_rpc_server__recv(s);
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
    raft_io_uv_rpc_server__abort(s);

out:
    return;
}

/**
 * Start reading incoming requests.
 */
static int raft_io_uv_rpc_server__start(struct raft_io_uv_rpc_server *s)
{
    int rv;

    rv = uv_read_start(s->stream, raft_io_uv_rpc_server__alloc_cb,
                       raft_io_uv_rpc_server__read_cb);
    if (rv != 0) {
        raft_warnf(s->rpc->logger, "start reading: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Create a new server object and append it to the servers array.
 */
static int raft_io_uv_rpc__add_server(struct raft_io_uv_rpc *r,
                                      unsigned id,
                                      const char *address,
                                      struct uv_stream_s *stream)
{
    struct raft_io_uv_rpc_server **servers;
    struct raft_io_uv_rpc_server *server;
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

    rv = raft_io_uv_rpc_server__init(server, r, id, address, stream);
    if (rv != 0) {
        goto err_after_server_alloc;
    }

    /* This will start reading requests. */
    rv = raft_io_uv_rpc_server__start(server);
    if (rv != 0) {
        goto err_after_server_init;
    }

    return 0;

err_after_server_init:
    raft_io_uv_rpc_server__close(server);

err_after_server_alloc:
    raft_free(server);

err_after_servers_realloc:
    /* Simply pretend that the connection was not inserted at all */
    r->n_servers--;

err:
    assert(rv != 0);

    return rv;
}

/**
 * Remove the given server connection
 */
static void raft_io_uv_rpc__remove_server(struct raft_io_uv_rpc *r,
                                          struct raft_io_uv_rpc_server *server)
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

static void raft_io_uv_rpc__connection_close_cb(struct uv_handle_s *handle)
{
    raft_free(handle);
}

/**
 * Callback invoked when a new incoming connection has been established.
 */
static void raft_io_uv_rpc__connection_cb(void *data,
                                          unsigned id,
                                          const char *address,
                                          struct uv_stream_s *stream)
{
    struct raft_io_uv_rpc *r = data;
    int rv;

    rv = raft_io_uv_rpc__add_server(r, id, address, stream);
    if (rv != 0) {
        raft_warnf(r->logger, "add server: %s", raft_strerror(rv));
        uv_close((struct uv_handle_s *)stream,
                 raft_io_uv_rpc__connection_close_cb);
    }
}

int raft_io_uv_rpc__init(struct raft_io_uv_rpc *r,
                         struct raft_logger *logger,
                         struct uv_loop_s *loop,
                         struct raft_io_uv_transport *transport)
{
    r->logger = logger;
    r->loop = loop;
    r->transport = transport;

    r->clients = NULL;
    r->servers = NULL;
    r->n_clients = 0;
    r->n_servers = 0;
    r->connect_retry_delay = RAFT_IO_UV_RPC_CLIENT__CONNECT_RETRY_DELAY;
    r->n_active = 1 /* The transport connection listener */;

    r->recv.data = NULL;
    r->recv.cb = NULL;

    r->stop.data = NULL;
    r->stop.cb = NULL;

    return 0;
}

void raft_io_uv_rpc__close(struct raft_io_uv_rpc *r)
{
    unsigned i;

    for (i = 0; i < r->n_clients; i++) {
        raft_io_uv_rpc_client__close(r->clients[i]);
        raft_free(r->clients[i]);
    }

    if (r->clients != NULL) {
        raft_free(r->clients);
    }

    for (i = 0; i < r->n_servers; i++) {
        raft_io_uv_rpc_server__close(r->servers[i]);
        raft_free(r->servers[i]);
    }

    if (r->servers != NULL) {
        raft_free(r->servers);
    }
}

int raft_io_uv_rpc__start(struct raft_io_uv_rpc *r,
                          unsigned id,
                          const char *address,
                          void *data,
                          void (*recv)(void *data, struct raft_message *msg))
{
    int rv;

    r->recv.data = data;
    r->recv.cb = recv;

    rv = r->transport->start(r->transport, id, address, r,
                             raft_io_uv_rpc__connection_cb);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void raft_io_uv_rpc__transport_stop_cb(void *data)
{
    struct raft_io_uv_rpc *r = data;

    r->n_active--;

    raft_io_uv_rpc__maybe_stopped(r);
}

void raft_io_uv_rpc__stop(struct raft_io_uv_rpc *r,
                          void *data,
                          void (*cb)(void *data))
{
    unsigned i;

    assert(r->stop.data == NULL);
    assert(r->stop.cb == NULL);

    r->stop.data = data;
    r->stop.cb = cb;

    for (i = 0; i < r->n_clients; i++) {
        raft_io_uv_rpc_client__stop(r->clients[i]);
    }

    for (i = 0; i < r->n_servers; i++) {
        raft_io_uv_rpc_server__stop(r->servers[i]);
    }

    r->transport->stop(r->transport, r, raft_io_uv_rpc__transport_stop_cb);

    raft_io_uv_rpc__maybe_stopped(r);
}

/**
 * Search a client object matching the given server ID. If not found, a new one
 * will be created and appended to the clients array.
 */
static int raft_io_uv_rpc__get_client(struct raft_io_uv_rpc *r,
                                      const unsigned id,
                                      const char *address,
                                      struct raft_io_uv_rpc_client **client)
{
    struct raft_io_uv_rpc_client **clients;
    unsigned n_clients;
    unsigned i;
    int rv;

    for (i = 0; i < r->n_clients; i++) {
        *client = r->clients[i];

        if ((*client)->id == id) {
            /* TODO: handle a change in the address */
            assert(strcmp((*client)->address, address) == 0);
            return 0;
        }
    }

    /* Grow the connections array */
    n_clients = r->n_clients + 1;
    clients = raft_realloc(r->clients, n_clients * sizeof *clients);
    if (clients == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    r->clients = clients;
    r->n_clients = n_clients;

    /* Initialize the new connection */
    *client = raft_malloc(sizeof **client);
    if (*client == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_clients_realloc;
    }

    clients[n_clients - 1] = *client;

    rv = raft_io_uv_rpc_client__init(*client, r, id, address);
    if (rv != 0) {
        goto err_after_client_alloc;
    }

    /* This will trigger the first connection attempt. */
    raft_io_uv_rpc_client__start(*client);

    return 0;

err_after_client_alloc:
    raft_free(*client);

err_after_clients_realloc:
    /* Simply pretend that the connection was not inserted at all */
    r->n_clients--;

err:
    assert(rv != 0);

    return rv;
}

static void raft_io_uv_rpc__send_write_cb(struct uv_write_s *req, int status)
{
    struct raft_io_uv_rpc_request *r = req->data;

    if (r->cb != NULL) {
        r->cb(r->data, status);
    }

    raft_io_uv_rpc_request__close(r);
    raft_free(r);
}

int raft_io_uv_rpc__send(struct raft_io_uv_rpc *r,
                         const struct raft_message *message,
                         void *data,
                         void (*cb)(void *data, int status))
{
    struct raft_io_uv_rpc_request *request;
    struct raft_io_uv_rpc_client *client;
    int rv;

    /* Allocate a new RPC request object. */
    request = raft_malloc(sizeof *request);
    if (request == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    /* Initialize the request object, encode the message. */
    rv = raft_io_uv_rpc_request__init(request, message, data, cb);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    /* Get the connection info for the target server, creating it if it doesn't
     * exist. */
    rv = raft_io_uv_rpc__get_client(r, message->server_id,
                                    message->server_address, &client);
    if (rv != 0) {
        goto err_after_request_encode;
    }

    /* If there's no connection available, let's fail immediately. */
    if (client->stream == NULL) {
        rv = RAFT_ERR_IO_CONNECT;
        goto err_after_request_encode;
    }

    rv = uv_write(&request->req, client->stream, request->bufs, request->n_bufs,
                  raft_io_uv_rpc__send_write_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        rv = RAFT_ERR_IO;
        goto err_after_request_encode;
    }

    return 0;

err_after_request_encode:
    raft_io_uv_rpc_request__close(request);

err_after_request_alloc:
    raft_free(request);

err:
    assert(rv != 0);

    return rv;
}

/**
 * State for the default implementation of the @raft_io_uv_transport interface.
 */
struct raft_io_uv_tcp
{
    struct raft_logger *logger;
    struct uv_loop_s *loop;
    unsigned id;
    const char *address;
    struct
    {
        struct uv_tcp_s tcp;
        void *data;
        void (*cb)(void *data,
                   unsigned id,
                   const char *address,
                   uv_stream_t *stream);
    } listener;
    struct
    {
        void *data;
        void (*cb)(void *data);
    } stop;
};

/**
 * Hold state for connection being accepted.
 */
struct raft_io_uv_tcp_accept
{
    struct raft_io_uv_tcp *tcp;
    struct uv_tcp_s *client;
    uv_buf_t buf;
    uint64_t preamble[3];
    unsigned id;
    size_t address_len;
    char *address;
};

/**
 * Hold state for a single connection attempt.
 */
struct raft_io_uv_tcp_connect
{
    struct uv_connect_s req;
    void *data;
    void (*cb)(void *data, int status);

    /* Counter of pending connect-related callbacks. Since we call
     * uv_tcp_connect() and uv_write() concurrently we need to know if the dust
     * has settle before releasing any memory in the associated callbacks. */
    unsigned pending_cbs;

    /* Initial handshake write request to send right away after connection. It
     * contains the protocol version, our ID and our address. */
    struct
    {
        struct uv_write_s req;
        uv_buf_t buf;
    } handshake;
};

static size_t raft_io_uv_tcp__sizeof_handshake(const char *address)
{
    size_t size = sizeof(uint64_t) + /* Protocol version. */
                  sizeof(uint64_t) + /* Server ID. */
                  sizeof(uint64_t) /* Size of the address buffer */;

    size += strlen(address) + 1;

    size = raft__pad64(size);

    return size;
}

static void raft_io_uv_tcp__encode_handshake(unsigned id,
                                             const char *address,
                                             void *buf)
{
    void *cursor = buf;
    size_t len = strlen(address) + 1;

    len = raft__pad64(len);
    raft__put64(&cursor, RAFT_IO_UV_RPC__PROTOCOL);
    raft__put64(&cursor, id);
    raft__put64(&cursor, len);

    strcpy(cursor, address);
}

static void raft_io_uv_tcp__close_cb(struct uv_handle_s *handle)
{
    struct raft_io_uv_tcp_accept *accept = handle->data;

    raft_free(accept->client);
    raft_free(accept);
}

/**
 * Abort a inbound connection due to bad handshake.
 */
static void raft_io_uv_tcp__abort(struct raft_io_uv_tcp_accept *accept)
{
    int rv;

    rv = uv_read_stop((uv_stream_t *)accept->client);
    assert(rv == 0);

    uv_close((uv_handle_t *)accept->client, raft_io_uv_tcp__close_cb);
}

static void raft_io_uv_tcp__handshake_alloc_cb(uv_handle_t *handle,
                                               size_t suggested_size,
                                               uv_buf_t *buf)
{
    struct raft_io_uv_tcp_accept *accept = handle->data;

    (void)suggested_size;

    if (accept->buf.base == NULL) {
        assert(accept->buf.len == 0);

        if (accept->id == 0) {
            accept->buf.base = (char *)accept->preamble;
            accept->buf.len = sizeof accept->preamble;
            goto out;
        }

        assert(accept->address == NULL);

        accept->address = raft_malloc(accept->address_len);
        if (accept->address == NULL) {
            goto out;
        }

        accept->buf.base = accept->address;
        accept->buf.len = accept->address_len;
    }

out:
    *buf = accept->buf;
}

static void raft_io_uv_tcp__handshake_read_cb(uv_stream_t *stream,
                                              ssize_t nread,
                                              const uv_buf_t *buf)
{
    struct raft_io_uv_tcp_accept *accept = stream->data;
    int rv;

    (void)buf;

    if (nread > 0) {
        size_t n = (size_t)nread;

        /* We shouldn't have read more data than the pending amount. */
        assert(n <= accept->buf.len);

        /* Advance the read window */
        accept->buf.base += n;
        accept->buf.len -= n;

        /* If there's more data to read in order to fill the current
         * read buffer, just return, we'll be invoked again. */
        if (accept->buf.len > 0) {
            goto out;
        }

        /* If we have completed reading the preable, let's parse it. */
        if (accept->id == 0) {
            uint64_t protocol = raft__flip64(accept->preamble[0]);

            if (protocol != RAFT_IO_UV_RPC__PROTOCOL) {
                goto abort;
            }

            accept->id = raft__flip64(accept->preamble[1]);
            accept->address_len = raft__flip64(accept->preamble[2]);

            accept->buf.base = NULL;
            accept->buf.len = 0;

            goto out;
        }

        assert(accept->address != NULL);

        rv = uv_read_stop(stream);
        assert(rv == 0);

        accept->tcp->listener.cb(accept->tcp->listener.data, accept->id,
                                 accept->address, stream);

        raft_free(accept->address);

        goto done;
    }

    /* The if nread>0 condition above should always exit the function with a
     * goto. */
    assert(nread <= 0);

    if (nread == 0) {
        /* Empty read just ignore it. */
        goto out;
    }

    /* TODO log the error */
    assert(nread < 0);

abort:
    raft_io_uv_tcp__abort(accept);

    return;

done:
    raft_free(accept);

out:
    return;
}

/**
 * Callback invoked when the listening socket of the transport has received a
 * new connection.
 */
static void raft_io_uv_tcp__listen_cb(uv_stream_t *server, int status)
{
    struct raft_io_uv_tcp *tcp = server->data;
    struct raft_io_uv_tcp_accept *accept;
    int rv;

    if (status < 0) {
        rv = -status;
        goto err;
    }

    accept = raft_malloc(sizeof *accept);
    if (accept == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    accept->tcp = tcp;
    accept->id = 0;
    accept->address = NULL;
    accept->buf.base = NULL;
    accept->buf.len = 0;

    accept->client = raft_malloc(sizeof *accept->client);
    if (accept->client == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_accept_alloc;
    }

    accept->client->data = accept;

    rv = uv_tcp_init(tcp->loop, accept->client);
    if (rv != 0) {
        goto err_after_client_alloc;
    }

    rv = uv_accept(server, (uv_stream_t *)accept->client);
    if (rv != 0) {
        goto err_after_client_init;
    }

    rv = uv_read_start((uv_stream_t *)accept->client,
                       raft_io_uv_tcp__handshake_alloc_cb,
                       raft_io_uv_tcp__handshake_read_cb);
    if (rv != 0) {
        goto err_after_client_init;
    }

    return;

err_after_client_init:
    uv_close((uv_handle_t *)accept->client, NULL);

err_after_client_alloc:
    raft_free(accept->client);

err_after_accept_alloc:
    raft_free(accept);

err:
    assert(rv != 0);
    /* TODO: log the failure */
}

/**
 * Split @address into @host and @port and populate @addr accordingly.
 */
static int raft_io_uv_tcp__parse_address(const char *address,
                                         struct sockaddr_in *addr)
{
    char buf[256];
    char *host;
    char *port;
    char *colon = ":";
    int rv;

    /* TODO: turn this poor man parsing into proper one */
    strcpy(buf, address);
    host = strtok(buf, colon);
    port = strtok(NULL, ":");
    if (port == NULL) {
        port = "8080";
    }

    rv = uv_ip4_addr(host, atoi(port), addr);
    if (rv != 0) {
        return RAFT_ERR_IO_CONNECT;
    }

    return 0;
}

/**
 * Start accepting incoming TCP connections from other servers.
 */
static int raft_io_uv_tcp__start(struct raft_io_uv_transport *t,
                                 unsigned id,
                                 const char *address,
                                 void *data,
                                 void (*cb)(void *data,
                                            unsigned id,
                                            const char *address,
                                            uv_stream_t *stream))
{
    struct raft_io_uv_tcp *tcp;
    struct sockaddr_in addr;
    int rv;

    tcp = t->data;

    tcp->id = id;
    tcp->address = address;

    tcp->listener.data = data;
    tcp->listener.cb = cb;

    rv = uv_tcp_init(tcp->loop, &tcp->listener.tcp);
    if (rv != 0) {
        raft_warnf(tcp->logger, "uv_tcp_init: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    rv = raft_io_uv_tcp__parse_address(tcp->address, &addr);
    if (rv != 0) {
        return rv;
    }

    rv = uv_tcp_bind(&tcp->listener.tcp, (const struct sockaddr *)&addr, 0);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        raft_warnf(tcp->logger, "uv_tcp_bind: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    rv = uv_listen((uv_stream_t *)&tcp->listener.tcp, 1,
                   raft_io_uv_tcp__listen_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? */
        raft_warnf(tcp->logger, "uv_tcp_listen: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

static void raft_io_uv_tcp__listener_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv_tcp *tcp = handle->data;

    tcp->stop.cb(tcp->stop.data);
}

static void raft_io_uv_tcp__stop(struct raft_io_uv_transport *t,
                                 void *data,
                                 void (*cb)(void *data))
{
    struct raft_io_uv_tcp *tcp = t->data;

    tcp->stop.data = data;
    tcp->stop.cb = cb;

    uv_close((struct uv_handle_s *)&tcp->listener.tcp,
             raft_io_uv_tcp__listener_close_cb);
}

static void raft_io_uv_tcp__connect_write_cb(struct uv_write_s *req, int status)
{
    struct raft_io_uv_tcp_connect *connect = req->data;

    (void)status;

    connect->pending_cbs--;
    if (connect->pending_cbs == 0) {
        raft_free(connect->handshake.buf.base);
        raft_free(connect);
    }
}

static void raft_io_uv_tcp__connect_cb(struct uv_connect_s *req, int status)
{
    struct raft_io_uv_tcp_connect *connect = req->data;

    connect->cb(connect->data, status);

    connect->pending_cbs--;
    if (connect->pending_cbs == 0) {
        raft_free(connect->handshake.buf.base);
        raft_free(connect);
    }
}

static int raft_io_uv_tcp__connect(struct raft_io_uv_transport *t,
                                   unsigned id,
                                   const char *address,
                                   struct uv_stream_s **stream,
                                   void *data,
                                   void (*cb)(void *data, int status))
{
    struct raft_io_uv_tcp *tcp = t->data;
    struct uv_tcp_s *client;
    struct raft_io_uv_tcp_connect *connect;
    struct sockaddr_in addr;
    int rv;

    (void)id;
    (void)t;

    connect = raft_malloc(sizeof *connect);
    if (connect == NULL) {
        /* UNTESTED: should be investigated */
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    connect->req.data = connect;
    connect->data = data;
    connect->cb = cb;
    connect->pending_cbs = 0;

    /* Initialize the handshake buffer. */
    connect->handshake.req.data = connect;
    connect->handshake.buf.len = raft_io_uv_tcp__sizeof_handshake(tcp->address);
    connect->handshake.buf.base = raft_malloc(connect->handshake.buf.len);
    if (connect->handshake.buf.base == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err_after_connect_alloc;
    }
    raft_io_uv_tcp__encode_handshake(tcp->id, tcp->address,
                                     connect->handshake.buf.base);

    client = raft_malloc(sizeof *client);
    if (client == NULL) {
        /* UNTESTED: should be investigated */
        rv = RAFT_ERR_NOMEM;
        goto err_after_handshake_alloc;
    }

    rv = uv_tcp_init(tcp->loop, client);
    if (rv != 0) {
        /* UNTESTED: in the current libuv implementation this can't fail */
        rv = RAFT_ERR_IO;
        goto err_after_tcp_alloc;
    }

    rv = raft_io_uv_tcp__parse_address(address, &addr);
    if (rv != 0) {
        goto err_after_tcp_init;
    }

    rv = uv_tcp_connect(&connect->req, client, (struct sockaddr *)&addr,
                        raft_io_uv_tcp__connect_cb);
    if (rv != 0) {
        /* UNTESTED: since parsing succeed, this should fail only because of
         * lack of system resources */
        raft_warnf(tcp->logger, "connect: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO_CONNECT;
        goto err_after_tcp_init;
    }
    connect->pending_cbs++;

    rv = uv_write(&connect->handshake.req, (struct uv_stream_s *)client,
                  &connect->handshake.buf, 1, raft_io_uv_tcp__connect_write_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        raft_warnf(tcp->logger, "write: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_tcp_init;
    }
    connect->pending_cbs++;

    *stream = (struct uv_stream_s *)client;

    return 0;

err_after_tcp_init:
    uv_close((uv_handle_t *)client, NULL);

err_after_tcp_alloc:
    raft_free(client);

err_after_handshake_alloc:
    raft_free(connect->handshake.buf.base);

err_after_connect_alloc:
    raft_free(connect);

err:
    assert(rv != 0);

    *stream = NULL;

    return rv;
}

int raft_io_uv_tcp_init(struct raft_io_uv_transport *t,
                        struct raft_logger *logger,
                        struct uv_loop_s *loop)
{
    struct raft_io_uv_tcp *tcp;

    tcp = raft_malloc(sizeof *tcp);
    if (tcp == NULL) {
        /* UNTESTED: not interesting */
        return RAFT_ERR_NOMEM;
    }

    tcp->logger = logger;
    tcp->loop = loop;
    tcp->listener.tcp.data = tcp;

    t->data = tcp;
    t->start = raft_io_uv_tcp__start;
    t->stop = raft_io_uv_tcp__stop;
    t->connect = raft_io_uv_tcp__connect;

    return 0;
}

void raft_io_uv_tcp_close(struct raft_io_uv_transport *t)
{
    struct raft_io_uv_tcp *tcp = t->data;

    raft_free(tcp);
}
