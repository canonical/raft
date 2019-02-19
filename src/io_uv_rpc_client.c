#include <string.h>

#include "assert.h"
#include "io_uv_encoding.h"
#include "io_uv_rpc_client.h"

/* Client state codes. */
enum {
    RAFT__IO_UV_RPC_CLIENT_CONNECTING = 1,
    RAFT__IO_UV_RPC_CLIENT_CONNECTED,
    RAFT__IO_UV_RPC_CLIENT_DELAY,
    RAFT__IO_UV_RPC_CLIENT_CLOSING,
    RAFT__IO_UV_RPC_CLIENT_CLOSED,
};

/* Initialize a new client associated with the given server. */
static int raft__io_uv_rpc_client_init(struct raft__io_uv_rpc_client *c,
                                       struct raft__io_uv_rpc *rpc,
                                       unsigned id,
                                       const char *address);
static void raft__io_uv_rpc_client_close(struct raft__io_uv_rpc_client *c);

/* Start the client by making the first connection attempt. */
static void raft__io_uv_rpc_client_start(struct raft__io_uv_rpc_client *c);

/* Perform a single connection attempt, scheduling a retry if it fails. */
static void raft__io_uv_rpc_client_connect(struct raft__io_uv_rpc_client *c);
static void raft__io_uv_rpc_client_connect_cb(struct raft_io_uv_connect *req,
                                              struct uv_stream_s *stream,
                                              int status);
static void raft__io_uv_rpc_client_connect_retry_cb(uv_timer_t *timer);

static void raft__io_uv_rpc_client_write_cb(struct uv_write_s *req, int status);

static void raft__io_uv_rpc_client_flush_queue(
    struct raft__io_uv_rpc_client *c);

static void raft__io_uv_rpc_client_stream_close_cb(struct uv_handle_s *handle);

int raft__io_uv_rpc_client_get(struct raft__io_uv_rpc *r,
                               const unsigned id,
                               const char *address,
                               struct raft__io_uv_rpc_client **client)
{
    struct raft__io_uv_rpc_client **clients;
    unsigned n_clients;
    unsigned i;
    int rv;

    assert(r->state == RAFT__IO_UV_RPC_ACTIVE);

    /* Check if we already have a client object for this peer server. */
    for (i = 0; i < r->n_clients; i++) {
        *client = r->clients[i];

        if ((*client)->id == id) {
            /* TODO: handle a change in the address */
            assert(strcmp((*client)->address, address) == 0);
            assert((*client)->state == RAFT__IO_UV_RPC_CLIENT_CONNECTED ||
                   (*client)->state == RAFT__IO_UV_RPC_CLIENT_DELAY ||
                   (*client)->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING);
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

    rv = raft__io_uv_rpc_client_init(*client, r, id, address);
    if (rv != 0) {
        goto err_after_client_alloc;
    }

    /* This will trigger a connection attempt. */
    raft__io_uv_rpc_client_start(*client);

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

int raft__io_uv_rpc_client_send(struct raft__io_uv_rpc_client *c,
                                struct raft__io_uv_rpc_send *req)
{
    int rv;

    assert(c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTED ||
           c->state == RAFT__IO_UV_RPC_CLIENT_DELAY ||
           c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING);

    req->client = c;

    /* If there's no connection available, let's either queue the request or
     * fail immediately. */
    if (c->state == RAFT__IO_UV_RPC_CLIENT_DELAY ||
        c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING) {
        assert(c->stream == NULL);
        if (c->n_send_queue < RAFT__IO_UV_RPC_CLIENT_QUEUE_SIZE) {
            RAFT__QUEUE_PUSH(&c->send_queue, &req->queue);
            c->n_send_queue++;
            return 0;
        }
        return RAFT_ERR_IO_CONNECT;
    }

    assert(c->stream != NULL);

    rv = uv_write(&req->write, c->stream, req->bufs, req->n_bufs,
                  raft__io_uv_rpc_client_write_cb);
    if (rv != 0) {
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        return RAFT_ERR_IO;
    }
    req->write.data = req;

    return 0;
}

void raft__io_uv_rpc_client_put(struct raft__io_uv_rpc_client *c)
{
    int rv;

    assert(c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTED ||
           c->state == RAFT__IO_UV_RPC_CLIENT_DELAY ||
           c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING);

    while (!RAFT__QUEUE_IS_EMPTY(&c->send_queue)) {
        raft__queue *head;
        struct raft__io_uv_rpc_send *req;
        head = RAFT__QUEUE_HEAD(&c->send_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_rpc_send, queue);
        RAFT__QUEUE_REMOVE(head);
        raft_free(req->bufs[0].base);
        raft_free(req->bufs);
        if (req->req->cb != NULL) {
            req->req->cb(req->req, RAFT_ERR_IO_CANCELED);
        }
        raft_free(req);
    }
    c->n_send_queue = 0;

    rv = uv_timer_stop(&c->timer);
    assert(rv == 0);

    uv_close((struct uv_handle_s *)&c->timer, NULL);

    /* If we are connecting, let's cancel the connection attempt and then
     * destroy ourselves when the connect callback fails. */
    if (c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING) {
        assert(c->stream == NULL);
        c->state = RAFT__IO_UV_RPC_CLIENT_CLOSING;
        c->req.cancel(&c->req);
        return;
    }

    if (c->state == RAFT__IO_UV_RPC_CLIENT_DELAY) {
        raft__io_uv_rpc_client_close(c);
        raft_free(c);
        return;
    }

    assert(c->stream != NULL);

    /* Wait for the stream handle to be closed before releasing our memory. This
     * makes sure that the connect and write callbacks get executed before we
     * destroy ourselves. */
    uv_close((uv_handle_t *)c->stream, raft__io_uv_rpc_client_stream_close_cb);
}

static int raft__io_uv_rpc_client_init(struct raft__io_uv_rpc_client *c,
                                       struct raft__io_uv_rpc *rpc,
                                       unsigned id,
                                       const char *address)
{
    int rv;

    c->rpc = rpc;
    c->timer.data = c;
    c->req.data = c;
    c->stream = NULL;
    c->n_connect_attempt = 0;
    c->id = id;

    /* Make a copy of the address string */
    c->address = raft_malloc(strlen(address) + 1);
    if (c->address == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }
    strcpy(c->address, address);

    c->state = 0;

    RAFT__QUEUE_INIT(&c->send_queue);
    c->n_send_queue = 0;

    return 0;

err:
    assert(rv != 0);

    return rv;
}

static void raft__io_uv_rpc_client_close(struct raft__io_uv_rpc_client *c)
{
    assert(c->address != NULL);
    raft_free(c->address);
}

static void raft__io_uv_rpc_client_start(struct raft__io_uv_rpc_client *c)
{
    int rv;

    assert(c->state == 0);
    assert(c->stream == NULL);

    rv = uv_timer_init(c->rpc->loop, &c->timer);
    assert(rv == 0); /* This should never fail */

    /* Make a first connection attempt right away. */
    raft__io_uv_rpc_client_connect(c);
}

static void raft__io_uv_rpc_client_connect(struct raft__io_uv_rpc_client *c)
{
    int rv;

    assert(c->stream == NULL);

    c->n_connect_attempt++;

    /* Trigger a connection attempt. */
    rv = c->rpc->transport->connect(c->rpc->transport, &c->req, c->id,
                                    c->address,
                                    raft__io_uv_rpc_client_connect_cb);
    if (rv != 0) {
        /* Restart the timer, so we can retry. */
        c->state = RAFT__IO_UV_RPC_CLIENT_DELAY;
        rv = uv_timer_start(&c->timer, raft__io_uv_rpc_client_connect_retry_cb,
                            c->rpc->connect_retry_delay, 0);
        assert(rv == 0);
        return;
    }

    c->state = RAFT__IO_UV_RPC_CLIENT_CONNECTING;
}

static void raft__io_uv_rpc_client_connect_cb(struct raft_io_uv_connect *req,
                                              struct uv_stream_s *stream,
                                              int status)
{
    struct raft__io_uv_rpc_client *c = req->data;
    void (*log)(struct raft_logger * logger, const char *format, ...);
    int rv;

    assert(c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTING ||
           c->state == RAFT__IO_UV_RPC_CLIENT_CLOSING);
    assert(c->stream == NULL);

    /* If we were closed before the connection was fully setup, let's close the
     * stream and bail out. */
    if (c->state == RAFT__IO_UV_RPC_CLIENT_CLOSING) {
        if (status == 0) {
            uv_close((uv_handle_t *)stream, (uv_close_cb)raft_free);
        }
        raft__io_uv_rpc_client_close(c);
        raft_free(c);
        return;
    }

    /* The connection attempt was successful. We're good. */
    if (status == 0) {
        assert(stream != NULL);
        c->stream = stream;
        c->state = RAFT__IO_UV_RPC_CLIENT_CONNECTED;
        c->n_connect_attempt = 0;
        c->stream->data = c;
        raft__io_uv_rpc_client_flush_queue(c);
        return;
    }

    /* Use debug level for logging the first few attempts, then switch to
     * warn. */
    if (c->n_connect_attempt < 10) {
        log = raft_debugf;
    } else {
        log = raft_warnf;
    }

    log(c->rpc->logger, "connect to %d (%s): %s", c->id, c->address,
        raft_strerror(status));

    /* Let's schedule another attempt. */
    c->state = RAFT__IO_UV_RPC_CLIENT_DELAY;

    rv = uv_timer_start(&c->timer, raft__io_uv_rpc_client_connect_retry_cb,
                        c->rpc->connect_retry_delay, 0);
    assert(rv == 0);
}

static void raft__io_uv_rpc_client_connect_retry_cb(uv_timer_t *timer)
{
    struct raft__io_uv_rpc_client *c = timer->data;

    assert(c->state == RAFT__IO_UV_RPC_CLIENT_DELAY);
    assert(c->stream == NULL);

    /* Retry to connect. */
    raft__io_uv_rpc_client_connect(c);
}

static void raft__io_uv_rpc_client_write_cb(struct uv_write_s *write,
                                            int status)
{
    struct raft__io_uv_rpc_send *req = write->data;
    struct raft__io_uv_rpc_client *c = req->client;

    /* If the write failed and we're not currently disconnecting, let's close
     * the stream handle, and trigger a new connection
     * attempt. */
    if (status != 0) {
        if (c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTED) {
            assert(c->stream != NULL);
            uv_close((struct uv_handle_s *)c->stream, (uv_close_cb)raft_free);
            c->stream = NULL;
            c->state = RAFT__IO_UV_RPC_CLIENT_CONNECTING;
            /* Trigger a new connection attempt. */
            raft__io_uv_rpc_client_connect(c);
        }
    }

    if (req->req->cb != NULL) {
        req->req->cb(req->req, status == 0 ? 0 : RAFT_ERR_IO);
    }

    /* Just release the first buffer. Further buffers are entry payloads, which
     * we were passed but we don't own. */
    raft_free(req->bufs[0].base);

    /* Release the buffers array. */
    raft_free(req->bufs);

    raft_free(req);
}

static void raft__io_uv_rpc_client_flush_queue(struct raft__io_uv_rpc_client *c)
{
    int rv;

    assert(c->state == RAFT__IO_UV_RPC_CLIENT_CONNECTED);
    assert(c->stream != NULL);

    while (!RAFT__QUEUE_IS_EMPTY(&c->send_queue)) {
        raft__queue *head;
        struct raft__io_uv_rpc_send *req;
        head = RAFT__QUEUE_HEAD(&c->send_queue);
        req = RAFT__QUEUE_DATA(head, struct raft__io_uv_rpc_send, queue);
        RAFT__QUEUE_REMOVE(head);
        rv = raft__io_uv_rpc_client_send(c, req);
        if (rv != 0) {
            if (req->req->cb != NULL) {
                req->req->cb(req->req, rv);
            }
            raft_free(req->bufs[0].base);
            raft_free(req);
        }
    }
    c->n_send_queue = 0;
}

static void raft__io_uv_rpc_client_stream_close_cb(struct uv_handle_s *handle)
{
    struct raft__io_uv_rpc_client *c = handle->data;

    raft_free(handle);

    raft__io_uv_rpc_client_close(c);
    raft_free(c);
}
