#include <string.h>

#include "assert.h"
#include "binary.h"
#include "io_uv_encoding.h"
#include "io_uv_rpc.h"
#include "io_uv_rpc_client.h"
#include "io_uv_rpc_server.h"

/**
 * Retry to connect every second.
 *
 * TODO: implement an exponential backoff instead.
 */
#define RAFT__IO_UV_RPC_CLIENT_CONNECT_RETRY_DELAY 1000

/* Invoked when a new incoming connection has been established. */
static void raft__io_uv_rpc_accept_cb(struct raft_io_uv_transport *t,
                                      unsigned id,
                                      const char *address,
                                      struct uv_stream_s *stream);

static void raft__io_uv_rpc_maybe_stopped(struct raft__io_uv_rpc *r);

int raft__io_uv_rpc_init(struct raft__io_uv_rpc *r,
                         struct raft_logger *logger,
                         struct uv_loop_s *loop,
                         struct raft_io_uv_transport *transport)
{
    r->logger = logger;
    r->loop = loop;
    r->transport = transport;
    r->transport->data = r;

    r->clients = NULL;
    r->servers = NULL;
    r->n_clients = 0;
    r->n_servers = 0;
    r->connect_retry_delay = RAFT__IO_UV_RPC_CLIENT_CONNECT_RETRY_DELAY;
    r->state = 0;

    r->recv_cb = NULL;
    r->close_cb = NULL;

    return 0;
}

int raft__io_uv_rpc_start(struct raft__io_uv_rpc *r,
			  raft__io_uv_rpc_recv_cb recv_cb)
{
    int rv;

    assert(r->state == 0);

    r->recv_cb = recv_cb;

    rv = r->transport->listen(r->transport, raft__io_uv_rpc_accept_cb);
    if (rv != 0) {
        return rv;
    }

    r->state = RAFT__IO_UV_RPC_ACTIVE;

    return 0;
}

int raft__io_uv_rpc_send(struct raft__io_uv_rpc *r,
                         struct raft_io_send *send,
                         const struct raft_message *message,
                         raft_io_send_cb cb)
{
    struct raft__io_uv_rpc_send *req;
    struct raft__io_uv_rpc_client *client;
    int rv;

    assert(r->state == RAFT__IO_UV_RPC_ACTIVE);

    /* Allocate a new request object. */
    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    req->req = send;

    send->impl = req;
    send->cb = cb;

    rv = raft_io_uv_encode__message(message, &req->bufs, &req->n_bufs);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    /* Get a client object connected to the target server, creating it if it
     * doesn't exist yet. */
    rv = raft__io_uv_rpc_client_get(r, message->server_id,
                                    message->server_address, &client);
    if (rv != 0) {
        goto err_after_request_encode;
    }

    rv = raft__io_uv_rpc_client_send(client, req);
    if (rv != 0) {
        goto err_after_request_encode;
    }

    return 0;

err_after_request_encode:
    /* Just release the first buffer. Further buffers are entry payloads, which
     * we were passed but we don't own. */
    raft_free(req->bufs[0].base);

    /* Release the buffers array. */
    raft_free(req->bufs);

err_after_request_alloc:
    raft_free(req);

err:
    assert(rv != 0);

    return rv;
}

void raft__io_uv_rpc_close(struct raft__io_uv_rpc *r,
                           raft__io_uv_rpc_close_cb cb)
{
    unsigned i;

    assert(r->state == RAFT__IO_UV_RPC_ACTIVE);

    r->close_cb = cb;
    r->state = RAFT__IO_UV_RPC_CLOSING;

    r->transport->stop(r->transport);

    for (i = 0; i < r->n_clients; i++) {
        raft__io_uv_rpc_client_put(r->clients[i]);
    }

    for (i = 0; i < r->n_servers; i++) {
        raft__io_uv_rpc_server_stop(r->servers[i]);
    }

    raft__io_uv_rpc_maybe_stopped(r);
}

static void raft__io_uv_rpc_accept_cb(struct raft_io_uv_transport *t,
                                      unsigned id,
                                      const char *address,
                                      struct uv_stream_s *stream)
{
    struct raft__io_uv_rpc *r = t->data;
    int rv;

    assert(r->state == RAFT__IO_UV_RPC_ACTIVE);

    rv = raft__io_uv_rpc_add_server(r, id, address, stream);
    if (rv != 0) {
        raft_warnf(r->logger, "add server: %s", raft_strerror(rv));
        uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
    }
}

/**
 * Request to stop this incoming connection, eventually releasing all associated
 * resources.
 */

/**
 * Invoke the stop callback if we were asked to be stopped and there are no more
 * pending asynchronous activities.
 */
static void raft__io_uv_rpc_maybe_stopped(struct raft__io_uv_rpc *r)
{
    assert(r->state == RAFT__IO_UV_RPC_CLOSING);

    if (r->close_cb == NULL) {
        return;
    }

    if (r->clients != NULL) {
        raft_free(r->clients);
    }

    if (r->servers != NULL) {
        raft_free(r->servers);
    }

    r->state = RAFT__IO_UV_RPC_CLOSED;
    r->close_cb(r);
}
