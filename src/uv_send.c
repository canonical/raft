#include <string.h>

#include "../include/raft/uv.h"

#include "assert.h"
#include "uv.h"
#include "uv_encoding.h"

/* The happy path for an io_uv_send request is:
 *
 * - Get the io_uv_client object whose address matches the one of target server.
 * - Encode the message write buffers into the client->stream handle.  Once the
 * - write completes, fire the send request callback.
 *
 * Possible failure modes are:
 *
 * - The io_uv->clients array has no client object with a matching address. In
 *   this case add a new client object to the array, add the send request to the
 *   queue of pending requests and submit a connection request. Once the
 *   connection request succeeds, try to write the encoded request to the
 *   connected stream handle. If the connection request fails, schedule another
 *   attempt.
 *
 * - The io_uv->clients array has a client object which is not connected. Add
 *   the send request to the pending queue, and, if there's no connection
 *   attempt already in progress, start a new one.
 *
 * - The write request fails (either synchronously or asynchronously). In this
 *   case we fire the request callback with an error, close the connection
 *   stream, and start a re-connection attempt.
 */

/* Set to 1 to enable tracing. */
#if 0
#define tracef(C, MSG, ...) uvDebugf(C->uv, MSG, ##__VA_ARGS__)
#else
#define tracef(C, MSG, ...)
#endif

/* Client state codes. */
enum {
    CONNECTING = 1,
    CONNECTED,
    DELAY,
    CLOSING,
    CLOSED,
};

/* Maximum number of requests that can be buffered.  */
#define QUEUE_SIZE 3

struct uvClient
{
    struct uv *uv;                  /* libuv I/O implementation object */
    struct uv_timer_s timer;        /* Schedule connection attempts */
    struct raft_uv_connect connect; /* Connection request */
    struct uv_stream_s *stream;     /* Connection handle */
    unsigned n_connect_attempt;     /* Consecutive connection attempts */
    unsigned id;                    /* ID of the other server */
    char *address;                  /* Address of the other server */
    int state;                      /* Current client state */
    queue send_reqs;                /* Pending send message requests */
    unsigned n_send_reqs;           /* Number of pending send requests */
};

/* Hold state for a single send RPC message request. */
struct send
{
    struct uvClient *c;       /* Client connected to the target server */
    struct raft_io_send *req; /* Uer request */
    uv_buf_t *bufs;           /* Encoded raft RPC message to send */
    unsigned n_bufs;          /* Number of buffers */
    uv_write_t write;         /* Stream write request */
    queue queue;              /* Pending send requests queue */
};

/* Free all memory used by the given send request object. */
static void closeRequest(struct send *r)
{
    /* Just release the first buffer. Further buffers are entry payloads, which
     * we were passed but we don't own. */
    raft_free(r->bufs[0].base);

    /* Release the buffers array. */
    raft_free(r->bufs);
}

static void copyAddress(const char *address1, char **address2)
{
    *address2 = raft_malloc(strlen(address1) + 1);
    if (*address2 == NULL) {
        return;
    }
    strcpy(*address2, address1);
}

/* Initialize a new client associated with the given server. */
static int initClient(struct uvClient *c,
                      struct uv *uv,
                      unsigned id,
                      const char *address)
{
    c->uv = uv;
    c->timer.data = c;
    c->connect.data = c;
    c->stream = NULL;
    c->n_connect_attempt = 0;
    c->id = id;
    copyAddress(address, &c->address); /* Make a copy of the address string */
    if (c->address == NULL) {
        return RAFT_NOMEM;
    }
    c->state = 0;
    QUEUE_INIT(&c->send_reqs);
    c->n_send_reqs = 0;

    return 0;
}

/* Final callback in the close chain of an io_uv__client object */
static void timerCloseCb(struct uv_handle_s *handle)
{
    struct uvClient *c = handle->data;
    assert(c->address != NULL);
    raft_free(c->address);
    raft_free(c);
}

/* Invoked once an encoded RPC message has been written out. */
static void startConnecting(struct uvClient *c);
static void writeCb(struct uv_write_s *write, const int status)
{
    struct send *r = write->data;
    struct uvClient *c = r->c;
    int cb_status = 0;

    tracef(c, "message write completed -> status %d", status);

    /* If the write failed and we're not currently disconnecting, let's close
     * the stream handle, and trigger a new connection
     * attempt. */
    if (status != 0) {
        cb_status = RAFT_IOERR;
        if (c->state == CONNECTED) {
            assert(status != UV_ECANCELED);
            assert(c->stream != NULL);
            uv_close((struct uv_handle_s *)c->stream, (uv_close_cb)raft_free);
            c->stream = NULL;
            c->state = CONNECTING;
            startConnecting(c); /* Trigger a new connection attempt. */
        } else if (status == UV_ECANCELED) {
            cb_status = RAFT_CANCELED;
        }
    }

    if (r->req->cb != NULL) {
        r->req->cb(r->req, cb_status);
    }

    closeRequest(r);
    raft_free(r);
}

int sendMessage(struct uvClient *c, struct send *r)
{
    int rv;
    assert(c->state == CONNECTED || c->state == DELAY ||
           c->state == CONNECTING);
    r->c = c;

    /* If there's no connection available, let's queue the request. */
    if (c->state == DELAY || c->state == CONNECTING) {
        assert(c->stream == NULL);
        if (c->n_send_reqs == QUEUE_SIZE) {
            /* Fail the oldest request */
            tracef(c, "queue full -> evict oldest message");
            queue *head;
            struct send *r2;
            head = QUEUE_HEAD(&c->send_reqs);
            r2 = QUEUE_DATA(head, struct send, queue);
            QUEUE_REMOVE(head);
            r2->req->cb(r2->req, RAFT_NOCONNECTION);
            closeRequest(r2);
            raft_free(r2);
            c->n_send_reqs--;
        }
        tracef(c, "no connection available -> enqueue message");
        QUEUE_PUSH(&c->send_reqs, &r->queue);
        c->n_send_reqs++;
        return 0;
    }

    assert(c->stream != NULL);
    tracef(c, "connection available -> write message");
    rv = uv_write(&r->write, c->stream, r->bufs, r->n_bufs, writeCb);
    if (rv != 0) {
        tracef(c, "write message failed -> rv %d", rv);
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        return RAFT_IOERR;
    }
    r->write.data = r;

    return 0;
}

/* Try to execute all send requests that were blocked in the queue waiting for a
 * connection. */
static void flushQueue(struct uvClient *c)
{
    int rv;
    assert(c->state == CONNECTED);
    assert(c->stream != NULL);
    tracef(c, "flush pending messages");
    while (!QUEUE_IS_EMPTY(&c->send_reqs)) {
        queue *head;
        struct send *r;
        head = QUEUE_HEAD(&c->send_reqs);
        r = QUEUE_DATA(head, struct send, queue);
        QUEUE_REMOVE(head);
        rv = sendMessage(c, r);
        if (rv != 0) {
            if (r->req->cb != NULL) {
                r->req->cb(r->req, rv);
            }
            closeRequest(r);
            raft_free(r);
        }
    }
    c->n_send_reqs = 0;
}

static void timerCb(uv_timer_t *timer)
{
    struct uvClient *c = timer->data;
    assert(c->state == DELAY);
    assert(c->stream == NULL);
    tracef(c, "timer expired -> attempt to reconnect");
    startConnecting(c); /* Retry to connect. */
}

static void connectCb(struct raft_uv_connect *req,
                      struct uv_stream_s *stream,
                      int status)
{
    struct uvClient *c = req->data;
    int level = RAFT_DEBUG;
    int rv;

    tracef(c, "connect attempt completed -> status %d", status);

    assert(c->state == CONNECTING || c->state == CLOSING);
    assert(c->stream == NULL);

    /* If the transport has been closed before the connection was fully setup,
     * it means that we're shutting down: let's bail out. */
    if (status == RAFT_CANCELED) {
        /* We must be careful to not reference c->uv, since that io_uv object
         * might have been released already. */
        assert(stream == NULL);
        assert(c->state == CLOSING);
        uv_close((struct uv_handle_s *)&c->timer, timerCloseCb);
        return;
    }

    /* TODO: this should not happen, but makes LXD heartbeat unit test fail:
     * understand why. */
    if (status == 0 && c->state == CLOSING) {
        uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
        uv_close((struct uv_handle_s *)&c->timer, timerCloseCb);
        return;
    }

    assert(c->state == CONNECTING);

    /* The connection attempt was successful. We're good. */
    if (status == 0) {
        assert(stream != NULL);
        c->stream = stream;
        c->state = CONNECTED;
        c->n_connect_attempt = 0;
        c->stream->data = c;
        flushQueue(c);
        return;
    }

    /* Use debug level for logging the first few attempts, then switch to
     * warn, but not too agressively. */
    if (c->n_connect_attempt >= 100 && c->n_connect_attempt % 30 == 0) {
        level = RAFT_WARN;
    }

    c->uv->io->emit(c->uv->io, level, "connect to %d (%s): %s", c->id,
                    c->address, raft_strerror(status));

    /* Let's schedule another attempt. */
    c->state = DELAY;
    rv = uv_timer_start(&c->timer, timerCb, c->uv->connect_retry_delay, 0);
    assert(rv == 0);
}

/* Perform a single connection attempt, scheduling a retry if it fails. */
static void startConnecting(struct uvClient *c)
{
    int rv;
    assert(c->stream == NULL);

    c->n_connect_attempt++;
    rv = c->uv->transport->connect(c->uv->transport, &c->connect, c->id,
                                   c->address, connectCb);
    if (rv != 0) {
        /* Restart the timer, so we can retry. */
        c->state = DELAY;
        rv = uv_timer_start(&c->timer, timerCb, c->uv->connect_retry_delay, 0);
        assert(rv == 0);
        return;
    }

    c->state = CONNECTING;
}

static int getClient(struct uv *uv,
                     const unsigned id,
                     const char *address,
                     struct uvClient **client)
{
    struct uvClient **clients;
    unsigned n_clients;
    unsigned i;
    int rv;

    /* Check if we already have a client object for this peer server. */
    for (i = 0; i < uv->n_clients; i++) {
        *client = uv->clients[i];

        if ((*client)->id == id) {
            /* TODO: handle a change in the address */
            /* assert(strcmp((*client)->address, address) == 0); */
            assert((*client)->state == CONNECTED || (*client)->state == DELAY ||
                   (*client)->state == CONNECTING);
            return 0;
        }
    }

    /* Grow the connections array */
    n_clients = uv->n_clients + 1;
    clients = raft_realloc(uv->clients, n_clients * sizeof *clients);
    if (clients == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    uv->clients = clients;
    uv->n_clients = n_clients;

    /* Initialize the new connection */
    *client = raft_malloc(sizeof **client);
    if (*client == NULL) {
        rv = RAFT_NOMEM;
        goto err_after_clients_realloc;
    }

    clients[n_clients - 1] = *client;

    rv = initClient(*client, uv, id, address);
    if (rv != 0) {
        goto err_after_client_alloc;
    }

    /* Start the client by making the first connection attempt. */
    rv = uv_timer_init((*client)->uv->loop, &(*client)->timer);
    assert(rv == 0);
    startConnecting(*client); /* Make a first connection attempt right away. */
    assert((*client)->state != 0);

    return 0;

err_after_client_alloc:
    raft_free(*client);

err_after_clients_realloc:
    /* Simply pretend that the connection was not inserted at all */
    uv->n_clients--;

err:
    assert(rv != 0);

    return rv;
}

int uvSend(struct raft_io *io,
           struct raft_io_send *req,
           const struct raft_message *message,
           raft_io_send_cb cb)
{
    struct uv *uv = io->impl;
    struct send *r;
    struct uvClient *c;
    int rv;

    assert(uv->state == UV__ACTIVE);

    /* Allocate a new request object. */
    r = raft_malloc(sizeof *r);
    if (r == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    r->req = req;
    req->cb = cb;

    rv = uvEncodeMessage(message, &r->bufs, &r->n_bufs);
    if (rv != 0) {
        goto err_after_request_alloc;
    }

    /* Get a client object connected to the target server, creating it if it
     * doesn't exist yet. */
    rv = getClient(uv, message->server_id, message->server_address, &c);
    if (rv != 0) {
        goto err_after_request_encode;
    }

    rv = sendMessage(c, r);
    if (rv != 0) {
        goto err_after_request_encode;
    }

    return 0;

err_after_request_encode:
    closeRequest(r);
err_after_request_alloc:
    raft_free(r);
err:
    assert(rv != 0);
    return rv;
}

static void streamCloseCb(struct uv_handle_s *handle)
{
    struct uvClient *c = handle->data;
    raft_free(handle);
    uv_close((struct uv_handle_s *)&c->timer, timerCloseCb);
}

static void closeClient(struct uvClient *c)
{
    int rv;

    assert(c->state == CONNECTED || c->state == DELAY ||
           c->state == CONNECTING);
    while (!QUEUE_IS_EMPTY(&c->send_reqs)) {
        queue *head;
        struct send *r;
        head = QUEUE_HEAD(&c->send_reqs);
        r = QUEUE_DATA(head, struct send, queue);
        QUEUE_REMOVE(head);
        if (r->req->cb != NULL) {
            r->req->cb(r->req, RAFT_CANCELED);
        }
        closeRequest(r);
        raft_free(r);
    }

    rv = uv_timer_stop(&c->timer);
    assert(rv == 0);

    /* If we are connecting, do nothing. The transport should have been closed
     * too and eventually it should invoke the connect callback. */
    if (c->state == CONNECTING) {
        goto out;
    }

    /* If we are waiting for the connect retry delay to expire, cancel the
     * timer, by closing it. */
    if (c->state == DELAY) {
        uv_close((struct uv_handle_s *)&c->timer, timerCloseCb);
        goto out;
    }

    /* If we are connected, let's close the outbound stream handle. This will
     * eventually make all inflight write request fail with UV_ECANCELED.
     *
     * Wait for the stream handle to be closed before releasing our memory. This
     * makes sure that the connect and write callbacks get executed before we
     * destroy ourselves. */
    assert(c->stream != NULL);
    tracef(c, "client stopped -> close outbound stream");
    uv_close((uv_handle_t *)c->stream, streamCloseCb);

out:
    c->state = CLOSING;
}

void uvSendClose(struct uv *uv)
{
    unsigned i;
    for (i = 0; i < uv->n_clients; i++) {
        closeClient(uv->clients[i]);
    }
}
