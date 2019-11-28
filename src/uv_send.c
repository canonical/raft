#include <string.h>

#include "../include/raft/uv.h"
#include "assert.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"

/* The happy path for an raft_io_send request is:
 *
 * - Get the uvClient object whose address matches the one of target server.
 * - Encode the message and write it using the uvClient's TCP handle.
 * - Once the write completes, fire the send request callback.
 *
 * Possible failure modes are:
 *
 * - The Uv->clients queue has no client object with a matching address. In this
 *   case add a new client object to the array, add the send request to the
 *   queue of pending requests and submit a connection request. Once the
 *   connection request succeeds, try to write the encoded request to the
 *   connected stream handle. If the connection request fails, schedule another
 *   attempt.
 *
 * - The Uv->clients queue has a client object which is not connected. Add the
 *   send request to the pending queue, and, if there's no connection attempt
 *   already in progress, start a new one.
 *
 * - The write request fails (either synchronously or asynchronously). In this
 *   case we fire the request callback with an error, close the connection
 *   stream, and start a re-connection attempt.
 */

/* Set to 1 to enable tracing. */
#if 0
#define tracef(C, ...) Tracef(C->uv->tracer, __VA_ARGS__)
#else
#define tracef(C, ...)
#endif

/* Client state codes. */
enum {
    UV__CLIENT_CONNECTING = 1, /* During transport->connect() */
    UV__CLIENT_CONNECTED,      /* After a successful connect attempt */
    UV__CLIENT_DELAY,          /* Wait before another transport->connect() */
    UV__CLIENT_CLOSING,
    UV__CLIENT_CLOSED,
};

/* Maximum number of requests that can be buffered.  */
#define UV__CLIENT_MAX_PENDING 3

struct uvClient
{
    struct uv *uv;                  /* libuv I/O implementation object */
    struct uv_timer_s timer;        /* Schedule connection attempts */
    struct raft_uv_connect connect; /* Connection request */
    struct uv_stream_s *stream;     /* Current connection handle */
    struct uv_stream_s *old_stream; /* Connection handle being closed */
    unsigned n_connect_attempt;     /* Consecutive connection attempts */
    unsigned id;                    /* ID of the other server */
    char *address;                  /* Address of the other server */
    int state;                      /* Current client state */
    queue pending;                  /* Pending send message requests */
    queue queue;                    /* Clients queue */
};

/* Hold state for a single send RPC message request. */
struct uvSend
{
    struct uvClient *client;  /* Client connected to the target server */
    struct raft_io_send *req; /* Uer request */
    uv_buf_t *bufs;           /* Encoded raft RPC message to send */
    unsigned n_bufs;          /* Number of buffers */
    uv_write_t write;         /* Stream write request */
    queue queue;              /* Pending send requests queue */
};

/* Free all memory used by the given send request object, including the object
 * itself. */
static void uvSendDestroy(struct uvSend *s)
{
    if (s->bufs != NULL) {
        /* Just release the first buffer. Further buffers are entry or snapshot
         * payloads, which we were passed but we don't own. */
        HeapFree(s->bufs[0].base);

        /* Release the buffers array. */
        HeapFree(s->bufs);
    }
    HeapFree(s);
}

/* Initialize a new client associated with the given server. */
static int uvClientInit(struct uvClient *c,
                        struct uv *uv,
                        unsigned id,
                        const char *address)
{
    int rv;
    c->uv = uv;
    c->timer.data = c;
    c->connect.data = NULL; /* Set upon starting a connect request */
    c->stream = NULL;       /* Set upon successful connection */
    c->old_stream = NULL;   /* Set after closing the current connection */
    c->n_connect_attempt = 0;
    c->id = id;
    c->address = HeapMalloc(strlen(address) + 1);
    if (c->address == NULL) {
        return RAFT_NOMEM;
    }
    strcpy(c->address, address);
    c->state = 0;
    QUEUE_INIT(&c->pending);
    rv = uv_timer_init(c->uv->loop, &c->timer);
    assert(rv == 0);
    return 0;
}

/* If there's no more pending cleanup, remove the client from the abort queue
 * and destroy it. */
static void uvClientMaybeDestroy(struct uvClient *c)
{
    struct uv *uv = c->uv;
    if (c->connect.data != NULL) {
        return;
    }
    if (c->timer.data != NULL) {
        return;
    }
    if (c->stream != NULL) {
        return;
    }

    while (!QUEUE_IS_EMPTY(&c->pending)) {
        queue *head;
        struct uvSend *send;
        struct raft_io_send *req;
        head = QUEUE_HEAD(&c->pending);
        send = QUEUE_DATA(head, struct uvSend, queue);
        QUEUE_REMOVE(head);
        req = send->req;
        uvSendDestroy(send);
        if (req->cb != NULL) {
            req->cb(req, RAFT_CANCELED);
        }
    }

    assert(c->address != NULL);
    QUEUE_REMOVE(&c->queue);
    HeapFree(c->address);
    HeapFree(c);
    uvMaybeFireCloseCb(uv);
}

/* Forward declaration. */
static void uvClientAttemptConnect(struct uvClient *c);

/* Invoked once an encoded RPC message has been written out. */
static void uvClientWriteCb(struct uv_write_s *write, const int status)
{
    struct uvSend *send = write->data;
    struct uvClient *c = send->client;
    struct raft_io_send *req = send->req;
    int cb_status = 0;

    tracef(c, "message write completed -> status %d", status);

    /* If the write failed and we're not currently disconnecting, let's close
     * the stream handle, and trigger a new connection attempt. */
    if (status != 0) {
        cb_status = RAFT_IOERR;
        if (c->state == UV__CLIENT_CONNECTED) {
            assert(status != UV_ECANCELED);
            assert(c->stream != NULL);
            uv_close((struct uv_handle_s *)c->stream, (uv_close_cb)HeapFree);
            c->stream = NULL;
            uvClientAttemptConnect(c); /* Trigger a new connection attempt. */
        } else if (status == UV_ECANCELED) {
            cb_status = RAFT_CANCELED;
        }
    }

    uvSendDestroy(send);

    if (req->cb != NULL) {
        req->cb(req, cb_status);
    }
}

static int uvClientSend(struct uvClient *c, struct uvSend *send)
{
    int rv;
    assert(c->state == UV__CLIENT_CONNECTED || c->state == UV__CLIENT_DELAY ||
           c->state == UV__CLIENT_CONNECTING);
    send->client = c;

    /* If there's no connection available, let's queue the request. */
    if (c->stream == NULL) {
        tracef(c, "no connection available -> enqueue message");
        QUEUE_PUSH(&c->pending, &send->queue);
        return 0;
    }

    tracef(c, "connection available -> write message");
    send->write.data = send;
    rv = uv_write(&send->write, c->stream, send->bufs, send->n_bufs,
                  uvClientWriteCb);
    if (rv != 0) {
        tracef(c, "write message failed -> rv %d", rv);
        /* UNTESTED: what are the error conditions? perhaps ENOMEM */
        return RAFT_IOERR;
    }

    return 0;
}

/* Try to execute all send requests that were blocked in the queue waiting for a
 * connection. */
static void uvClientFlushPending(struct uvClient *c)
{
    int rv;
    assert(c->state == UV__CLIENT_CONNECTED);
    assert(c->stream != NULL);
    tracef(c, "flush pending messages");
    while (!QUEUE_IS_EMPTY(&c->pending)) {
        queue *head;
        struct uvSend *send;
        head = QUEUE_HEAD(&c->pending);
        send = QUEUE_DATA(head, struct uvSend, queue);
        QUEUE_REMOVE(head);
        rv = uvClientSend(c, send);
        if (rv != 0) {
            if (send->req->cb != NULL) {
                send->req->cb(send->req, rv);
            }
            uvSendDestroy(send);
        }
    }
}

static void uvClientTimerCb(uv_timer_t *timer)
{
    struct uvClient *c = timer->data;
    assert(c->state == UV__CLIENT_DELAY);
    assert(c->stream == NULL);
    tracef(c, "timer expired -> attempt to reconnect");
    uvClientAttemptConnect(c); /* Retry to connect. */
}

/* Return the number of send requests that we have been parked in the send queue
 * because no connection is available yet. */
static unsigned uvClientPendingCount(struct uvClient *c)
{
    queue *head;
    unsigned n = 0;
    QUEUE_FOREACH(head, &c->pending) { n++; }
    return n;
}

static void uvClientConnectCb(struct raft_uv_connect *req,
                              struct uv_stream_s *stream,
                              int status)
{
    struct uvClient *c = req->data;
    unsigned n_pending;
    int level = RAFT_DEBUG;
    int rv;

    tracef(c, "connect attempt completed -> status %s",
           errCodeToString(status));

    assert(c->state == UV__CLIENT_CONNECTING || c->state == UV__CLIENT_CLOSING);
    assert(c->stream == NULL);

    c->connect.data = NULL;

    /* If we are closing, bail out, possibly discarding the new connection. */
    if (c->state == UV__CLIENT_CLOSING) {
        if (status == 0) {
            uv_close((struct uv_handle_s *)stream, (uv_close_cb)HeapFree);
        }
        uvClientMaybeDestroy(c);
        return;
    }

    assert(c->state == UV__CLIENT_CONNECTING);

    /* If, the connection attempt was successful, we're good. */
    if (status == 0) {
        assert(stream != NULL);
        c->stream = stream;
        c->state = UV__CLIENT_CONNECTED;
        c->n_connect_attempt = 0;
        c->stream->data = c;
        uvClientFlushPending(c);
        return;
    }

    /* Shrink the queue of pending requests, by failing the oldest ones */
    n_pending = uvClientPendingCount(c);
    if (n_pending > UV__CLIENT_MAX_PENDING) {
        unsigned i;
        for (i = 0; i < n_pending - UV__CLIENT_MAX_PENDING; i++) {
            tracef(c, "queue full -> evict oldest message");
            queue *head;
            struct uvSend *old_send;
            struct raft_io_send *old_req;
            head = QUEUE_HEAD(&c->pending);
            old_send = QUEUE_DATA(head, struct uvSend, queue);
            QUEUE_REMOVE(head);
            old_req = old_send->req;
            uvSendDestroy(old_send);
            if (old_req->cb != NULL) {
                old_req->cb(old_req, RAFT_NOCONNECTION);
            }
        }
    }

    /* Use debug level for logging the first few attempts, then switch to
     * warn, but not too agressively. */
    if (c->n_connect_attempt >= 100 && c->n_connect_attempt % 30 == 0) {
        level = RAFT_WARN;
    }

    /*c->uv->logger->emit(c->uv->logger, level,
                        c->uv->io->time(c->uv->io), "connect to %d (%s): %s",
                        c->id, c->address, raft_strerror(status));*/
    (void)level;

    /* Let's schedule another attempt. */
    c->state = UV__CLIENT_DELAY;
    rv = uv_timer_start(&c->timer, uvClientTimerCb, c->uv->connect_retry_delay,
                        0);
    assert(rv == 0);
}

/* Perform a single connection attempt, scheduling a retry if it fails. */
static void uvClientAttemptConnect(struct uvClient *c)
{
    int rv;
    assert(c->stream == NULL);

    c->n_connect_attempt++;
    c->state = UV__CLIENT_CONNECTING;

    c->connect.data = c;
    rv = c->uv->transport->connect(c->uv->transport, &c->connect, c->id,
                                   c->address, uvClientConnectCb);
    if (rv != 0) {
        /* Restart the timer, so we can retry. */
        c->connect.data = NULL;
        c->state = UV__CLIENT_DELAY;
        rv = uv_timer_start(&c->timer, uvClientTimerCb,
                            c->uv->connect_retry_delay, 0);
        assert(rv == 0);
    }
}

static void uvStreamCloseCb(struct uv_handle_s *handle)
{
    struct uvClient *c = handle->data;
    assert(handle == (struct uv_handle_s *)c->stream);
    HeapFree(c->stream);
    c->stream = NULL;
    uvClientMaybeDestroy(c);
}

/* Final callback in the close chain of an io_uv__client object */
static void uvClientTimerCloseCb(struct uv_handle_s *handle)
{
    struct uvClient *c = handle->data;
    assert(handle == (struct uv_handle_s *)&c->timer);
    c->timer.data = NULL;
    uvClientMaybeDestroy(c);
}

/* Start shutting down a client since the raft_io instance has been closed. */
static void uvClientAbort(struct uvClient *c)
{
    struct uv *uv = c->uv;
    int rv;

    assert(uv->closing);
    assert(c->state == UV__CLIENT_CONNECTED || c->state == UV__CLIENT_DELAY ||
           c->state == UV__CLIENT_CONNECTING);

    QUEUE_REMOVE(&c->queue);
    QUEUE_PUSH(&uv->aborting, &c->queue);

    rv = uv_timer_stop(&c->timer);
    assert(rv == 0);

    /* If we are connected, let's close the outbound stream handle. This will
     * eventually complete all inflight write requests, possibly with failing
     * them with UV_ECANCELED. */
    if (c->stream != NULL) {
        assert(c->state == UV__CLIENT_CONNECTED);
        tracef(c, "client stopped -> close outbound stream");
        uv_close((uv_handle_t *)c->stream, uvStreamCloseCb);
    }

    /* Closing the timer implicitely stop it, so the timeout callback won't be
     * fired. */
    uv_close((struct uv_handle_s *)&c->timer, uvClientTimerCloseCb);
    c->state = UV__CLIENT_CLOSING;
}

/* Find the client object associated with the given server, or create one if
 * there's none yet. */
static int uvGetClient(struct uv *uv,
                       const unsigned id,
                       const char *address,
                       struct uvClient **client)
{
    queue *head;
    int rv;

    /* Check if we already have a client object for this peer server. */
    QUEUE_FOREACH(head, &uv->clients)
    {
        *client = QUEUE_DATA(head, struct uvClient, queue);
        if ((*client)->id != id) {
            continue;
        }
        /* TODO: handle a change in the address */
        /* assert(strcmp((*client)->address, address) == 0); */
        assert((*client)->state == UV__CLIENT_CONNECTING ||
               (*client)->state == UV__CLIENT_CONNECTED ||
               (*client)->state == UV__CLIENT_DELAY);
        return 0;
    }

    /* Initialize the new connection */
    *client = HeapMalloc(sizeof **client);
    if (*client == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    rv = uvClientInit(*client, uv, id, address);
    if (rv != 0) {
        goto err_after_client_alloc;
    }
    QUEUE_PUSH(&uv->clients, &(*client)->queue);

    /* Make a first connection attempt right away.. */
    uvClientAttemptConnect(*client);
    assert((*client)->state != 0);

    return 0;

err_after_client_alloc:
    HeapFree(*client);
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
    struct uvSend *send;
    struct uvClient *client;
    int rv;

    assert(!uv->closing);

    /* Allocate a new request object. */
    send = HeapMalloc(sizeof *send);
    if (send == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }
    send->req = req;
    req->cb = cb;

    rv = uvEncodeMessage(message, &send->bufs, &send->n_bufs);
    if (rv != 0) {
        send->bufs = NULL;
        goto err_after_send_alloc;
    }

    /* Get a client object connected to the target server, creating it if it
     * doesn't exist yet. */
    rv = uvGetClient(uv, message->server_id, message->server_address, &client);
    if (rv != 0) {
        goto err_after_send_alloc;
    }

    rv = uvClientSend(client, send);
    if (rv != 0) {
        goto err_after_send_alloc;
    }

    return 0;

err_after_send_alloc:
    uvSendDestroy(send);
err:
    assert(rv != 0);
    return rv;
}

void uvSendClose(struct uv *uv)
{
    assert(uv->closing);
    while (!QUEUE_IS_EMPTY(&uv->clients)) {
        queue *head;
        struct uvClient *client;
        head = QUEUE_HEAD(&uv->clients);
        client = QUEUE_DATA(head, struct uvClient, queue);
        uvClientAbort(client);
    }
}
