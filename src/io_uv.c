#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

#include "assert.h"
#include "binary.h"
#include "io_uv_rpc.h"
#include "io_uv_store.h"

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    struct raft_logger *logger;             /* Logger */
    struct uv_loop_s *loop;                 /* UV event loop */
    struct raft_io_uv_transport *transport; /* Network transport */
    struct raft_io_uv_store store;          /* Implement on-disk persistance */
    struct raft_io_uv_rpc rpc;              /* Implement network RPC */
    struct uv_timer_s ticker;               /* Timer for periodic ticks */
    uint64_t last_tick;                     /* Timestamp of the last tick */

    /* Track the number of active asynchronous operations that need to be
     * completed before this backend can be considered fully stopped. */
    unsigned n_active;

    struct
    {
        void *data;
        void (*cb)(void *data, const unsigned msecs);
    } tick;

    struct
    {
        void *data;
        void (*cb)(void *data);
    } stop;
};

/**
 * Periodic tick timer callback, for invoking the ticker function.
 */
static void raft_io_uv__ticker_cb(uv_timer_t *ticker)
{
    struct raft_io_uv *uv;
    uint64_t now;

    uv = ticker->data;

    /* Get current time */
    now = uv_now(uv->loop);

    /* Invoke the ticker */
    uv->tick.cb(uv->tick.data, now - uv->last_tick);

    /* Update the last tick timestamp */
    uv->last_tick = now;
}

/**
 * Start the backend.
 */
static int raft_io_uv__start(const struct raft_io *io,
                             unsigned id,
                             const char *address,
                             unsigned msecs,
                             void *data,
                             void (*tick)(void *data, unsigned elapsed),
                             void (*recv)(void *data, struct raft_message *msg))
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    assert(uv->tick.data == NULL);
    assert(uv->tick.cb == NULL);

    uv->tick.data = data;
    uv->tick.cb = tick;

    uv->last_tick = uv_now(uv->loop);

    /* Initialize the tick timer handle. */
    rv = uv_timer_init(uv->loop, &uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_timer_init: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err;
    }
    uv->ticker.data = uv;

    /* Start the tick timer handle. */
    rv = uv_timer_start(&uv->ticker, raft_io_uv__ticker_cb, 0, msecs);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_timer_start: %s", uv_strerror(rv));
        rv = RAFT_ERR_IO;
        goto err_after_ticker_init;
    }
    uv->n_active++;

    rv = raft_io_uv_store__start(&uv->store);
    if (rv != 0) {
        goto err_after_ticker_start;
    }
    uv->n_active++;

    rv = raft_io_uv_rpc__start(&uv->rpc, id, address, data, recv);
    if (rv != 0) {
        goto err_after_store_start;
    }
    uv->n_active++;

    return 0;

err_after_store_start:
    raft_io_uv_store__stop(&uv->store, NULL, NULL);

err_after_ticker_start:
    uv_timer_stop(&uv->ticker);

err_after_ticker_init:
    uv_close((struct uv_handle_s *)&uv->ticker, NULL);
err:
    assert(rv != 0);

    return rv;
}

/**
 * Invoke the stop callback if we were asked to be stopped and there are no more
 * pending asynchronous activities.
 */
static void raft_io_uv__maybe_stopped(struct raft_io_uv *uv)
{
    if (uv->stop.cb != NULL && uv->n_active == 0) {
        uv->stop.cb(uv->stop.data);

        uv->stop.data = NULL;
        uv->stop.cb = NULL;
    }
}

static void raft_io_uv__ticker_close_cb(uv_handle_t *handle)
{
    struct raft_io_uv *uv = handle->data;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static void raft_io_uv__store_stop_cb(void *p)
{
    struct raft_io_uv *uv = p;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

static void raft_io_uv__rpc_stop_cb(void *p)
{
    struct raft_io_uv *uv = p;

    uv->n_active--;

    raft_io_uv__maybe_stopped(uv);
}

/**
 * Stop the backend.
 */
static int raft_io_uv__stop(const struct raft_io *io,
                            void *data,
                            void (*cb)(void *p))
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    assert(uv->stop.data == NULL);
    assert(uv->stop.cb == NULL);

    uv->stop.data = data;
    uv->stop.cb = cb;

    /* Stop the tick timer handle. */
    rv = uv_timer_stop(&uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->logger, "uv_stop_timer: %s", uv_strerror(rv));
        return RAFT_ERR_INTERNAL;
    }

    /* Close the ticker timer handle. */
    uv_close((uv_handle_t *)&uv->ticker, raft_io_uv__ticker_close_cb);

    raft_io_uv_store__stop(&uv->store, uv, raft_io_uv__store_stop_cb);

    raft_io_uv_rpc__stop(&uv->rpc, uv, raft_io_uv__rpc_stop_cb);

    return 0;
}

static int raft_io_uv__load(const struct raft_io *io,
                            raft_term *term,
                            unsigned *voted_for,
                            raft_index *start_index,
                            struct raft_entry **entries,
                            size_t *n_entries)
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_store__load(&uv->store, term, voted_for, start_index,
                                  entries, n_entries);
}

static int raft_io_uv__bootstrap(const struct raft_io *io,
                                 const struct raft_configuration *conf)
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_store__bootstrap(&uv->store, conf);
}

static int raft_io_uv__set_term(struct raft_io *io, const raft_term term)
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_store__term(&uv->store, term);
}

static int raft_io_uv__set_vote(struct raft_io *io, const unsigned server_id)
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_store__vote(&uv->store, server_id);
}

static int raft_io_uv__append(const struct raft_io *io,
                              const struct raft_entry entries[],
                              unsigned n,
                              void *data,
                              void (*cb)(void *data, int status))
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_store__append(&uv->store, entries, n, data, cb);
}

static int raft_io_uv__send(const struct raft_io *io,
                            const struct raft_message *message,
                            void *data,
                            void (*cb)(void *data, int status))
{
    struct raft_io_uv *uv;

    uv = io->data;

    return raft_io_uv_rpc__send(&uv->rpc, message, data, cb);
}

int raft_io_uv_init(struct raft_io *io,
                    struct raft_logger *logger,
                    struct uv_loop_s *loop,
                    const char *dir,
                    struct raft_io_uv_transport *transport)
{
    struct raft_io_uv *uv;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        rv = RAFT_ERR_NOMEM;
        goto err;
    }

    rv = raft_io_uv_store__init(&uv->store, logger, loop, dir);
    if (rv != 0) {
        goto err_after_uv_alloc;
    }

    rv = raft_io_uv_rpc__init(&uv->rpc, logger, loop, transport);
    if (rv != 0) {
        goto err_after_store_init;
    }

    uv->logger = logger;
    uv->loop = loop;
    uv->transport = transport;

    uv->last_tick = 0;

    io->data = uv;
    io->start = raft_io_uv__start;
    io->stop = raft_io_uv__stop;
    io->load = raft_io_uv__load;
    io->bootstrap = raft_io_uv__bootstrap;
    io->set_term = raft_io_uv__set_term;
    io->set_vote = raft_io_uv__set_vote;
    io->append = raft_io_uv__append;
    io->send = raft_io_uv__send;

    return 0;

err_after_store_init:
    raft_io_uv_store__close(&uv->store);

err_after_uv_alloc:
    raft_free(uv);

err:
    assert(rv != 0);
    return rv;
}

void raft_io_uv_close(struct raft_io *io)
{
    struct raft_io_uv *uv;

    uv = io->data;

    raft_io_uv_store__close(&uv->store);
    raft_io_uv_rpc__close(&uv->rpc);

    raft_free(uv);
}
