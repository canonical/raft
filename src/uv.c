#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../include/raft.h"

#include "error.h"
#include "logger.h"

/**
 * Raft I/O backend implementation based on libuv.
 */
struct raft_uv__backend
{
    struct raft *raft;
    struct uv_loop_s *loop;
    struct uv_timer_s ticker;
    uint64_t last_tick;
};

/**
 * Convenience for wrapping an error message from libuv.
 */
static void raft_error__uv(struct raft *r, const int rv, const char *fmt, ...)
{
    const char *msg = uv_strerror(rv);
    va_list args;

    strncpy(r->errmsg, msg, strlen(msg));

    if (fmt == NULL) {
        return;
    }

    va_start(args, fmt);
    raft_error__vwrapf(r, fmt, args);
    va_end(args);
}

/**
 * Periodic tick timer callback.
 */
static void raft_uv__ticker_cb(uv_timer_t *ticker)
{
    struct raft_uv__backend *backend;
    uint64_t now;

    backend = ticker->data;

    now = uv_now(backend->loop);

    raft_tick(backend->raft, now - backend->last_tick);

    backend->last_tick = now;

}

/**
 * Start the backend.
 */
static int raft_uv__start(struct raft *r, unsigned tick)
{
    struct raft_uv__backend *backend;
    int rv;

    backend = r->backend.data;
    backend->last_tick = uv_now(backend->loop);

    rv = uv_timer_init(backend->loop, &backend->ticker);
    if (rv != 0) {
        raft_error__uv(r, rv, "init tick timer");
        return RAFT_ERR_INTERNAL;
    }
    backend->ticker.data = backend;

    rv = uv_timer_start(&backend->ticker, raft_uv__ticker_cb, 0, tick);
    if (rv != 0) {
        raft_error__uv(r, rv, "start tick timer");
        return RAFT_ERR_INTERNAL;
    }

    return 0;
}

static int raft_uv__stop(struct raft *r)
{
    struct raft_uv__backend *backend;
    int rv;

    backend = r->backend.data;

    rv = uv_timer_stop(&backend->ticker);
    if (rv != 0) {
        raft_error__uv(r, rv, "stop tick timer");
        return RAFT_ERR_INTERNAL;
    }

    uv_close((uv_handle_t *)&backend->ticker, NULL);

    return 0;
}

static void raft_uv__close(struct raft *r)
{
    raft_free(r->backend.data);
}

int raft_uv(struct raft *r, struct uv_loop_s *loop)
{
    struct raft_uv__backend *backend;

    assert(r != NULL);
    assert(loop != NULL);

    backend = raft_malloc(sizeof *backend);
    if (backend == NULL) {
        return RAFT_ERR_NOMEM;
    }

    backend->raft = r;
    backend->loop = loop;

    r->backend.data = backend;
    r->backend.start = raft_uv__start;
    r->backend.stop = raft_uv__stop;
    r->backend.close = raft_uv__close;

    return 0;
}
