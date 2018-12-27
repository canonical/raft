#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../include/raft.h"

#include "binary.h"
#include "error.h"
#include "io_uv_store.h"
#include "logger.h"
#include "uv_fs.h"

/**
 * Raft I/O implementation based on libuv.
 */
struct raft_io_uv
{
    struct raft_io_uv_store store; /* Implement on-disk persistance */
    struct uv_loop_s *loop;        /* UV event loop */
    struct uv_timer_s ticker;      /* Timer for periodic calls to raft_tick */
    uint64_t last_tick;            /* Timestamp of the last raft_tick call */

    /* The file containing the entries for the current open segment. This must
     * always be a valid file. */
    struct raft_uv_file open_segment;

    /* Parameters passed via raft_io->init */
    const struct raft_io_queue *queue;                 /* Request queue */
    struct raft_logger *logger;                        /* Logger */
    void *p;                                           /* Custom data pointer */
    void (*tick)(void *, const unsigned);              /* Tick function */
    void (*notify)(void *, const unsigned, const int); /* Notify function */

    char *errmsg; /* Error message buffer */
};

/**
 * Initializer.
 */
static void raft_io_uv__init(const struct raft_io *io,
                             const struct raft_io_queue *queue,
                             struct raft_logger *logger,
                             void *p,
                             void (*tick)(void *, const unsigned),
                             void (*notify)(void *, const unsigned, const int))
{
    struct raft_io_uv *uv;

    uv = io->data;

    uv->queue = queue;
    uv->logger = logger;
    uv->p = p;
    uv->tick = tick;
    uv->notify = notify;
}

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
    uv->tick(uv->p, now - uv->last_tick);

    /* Update the last tick timestamp */
    uv->last_tick = now;
}

/**
 * Start the backend.
 */
static int raft_io_uv__start(const struct raft_io *io, const unsigned msecs)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    /* Initialize the tick timer handle. */
    rv = uv_timer_init(uv->loop, &uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "init tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }
    uv->ticker.data = uv;

    /* Start the tick timer handle. */
    rv = uv_timer_start(&uv->ticker, raft_io_uv__ticker_cb, 0, msecs);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "start tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_IO;
    }

    return 0;
}

/**
 * Stop the backend.
 */
static int raft_io_uv__stop(const struct raft_io *io)
{
    struct raft_io_uv *uv;
    int rv;

    uv = io->data;

    /* Stop the tick timer handle. */
    rv = uv_timer_stop(&uv->ticker);
    if (rv != 0) {
        raft_errorf(uv->errmsg, "stop tick timer: %s", uv_strerror(rv));
        return RAFT_ERR_INTERNAL;
    }

    /* Close the ticker timer handle. */
    uv_close((uv_handle_t *)&uv->ticker, NULL);

    return 0;
}

/**
 * Close the backend, releasing all resources it allocated.
 */
static void raft_io_uv__close(const struct raft_io *io)
{
    struct raft_io_uv *uv;

    uv = io->data;

    raft_io_uv_store__close(&uv->store);
    raft_free(uv);
}

static int raft_io_uv__read_state(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    return raft_io_uv_store__load(&uv->store, request, uv->errmsg);
}

static int raft_io_uv__write_term(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    return raft_io_uv_store__term(&uv->store, request, uv->errmsg);
}

static int raft_io_uv__write_vote(struct raft_io_uv *uv,
                                  struct raft_io_request *request)
{
    return raft_io_uv_store__vote(&uv->store, request, uv->errmsg);
}

static int raft_io_uv__write_log(struct raft_io_uv *uv,
                                 const unsigned request_id)
{
    return raft_io_uv_store__entries(&uv->store, request_id, uv->errmsg);
}

static int raft_io_uv__submit(const struct raft_io *io,
                              const unsigned request_id)
{
    struct raft_io_request *request;
    struct raft_io_uv *uv;
    int rv;

    assert(io != NULL);

    uv = io->data;

    /* Get the request object */
    request = raft_io_queue_get(uv->queue, request_id);

    assert(request != NULL);

    /* Dispatch the request */
    switch (request->type) {
        case RAFT_IO_READ_STATE:
            rv = raft_io_uv__read_state(uv, request);
            break;
        case RAFT_IO_WRITE_TERM:
            rv = raft_io_uv__write_term(uv, request);
            break;
        case RAFT_IO_WRITE_VOTE:
            rv = raft_io_uv__write_vote(uv, request);
            break;
        case RAFT_IO_WRITE_LOG:
            rv = raft_io_uv__write_log(uv, request_id);
            break;
        default:
            assert(0);
    }

    return rv;
}

int raft_io_uv_init(struct raft_io *io, struct uv_loop_s *loop, const char *dir)
{
    struct raft_io_uv *uv;
    int rv;

    assert(io != NULL);
    assert(loop != NULL);
    assert(dir != NULL);

    /* Allocate the raft_io_uv object */
    uv = raft_malloc(sizeof *uv);
    if (uv == NULL) {
        raft_errorf(io->errmsg, "can't allocate I/O implementation instance");
        return RAFT_ERR_NOMEM;
    }

    uv->loop = loop;
    uv->last_tick = 0;
    uv->errmsg = io->errmsg;

    rv = raft_io_uv_store__init(&uv->store, dir, uv->errmsg);
    if (rv != 0) {
        raft_free(uv);
        return RAFT_ERR_NOMEM;
    }

    io->data = uv;
    io->init = raft_io_uv__init;
    io->start = raft_io_uv__start;
    io->stop = raft_io_uv__stop;
    io->close = raft_io_uv__close;
    io->submit = raft_io_uv__submit;

    return 0;
}
