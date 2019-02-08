#include <assert.h>
#include <stdio.h>

#include <uv.h>

#include "../include/raft.h"

#define N_SERVERS 3 /* Number of servers in the example cluster */

/**
 * Simple finite state machine that just increases a counter.
 */
struct __fsm
{
    int count;
};

static int __fsm__apply(struct raft_fsm *f, const struct raft_buffer *buf)
{
    struct __fsm *fsm = f->data;

    if (buf->len != 8) {
        return -1;
    }

    fsm->count += *(uint64_t *)buf->base;

    return 0;
}

static int __fsm_init(struct raft_fsm *f)
{
    struct __fsm *fsm = raft_malloc(sizeof *fsm);

    if (fsm == NULL) {
        printf("error: can't allocate finite state machine\n");
        return 1;
    }

    fsm->count = 0;

    f->version = 1;
    f->data = fsm;
    f->apply = __fsm__apply;

    return 0;
}

static void __fsm_close(struct raft_fsm *f)
{
    raft_free(f->data);
}

/**
 * Example raft server.
 */
struct __server
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint;
    struct uv_timer_s timer;
    struct raft_logger logger;
    struct raft_io_uv_transport transport;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
    const char *dir;
    unsigned id;
    char address[64];
};

static void __server_raft_stop_cb(void *data)
{
    struct uv_loop_s *loop = data;

    uv_stop(loop);
}

static void __server_timer_close_cb(struct uv_handle_s *handle)
{
    struct __server *s = handle->data;
    int rv;

    rv = raft_stop(&s->raft, handle->loop, __server_raft_stop_cb);
    if (rv != 0) {
        printf("error: stop instance: %s\n", raft_strerror(rv));
    }
}

static void __server_sigint_close_cb(struct uv_handle_s *handle)
{
    struct __server *s = handle->data;

    uv_timer_stop(&s->timer);

    uv_close((uv_handle_t *)&s->timer, __server_timer_close_cb);
}

/**
 * Handler triggered by SIGINT. It will initiate the shutdown sequence.
 */
static void __server_sigint_cb(struct uv_signal_s *handle, int signum)
{
    assert(signum == SIGINT);

    uv_signal_stop(handle);

    uv_close((uv_handle_t *)handle, __server_sigint_close_cb);
}

static int __server_init(struct __server *s, const char *dir, unsigned id)
{
    struct raft_configuration configuration;
    unsigned i;
    int rv;

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&s->loop);
    if (rv != 0) {
        printf("error: loop init: %s\n", uv_strerror(rv));
        goto err;
    }
    s->loop.data = s;

    /* Add a signal handler to stop the Raft engine upon SIGINT */
    rv = uv_signal_init(&s->loop, &s->sigint);
    if (rv != 0) {
        printf("error: sigint init: %s\n", uv_strerror(rv));
        goto err_after_loop_init;
    }
    s->sigint.data = s;

    /* Add a timer to periodically try to propose a new entry */
    rv = uv_timer_init(&s->loop, &s->timer);
    if (rv != 0) {
        printf("error: timer init: %s\n", uv_strerror(rv));
        goto err_after_sigint_init;
    }
    s->timer.data = s;

    /* Setup logging */
    raft_default_logger_set_server_id(id);
    raft_default_logger_set_level(RAFT_INFO);

    s->logger = raft_default_logger;

    /* Initialize the TCP-based RPC transport */
    rv = raft_io_uv_tcp_init(&s->transport, &s->logger, &s->loop);
    if (rv != 0) {
        printf("error: init TCP transport: %s\n", raft_strerror(rv));
        goto err_after_timer_init;
    }

    /* Initialize the libuv-based I/O backend */
    rv = raft_io_uv_init(&s->io, &s->logger, &s->loop, dir, &s->transport);
    if (rv != 0) {
        printf("error: enable uv integration: %s\n", raft_strerror(rv));
        goto err_after_tcp_init;
    }

    /* Initialize the finite state machine. */
    rv = __fsm_init(&s->fsm);
    if (rv != 0) {
        goto err_after_io_init;
    }

    /* Render the address */
    sprintf(s->address, "127.0.0.1:900%d", id);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->logger, &s->io, &s->fsm, NULL, id, s->address);
    if (rv != 0) {
        printf("error: init engine: %s\n", raft_strerror(rv));
        goto err_after_fsm_init;
    }

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    for (i = 0; i < N_SERVERS; i++) {
        char address[64];
        unsigned id = i + 1;

        sprintf(address, "127.0.0.1:900%d", id);
        rv = raft_configuration_add(&configuration, id, address, true);
        if (rv != 0) {
            printf("error: add server %d to configuration: %s\n", id,
                   raft_strerror(rv));
            goto err_after_raft_init;
        }
    }
    rv = s->io.bootstrap(&s->io, &configuration);
    if (rv != 0 && rv != RAFT_ERR_IO_NOTEMPTY) {
        printf("error: bootstrap: %s\n", raft_strerror(rv));
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);

err_after_raft_init:
    raft_close(&s->raft);

err_after_fsm_init:
    __fsm_close(&s->fsm);

err_after_io_init:
    raft_io_uv_close(&s->io);

err_after_tcp_init:
    raft_io_uv_tcp_close(&s->transport);

err_after_timer_init:
    uv_close((struct uv_handle_s *)&s->timer, NULL);

err_after_sigint_init:
    uv_close((struct uv_handle_s *)&s->sigint, NULL);

err_after_loop_init:
    uv_loop_close(&s->loop);

err:
    return rv;
}

static void __server_close(struct __server *s)
{
    raft_close(&s->raft);
    __fsm_close(&s->fsm);
    raft_io_uv_close(&s->io);
    raft_io_uv_tcp_close(&s->transport);
    uv_loop_close(&s->loop);
}

static void __server_timer_cb(uv_timer_t *timer)
{
    struct __server *s = timer->data;
    struct raft_buffer buf;
    int rv;

    if (s->raft.state != RAFT_STATE_LEADER) {
        return;
    }

    buf.len = sizeof(uint64_t);
    buf.base = raft_malloc(buf.len);

    *(uint64_t*)buf.base = 1;

    if (buf.base == NULL) {
        printf("error: allocate new entry: out of memory\n");
        return;
    }

    rv = raft_propose(&s->raft, &buf, 1);
    if (rv != 0) {
        printf("error: propose new entry: %s\n", raft_strerror(rv));
        return;
    }
}

static int __server_start(struct __server *s)
{
    int rv;

    rv = raft_start(&s->raft);
    if (rv != 0) {
        printf("error: start engine: %s\n", raft_strerror(rv));
        goto err;
    }

    rv = uv_signal_start(&s->sigint, __server_sigint_cb, SIGINT);
    if (rv != 0) {
        printf("error: sigint start: %s\n", uv_strerror(rv));
        goto err_after_raft_start;
    }

    rv = uv_timer_start(&s->timer, __server_timer_cb, 0, 1000);
    if (rv != 0) {
        printf("error: sigint start: %s\n", uv_strerror(rv));
        goto err_after_sigint_start;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&s->loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        printf("error: loop run: %s\n", uv_strerror(rv));
        goto err_after_timer_start;
    }

    return 0;

err_after_timer_start:
    uv_timer_stop(&s->timer);

err_after_sigint_start:
    uv_signal_stop(&s->sigint);

err_after_raft_start:
    raft_stop(&s->raft, NULL, NULL);
err:
    return rv;
}

int main(int argc, char *argv[])
{
    struct __server server;
    const char *dir;
    unsigned id;
    int rv;

    if (argc != 3) {
        printf("usage: example-server <dir> <id>\n");
        return 1;
    }

    dir = argv[1];
    id = atoi(argv[2]);

    /* Initialize the server. */
    rv = __server_init(&server, dir, id);
    if (rv != 0) {
        goto err;
    }

    /* Run. */
    rv = __server_start(&server);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Clean up */
    __server_close(&server);

    return 0;

err_after_server_init:
    __server_close(&server);

err:
    return rv;
}
