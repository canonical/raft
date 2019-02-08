#include <assert.h>
#include <stdio.h>

#include <uv.h>

#include "../include/raft.h"

#define N_SERVERS 3 /* Number of servers in the example cluster */

static void __stop_cb(void *data)
{
    struct uv_loop_s *loop = data;

    uv_stop(loop);
}

/**
 * Handler triggered by SIGINT. It will stop the raft engine.
 */
static void __sigint_cb(uv_signal_t *handle, int signum)
{
    struct raft *raft = handle->data;
    int rv;

    assert(signum == SIGINT);

    uv_signal_stop(handle);
    uv_close((uv_handle_t *)handle, NULL);

    rv = raft_stop(raft, handle->loop, __stop_cb);
    if (rv != 0) {
        printf("error: stop instance: %s\n", raft_strerror(rv));
    }
}

int main(int argc, char *argv[])
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint;
    struct raft_logger logger;
    struct raft_io_uv_transport transport;
    struct raft_io io;
    struct raft_configuration configuration;
    struct raft raft;
    const char *dir;
    unsigned id;
    char address[64];
    unsigned i;
    int rv;

    if (argc != 3) {
        printf("usage: example-server <dir> <id>\n");
        return 1;
    }

    dir = argv[1];
    id = atoi(argv[2]);
    sprintf(address, "127.0.0.1:900%d", id);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        printf("error: loop init: %s\n", uv_strerror(rv));
        goto err;
    }
    loop.data = &raft;

    /* Add a signal handler to stop the Raft engine upon SIGINT */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        printf("error: sigint init: %s\n", uv_strerror(rv));
        goto err_after_loop_init;
    }
    sigint.data = &raft;

    rv = uv_signal_start(&sigint, __sigint_cb, SIGINT);
    if (rv != 0) {
        printf("error: sigint start: %s\n", uv_strerror(rv));
        goto err_after_sigint_init;
    }

    logger = raft_default_logger;
    logger.data = &id;

    /* Initialize the TCP-based RPC transport */
    rv = raft_io_uv_tcp_init(&transport, &logger, &loop);
    if (rv != 0) {
        printf("error: init TCP transport: %s\n", raft_strerror(rv));
        goto err_after_sigint_start;
    }

    /* Initialize the libuv-based I/O backend */
    rv = raft_io_uv_init(&io, &logger, &loop, dir, &transport);
    if (rv != 0) {
        printf("error: enable uv integration: %s\n", raft_strerror(rv));
        goto err_after_tcp_init;
    }

    /* Bootstrap */
    raft_configuration_init(&configuration);
    for (i = 0; i < N_SERVERS; i++) {
        char address[64];
        unsigned id = i + 1;

        sprintf(address, "127.0.0.1:900%d", id);
        rv = raft_configuration_add(&configuration, id, address, true);
        if (rv != 0) {
            printf("error: add server %d to configuration: %s\n", id,
                   raft_strerror(rv));
            goto err_after_io_init;
        }
    }
    rv = io.bootstrap(&io, &configuration);
    if (rv != 0 && rv != RAFT_ERR_IO_NOTEMPTY) {
        printf("error: bootstrap: %s\n", raft_strerror(rv));
        goto err_after_configuration_init;
    }

    /* Initialize and start the Raft engine, using libuv-based I/O backend. */
    rv = raft_init(&raft, &logger, &io, NULL, NULL, id, address);
    if (rv != 0) {
        printf("error: init engine: %s\n", raft_strerror(rv));
        goto err_after_configuration_init;
    }

    rv = raft_start(&raft);
    if (rv != 0) {
        printf("error: start engine: %s\n", raft_strerror(rv));
        goto err_after_raft_init;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        printf("error: loop run: %s\n", uv_strerror(rv));
        goto err_after_raft_start;
    }

    /* Clean up */
    raft_configuration_close(&configuration);
    raft_close(&raft);
    raft_io_uv_close(&io);
    raft_io_uv_tcp_close(&transport);
    uv_loop_close(&loop);

    return 0;

err_after_raft_start:
    raft_stop(&raft, NULL, NULL);

err_after_raft_init:
    raft_close(&raft);

err_after_configuration_init:
    raft_configuration_close(&configuration);

err_after_io_init:
    raft_io_uv_close(&io);

err_after_tcp_init:
    raft_io_uv_tcp_close(&transport);

err_after_sigint_start:
    uv_signal_stop(&sigint);

err_after_sigint_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);

err_after_loop_init:
    /* Spin a few time to trigger close callbacks. */
    for (i = 0; i < 5; i++) {
        if (uv_run(&loop, UV_RUN_NOWAIT) == 0) {
            break;
        }
    }
    uv_loop_close(&loop);

err:
    return rv;
}
