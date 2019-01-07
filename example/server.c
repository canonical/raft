#include <assert.h>
#include <stdio.h>

#include <uv.h>

#include "../include/raft.h"

/**
 * Handler triggered by SIGINT. It will stop the raft engine.
 */
static void __sigint_cb(uv_signal_t *handle, int signum)
{
    struct raft *raft = handle->data;

    assert(signum == SIGINT);

    uv_signal_stop(handle);
    uv_close((uv_handle_t *)handle, NULL);

    raft_stop(raft);
}

int main(int argc, char *argv[])
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint;
    struct raft_io io;
    struct raft raft;
    const char *dir;
    int rv;

    if (argc != 2) {
        printf("usage: example-server <dir>\n");
	return 1;
    }

    dir = argv[1];

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        printf("error: loop init: %s\n", uv_strerror(rv));
        return rv;
    }

    /* Add a signal handler to stop the Raft engine upon SIGINT */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        printf("error: sigint init: %s\n", uv_strerror(rv));
        return rv;
    }

    sigint.data = &raft;

    rv = uv_signal_start(&sigint, __sigint_cb, SIGINT);
    if (rv != 0) {
        printf("error: sigint start: %s\n", uv_strerror(rv));
        return rv;
    }

    rv = raft_io_uv_init(&io, &loop, dir);
    if (rv != 0) {
        printf("error: enable uv integration: %s\n", raft_errmsg(&raft));
        return rv;
    }

    /* Initialize and start the Raft engine, using libuv integration. */
    raft_init(&raft, &io, NULL /* TODO: fsm implementation */, NULL, 1);

    rv = raft_start(&raft);
    if (rv != 0) {
        printf("error: start engine: %s\n", raft_errmsg(&raft));
        return rv;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        printf("loop run: %s\n", uv_strerror(rv));
        return rv;
    }

    /* Clean up */
    raft_close(&raft);

    rv = uv_loop_close(&loop);
    if (rv != 0) {
        printf("loop close: %s\n", uv_strerror(rv));
        return rv;
    }

    return 0;
}
