#include "io.h"

#include "assert.h"
#include "log.h"

void IoPendingIncrement(struct raft *r)
{
    r->io_pending++;
}

void IoPendingDecrement(struct raft *r)
{
    assert(r->io_pending > 0);
    r->io_pending--;

    /* If we are shutting down and there are no more pending I/O requests, let's
     * invoke the close callback. */
    if (r->state == RAFT_UNAVAILABLE && r->io_pending == 0) {
        r->io->close(r->io, io_close_cb);
    }
}

void io_close_cb(struct raft_io *io)
{
    struct raft *r = io->data;

    if (r->io_pending > 0) {
        return;
    }

    raft_free(r->address);
    logClose(&r->log);
    raft_configuration_close(&r->configuration);

    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}
