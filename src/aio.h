/**
 * Declaration of the kernel AIO APIs that we use. This avoids having to depend
 * on libaio.
 */

#ifndef RAFT_AIO_H
#define RAFT_AIO_H

#include <linux/aio_abi.h>
#include <time.h>

int io_setup(unsigned n, aio_context_t *ctx);

int io_destroy(aio_context_t ctx);

int io_submit(aio_context_t ctx, long n, struct iocb **iocbs);

int io_getevents(aio_context_t ctx,
                 long min_nr,
                 long max_nr,
                 struct io_event *events,
                 struct timespec *timeout);

#endif /* RAFT_AIO_H */
