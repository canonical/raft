#include <sys/syscall.h>
#include <unistd.h>

#include "aio.h"

int io_setup(unsigned nr, aio_context_t *ctxp)
{
    return syscall(__NR_io_setup, nr, ctxp);
}

int io_destroy(aio_context_t ctx)
{
    return syscall(__NR_io_destroy, ctx);
}

int io_submit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
    return syscall(__NR_io_submit, ctx, nr, iocbpp);
}

int io_getevents(aio_context_t ctx,
                 long min_nr,
                 long max_nr,
                 struct io_event *events,
                 struct timespec *timeout)
{
    return syscall(__NR_io_getevents, ctx, min_nr, max_nr, events, timeout);
}
