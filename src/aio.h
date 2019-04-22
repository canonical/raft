/* Declaration of the kernel AIO APIs that we use. This avoids having to depend
 * on libaio. */

#ifndef AIO_H_
#define AIO_H_

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

#endif /* AIO_H_ */
