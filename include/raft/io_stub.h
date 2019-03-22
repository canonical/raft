/**
 * Stub implementation of the @raft_io interface, meant for unit tests.
 */
#ifndef RAFT_IO_STUB_H
#define RAFT_IO_STUB_H

#include <stdbool.h>

struct raft_io;
struct raft_message;
struct raft_entry;
struct raft_snapshot;

typedef unsigned long long raft_term;

/**
 * Configure the given @raft_io instance to use a stub in-memory I/O
 * implementation.
 */
int raft_io_stub_init(struct raft_io *io);

/**
 * Release all memory held by the given stub I/O implementation.
 */
void raft_io_stub_close(struct raft_io *io);

/**
 * Set the current time, without invoking the tick callback.
 */
void raft_io_stub_set_time(struct raft_io *io, unsigned time);

/**
 * Set the random integer generator.
 */
void raft_io_stub_set_random(struct raft_io *io, int (*f)(int, int));

/**
 * Set the minimum and maximum values of the random latency that is assigned to
 * a message once the callback of its send request has been fired and it has
 * been enqueued for delivery. By default there is no latency and messages are
 * delivered instantaneously.
 */
void raft_io_stub_set_latency(struct raft_io *io, unsigned min, unsigned max);

/**
 * Set the initial term stored in this instance.
 */
void raft_io_stub_set_term(struct raft_io *io, raft_term term);

/**
 * Set the initial snapshot stored in this instance.
 */
void raft_io_stub_set_snapshot(struct raft_io *io,
                               struct raft_snapshot *snapshot);

/**
 * Set the initial entries stored in this instance.
 */
void raft_io_stub_set_entries(struct raft_io *io,
                              struct raft_entry *entry,
                              unsigned n);

/**
 * Advance the stub time by the given number of milliseconds, and invoke the
 * tick callback accordingly. Also, update the timers of messages in the
 * transmit queue, and if any of them expires deliver it to the destination peer
 * (if connected).
 */
void raft_io_stub_advance(struct raft_io *io, unsigned msecs);

/**
 * Flush the oldest request in the pending I/O queue, invoking the associated
 * callback as appropriate.
 *
 * Return true if there are more pending requests in the queue.
 */
bool raft_io_stub_flush(struct raft_io *io);

/**
 * Flush all pending I/O requests.
 */
void raft_io_stub_flush_all(struct raft_io *io);

/**
 * Return the amount of milliseconds left before the next message gets
 * delivered. If no delivery is pending, return -1.
 */
int raft_io_stub_next_deliver_timeout(struct raft_io *io);

/**
 * Manually trigger the delivery of a message, invoking the recv callback.
 */
void raft_io_stub_deliver(struct raft_io *io, struct raft_message *message);

/**
 * Return the number of pending append requests (i.e. requests successfully
 * submitted with raft_io->append(), but whose callbacks haven't been fired
 * yet).
 */
unsigned raft_io_stub_n_appending(struct raft_io *io);

/**
 * Return the entries if the i'th pending append request, or NULL.
 */
void raft_io_stub_appending(struct raft_io *io,
                            unsigned i,
                            const struct raft_entry **entries,
                            unsigned *n);

/**
 * Return the number of pending raft_io_send requests (i.e. requests
 * successfully submitted with raft_io->send(), but whose callbacks haven't been
 * fired yet).
 */
unsigned raft_io_stub_n_sending(struct raft_io *io);

/**
 * Return a pointer to the message associated with the i'th pending raft_io_send
 * request, or NULL.
 */
void raft_io_stub_sending(struct raft_io *io,
                          unsigned i,
                          struct raft_message **message);

/**
 * Inject a failure that will be triggered after @delay I/O requests and occur
 * @repeat times.
 */
void raft_io_stub_fault(struct raft_io *io, int delay, int repeat);

/**
 * Convenience for getting the current term stored in the stub.
 */
unsigned raft_io_stub_term(struct raft_io *io);

/**
 * Convenience for getting the current vote stored in the stub.
 */
unsigned raft_io_stub_vote(struct raft_io *io);

/**
 * Connect @io to @other, enabling delivery of messages sent from @io to @other.
 */
void raft_io_stub_connect(struct raft_io *io, struct raft_io *other);

/**
 * Return #true if @io is connected to @other.
 */
bool raft_io_stub_connected(struct raft_io *io, struct raft_io *other);

/**
 * Diconnect @io from @other, disabling delivery of messages sent from @io to
 * @other.
 */
void raft_io_stub_disconnect(struct raft_io *io, struct raft_io *other);

/**
 * Reconnect @io to @other, re-enabling delivery of messages sent from @io to
 * @other.
 */
void raft_io_stub_reconnect(struct raft_io *io, struct raft_io *other);

/**
 * Enable or disable silently dropping all outgoing messages of type @type.
 */
void raft_io_stub_drop(struct raft_io *io, int type, bool flag);

#endif /* RAFT_IO_STUB_H */
