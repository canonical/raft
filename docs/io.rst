.. _io:

:c:type:`struct raft_io` --- I/O backend interface
==================================================

The I/O backend struct defines an interface for performing periodic ticks, log
store read/write and send/receive of network RPCs.

Data types
----------

.. c:type:: struct raft_io

   Hold pointers to an actual implementation of the I/O backend interface.

.. c:type:: void (*raft_io_close_cb)(struct raft_io *io)

   Type definition for callback passed to :c:func:`raft_io.close()`.

.. c:type:: void (*raft_io_tick_cb)(struct raft_io *io)

   Callback invoked by the I/O implementation at regular intervals.

.. c:type:: void (*raft_io_recv_cb)(struct raft_io *io, struct raft_message *msg)

   Callback invoked by the I/O implementation when an RPC message is received.


Public members
^^^^^^^^^^^^^^

.. c:member:: void* data

    Space for user-defined arbitrary data. C-Raft does not use and does not
    touch this field.

.. c:member:: int version

   API version implemented by this instance. Currently 1.

.. c:member:: void* impl

   Implementation-defined state.

.. c:member:: char errmsg[RAFT_ERRMSG_BUF_SIZE]

   Human-readable message providing diagnostic information about the last error
   occurred.

.. c:member:: int (*init)(struct raft_io *io, raft_id id, const char *address)

   Initialize the backend with operational parameters such as server ID and
   address.

.. c:member:: void (*close)(struct raft_io *io, raft_io_close_cb cb)

   Release all resources used by the backend.

   The :code:`tick` and :code:`recv` callbacks must not be invoked anymore, and
   pending asynchronous requests be completed or canceled as soon as
   possible. Invoke the close callback once the :c:type:`raft_io` instance can
   be freed.

.. c:member:: int (*load)(struct raft_io *io, raft_term *term, raft_id *voted_for,  struct raft_snapshot **snapshot, raft_index *start_index, struct raft_entry *entries[], size_t *n_entries)

   Load persisted state from storage.

   The implementation must synchronously load the current state from its
   storage backend and return information about it through the given
   pointers.

   The implementation can safely assume that this method will be invoked exactly
   one time, before any call to :c:func:`raft_io.append()` or
   c:func:`raft_io.truncate()`, and then won't be invoked again.

   The snapshot object and entries array must be allocated and populated using
   :c:func:`raft_malloc`. If this function completes successfully, ownership of
   such memory is transfered to the caller.

.. c:member:: int (*start)(struct raft_io *io, unsigned msecs, raft_io_tick_cb tick, raft_io_recv_cb recv)

    Start the backend.

    From now on the implementation must start accepting RPC requests and must
    invoke the :code:`tick` callback every :code:`msecs` milliseconds. The
    :code:`recv` callback must be invoked when receiving a message.

.. c:member:: int (*bootstrap)(struct raft_io *io, const struct raft_configuration *conf)

    Bootstrap a server belonging to a new cluster.

    The implementation must synchronously persist the given configuration as the
    first entry of the log. The current persisted term must be set to 1 and the
    vote to nil.

    If an attempt is made to bootstrap a server that has already some state,
    then RAFT_CANTBOOTSTRAP must be returned.

.. c:member:: int (*recover)(struct raft_io *io, const struct raft_configuration *conf)

   Force appending a new configuration as last entry of the log.

.. c:member:: int (*set_term)(struct raft_io *io, raft_term term)

    Synchronously persist current term (and nil vote).

    The implementation MUST ensure that the change is durable before returning
    (e.g. using :code:`fdatasync()` or O_DSYNC).

.. c:member:: int (*set_vote)(struct raft_io *io, raft_id server_id)

    Synchronously persist who we voted for.

    The implementation MUST ensure that the change is durable before returning
    (e.g. using :code:`fdatasync()` or O_DSYNC).

.. c:member:: int (*send)(struct raft_io *io, struct raft_io_send *req, const struct raft_message *message, raft_io_send_cb cb)

    Asynchronously send an RPC message.

    The implementation is guaranteed that the memory referenced in the given
    message will not be released until the :code:`cb` callback is invoked.

.. c:member:: int (*append)(struct raft_io *io, struct raft_io_append *req, const struct raft_entry entries[], unsigned n, raft_io_append_cb cb)

    Asynchronously append the given entries to the log.

    The implementation is guaranteed that the memory holding the given entries
    will not be released until the :code:`cb` callback is invoked.

.. c:member:: int (*truncate)(struct raft_io *io, raft_index index)

   Asynchronously truncate all log entries from the given index onwards.

.. c:member:: int (*snapshot_put)(struct raft_io *io, unsigned trailing, struct raft_io_snapshot_put *req, const struct raft_snapshot *snapshot, raft_io_snapshot_put_cb cb)

    Asynchronously persist a new snapshot. If the :code:`trailing` parameter is
    greater than zero, then all entries older that :code:`snapshot->index -
    trailing` must be deleted. If the :code:`trailing` parameter is :code:`0`,
    then the snapshot completely replaces all existing entries, which should all
    be deleted. Subsequent calls to append() should append entries starting at
    index :code:`snapshot->index + 1`.

    If a request is submitted, the raft engine won't submit any other request
    until the original one has completed.

.. c:member:: int (*snapshot_get)(struct raft_io *io, struct raft_io_snapshot_get *req, raft_io_snapshot_get_cb cb)

   Asynchronously load the last snapshot.

.. c:member:: raft_time (*time)(struct raft_io *io)

   Return the current time, expressed in milliseconds.

.. c:member:: int (*random)(struct raft_io *io, int min, int max)

   Generate a random integer between :code:`min` and :code:`max`.
