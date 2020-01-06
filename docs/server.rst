.. _server:

:c:type:`struct raft` --- Raft server
=====================================

The raft server struct is the central part of C-Raft. It holds and drive the
state of a single raft server in a cluster.

Data types
----------

.. c:type:: struct raft

    A single raft server in a cluster.

.. c:type:: raft_id

   Hold the value of a raft server ID. Guaranteed to be at least 64-bit long.

.. c:type:: void (*raft_close_cb)(struct raft *r)

    Type definition for callback passed to :c:func:`raft_close`.


Public members
^^^^^^^^^^^^^^

.. c:member:: void* data

    Space for user-defined arbitrary data. C-Raft does not use and does not
    touch this field.

.. c:member:: raft_id id

    Server ID. Readonly.

API
---

.. c:function:: int raft_init(struct raft *r, struct raft_io *io, struct raft_fsm *fsm, raft_id id, const char *address)

    Initialize a raft server object.

.. c:function:: int raft_close(struct raft* r, raft_close_cb cb)

    Close a raft server object, releasing all used resources.

    The memory of the object itself can be released only once the given close
    callback has been invoked.

.. c:function:: int raft_start(struct raft* r)

   Start a raft server.

   The initial term, vote, snapshot and entries will be loaded from disk using
   the :c:func:`raft_io->load()` method. The instance will start as follower, unless
   it's the only voting server in the cluster, in which case it will
   automatically elect itself and become leader.
