.. _server:

:c:type:`struct raft` --- Raft server
=====================================

The raft server struct is the central part of C-Raft. It holds and drive the
state of a single raft server in a cluster.

Data types
----------

.. c:type:: struct raft

    A single raft server in a cluster.

Public members
^^^^^^^^^^^^^^

.. c:member:: void* struct raft.data

    Space for user-defined arbitrary data. C-Raft does not use and does not
    touch this field.

API
---

.. c:function:: int raft_init(struct raft *r, struct raft_io *io, struct raft_fsm *fsm, raft_id id, const char *address)

