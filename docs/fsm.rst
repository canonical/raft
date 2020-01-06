.. _fsm:

:c:type:`struct raft_fsm` --- Application state machine
=======================================================

The FSM struct defines the interface that the application's state machine must
implement in order to be replicated by Raft.

Data types
----------

.. c:type:: struct raft_fsm

   Hold pointers to an actual implementation of the FSM interface.

Public members
^^^^^^^^^^^^^^

.. c:member:: void* data

    Space for user-defined arbitrary data. C-Raft does not use and does not
    touch this field.

.. c:member:: int version

   API version implemented by this instance. Currently 1.

.. c:member:: int (*apply)(struct raft_fsm *fsm,  const struct raft_buffer *buf, void **result)

   Apply a committed RAFT_COMMAND entry to the state machine.

.. c:member:: int (*snapshot)(struct raft_fsm *fsm, struct raft_buffer *bufs[], unsigned *n_bufs)

    Take a snapshot of the state machine.

.. c:member:: int (*restore)(struct raft_fsm *fsm, struct raft_buffer *buf)

    Restore a snapshot of the state machine.
