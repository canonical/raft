.. image:: https://travis-ci.org/CanonicalLtd/raft.png
   :target: https://travis-ci.org/CanonicalLtd/raft

.. image:: https://codecov.io/gh/CanonicalLtd/raft/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/CanonicalLtd/raft

C implementation of the Raft consensus protocol, BSD licensed.

See `raft.h <https://github.com/CanonicalLtd/raft/blob/master/include/raft.h>`_ for full documentation.

Building
========

.. code-block:: bash
   :class: ignore

   autoreconf -i
   ./configure
   make
