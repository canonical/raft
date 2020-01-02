C-Raft
======

C-Raft is a fully asynchronous C implementation of the Raft consensus protocol.

Design
------

The library has modular design: its core part implements only the core Raft
algorithm logic, in a fully platform independent way. On top of that, a
pluggable interface defines the I/O implementation for networking and disk
persistence.

A stock implementation of the I/O interface is provided when building the
library with default options. It is based on `libuv`_ and should fit the vast
majority of use cases.

.. _libuv: http://libuv.org


Features
--------

C-Raft implements all the basic features described in the Raft dissertation:

* Leader election
* Log replication
* Log compaction
* Membership changes

It also includes a few optional enhancements:

* Optimistic pipelining to reduce log replication latency
* Writing to leader's disk in parallel
* Automatic stepping down when the leader loses quorum
* Leadership transfer extension
* Non-voting servers

.. toctree::
   :maxdepth: 1

   self
   api

Quick start
-----------

Make sure that `libuv`_ is installed on your system, then run:

.. code-block:: bash

   autoreconf -i
   ./configure --enable-example
   make

Then create a :file:`main.c` file with this simple test program that just runs a
single raft server and implements a basic state machine for incrementing a
counter:
