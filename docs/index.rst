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

Quick start
-----------

Make sure that `libuv`_ is installed on your system, then run:

.. code-block:: bash
   :linenos:

   autoreconf -i
   ./configure --enable-example
   make

Then create a :file:`main.c` file with this simple test program that just runs a
single raft server and implements a basic state machine for incrementing a
counter:

.. code-block:: C
   :linenos:

   #include <raft.h>
   #include <raft/uv.h>
   
   static raft_id id = 12345;
   static const char *address = "127.0.0.1:8080";
   static const char *dir = "/tmp/raft-quick-start";
   static struct uv_loop_s loop;
   static struct raft_uv_transport transport;
   static struct raft_io io;
   static struct raft_fsm fsm;
   static struct raft raft;
   static struct raft_configuration conf;
   static struct uv_timer_s timer;
   static struct raft_apply apply;
   static unsigned counter = 0;
   static uint64_t command;
   
   static int applyCommand(struct raft_fsm *fsm,
                           const struct raft_buffer *buf,
                           void **result) {
       counter += *(uint64_t *)buf->base;
       printf("counter: %u\n", counter);
       return 0;
   }
   
   static void submitCommand(uv_timer_t *timer) {
       struct raft_buffer buf;
       command = uv_now(timer->loop) % 10;
       buf.len = sizeof command;
       buf.base = &command;
       raft_apply(&raft, &apply, &buf, 1, NULL);
   }
   
   int main() {
       mkdir(dir, 0755);
       uv_loop_init(&loop);
       raft_uv_tcp_init(&transport, &loop);
       raft_uv_init(&io, &loop, dir, &transport);
       fsm.apply = applyCommand;
       raft_init(&raft, &io, &fsm, id, address);
       raft_configuration_init(&conf);
       raft_configuration_add(&conf, id, address, RAFT_VOTER);
       raft_bootstrap(&raft, &conf);
       raft_start(&raft);
       uv_timer_init(&loop, &timer);
       uv_timer_start(&timer, submitCommand, 0, 1000);
       uv_run(&loop, UV_RUN_DEFAULT);
   }

You can compile and run it with:

.. code-block:: bash
   :linenos:

   cc main.c -o main -lraft -luv && ./main

Licence
-------

This raft C library is released under a slightly modified version of LGPLv3,
that includes a copiright exception letting users to statically link the library
code in their project and release the final work under their own terms. See the
full `license`_ text.

.. _license: https://github.com/canonical/raft/blob/master/LICENSE

toc
~~~

.. toctree::
   :maxdepth: 1

   self
   getting-started
   api
