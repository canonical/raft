[![Build Status](https://travis-ci.org/CanonicalLtd/raft.png)](https://travis-ci.org/CanonicalLtd/raft) [![codecov](https://codecov.io/gh/CanonicalLtd/raft/branch/master/graph/badge.svg)](https://codecov.io/gh/CanonicalLtd/raft)

Fully asynchronous C implementation of the Raft consensus protocol.

The library has modular design: its core part implements only the core Raft
algorithm logic, in a fully platform independent way. On top of that, a
pluggable interface defines the I/O implementation for networking (send/receive
RPC messages) and disk persistence (store log entries).

A stock implementation of the I/O interface is provided when building the
library with default options. It is based on [libuv](http://libuv.org) and
should fit the fast majority of use cases. The only catch is that it requires
Linux, since it uses the Linux
[AIO](http://man7.org/linux/man-pages/man2/io_submit.2.html) API for disk
I/O. Patches are welcome to add support for more platforms.

See [raft.h](https://github.com/CanonicalLtd/raft/blob/master/include/raft.h) for full documentation.

Building
--------

```bash
autoreconf -i
./configure
make
```

Example
-------

The best way to understand how to use the library is probably reading the code
of the [example server](https://github.com/CanonicalLtd/raft/blob/master/example/server.c)
included in the source code.

You can also see the example server in action by running:

```bash
./example-cluster
```

which spawns a little cluster of 3 servers, runs a sample workload, and randomly
stops and restarts a server from time to time.

Notable users
-------------

- [dqlite](https://github.com/CanonicalLtd/dqlite)
