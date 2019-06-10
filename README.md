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

Features
--------

This implementation includes all the basic features described in the Raft
dissertation:

- Leader election
- Log replication
- Log compaction
- Membership changes

It also includes a few optional enhancements:

- Optimistic pipelining to reduce log replication latency
- Writing to leader's disk in parallel
- Automatic stepping down when the leader loses quorum

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

Quick guide
-----------

It is recommended that you read
[raft.h](https://github.com/CanonicalLtd/raft/blob/master/include/raft.h) for
documentation details, but here's a quick high-level guide of what you'll need
to do (error handling is omitted for brevity).

Define your application Raft FSM, implementing the ```raft_fsm``` interface:

```C
struct raft_fsm
{
  void *data;
  int (*apply)(struct raft_fsm *fsm, const struct raft_buffer *buf, void **result);
  int (*snapshot)(struct raft_fsm *fsm, struct raft_buffer *bufs[], unsigned *n_bufs);
  int (*restore)(struct raft_fsm *fsm, struct raft_buffer *buf);
}
```

Create an instance of the stock ```raft_io``` interface (or implement your one
if really the one that comes with the library does not fit):

```C
const char *dir = "/your/raft/data";
struct uv_loop_s loop;
struct raft_uv_transport transport;
struct raft_io io;
uv_loop_init(&loop);
raft_uv_tcp_init(&transport, &loop);
raft_uv_init(&io, &loop, dir, &transport);
```

Pick a unique ID and address for each server and initialize the raft object:

```C
unsigned id = 1;
const char *address = "192.168.1.1:9999";
struct raft raft;
raft_init(&raft, &io, &fsm, id, address);
```

If it's the first time you start the cluster, create a configuration object
containing each server that should be present in the cluster (typically just
one, since you can grow your cluster at a later point using ```raft_add``` and
```raft_promote```) and bootstrap:

```C
struct raft_configuration configuration;
raft_configuration_init(&configuration);
raft_configuration_add(&configuration, 1, "192.168.1.1:9999", true);
io.bootstrap(&io, &configuration);
```

Start the raft server:

```C
raft_start(&raft);
uv_run(&loop, UV_RUN_DEFAULT);
```

Asynchronously submit requests to apply new commands to your application FSM:

```C
static void applyCb(struct raft_apply *req, int status, void *result) {
  /* ... */
}

struct raft_apply req;
struct raft_buffer buf;
buf.len = ...; /* The length of your FSM entry data */
buf.base = ...; /* Your FSM entry data */
raft_apply(&raft, &req, &buf, 1, apply_callback);
```
  
Notable users
-------------

- [dqlite](https://github.com/CanonicalLtd/dqlite)
