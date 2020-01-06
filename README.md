[![Build Status](https://travis-ci.org/canonical/raft.png)](https://travis-ci.org/canonical/raft) [![codecov](https://codecov.io/gh/canonical/raft/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/raft) [![Documentation Status](https://readthedocs.org/projects/raft/badge/?version=latest)](https://raft.readthedocs.io/en/latest/?badge=latest)

Fully asynchronous C implementation of the Raft consensus protocol.

The library has modular design: its core part implements only the core Raft
algorithm logic, in a fully platform independent way. On top of that, a
pluggable interface defines the I/O implementation for networking (send/receive
RPC messages) and disk persistence (store log entries and snapshots).

A stock implementation of the I/O interface is provided when building the
library with default options. It is based on [libuv](http://libuv.org) and
should fit the vast majority of use cases. The only catch is that it currently
requires Linux, since it uses the Linux
[AIO](http://man7.org/linux/man-pages/man2/io_submit.2.html) API for disk
I/O. Patches are welcome to add support for more platforms.

See [raft.h](https://github.com/canonical/raft/blob/master/include/raft.h) for full documentation.

Licence
-------

This raft C library is released under a slightly modified version of LGPLv3,
that includes a copiright exception letting users to statically link the library
code in their project and release the final work under their own terms. See the
full [license](https://github.com/canonical/raft/blob/LICENSE) text.

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
- Leadership transfer extension

Building
--------

```bash
autoreconf -i
./configure --enable-example
make
```

Example
-------

The best way to understand how to use the library is probably reading the code
of the [example server](https://github.com/canonical/raft/blob/master/example/server.c)
included in the source code.

You can also see the example server in action by running:

```bash
./example/cluster
```

which spawns a little cluster of 3 servers, runs a sample workload, and randomly
stops and restarts a server from time to time.

Quick guide
-----------

It is recommended that you read
[raft.h](https://github.com/canonical/raft/blob/master/include/raft.h) for
documentation details, but here's a quick high-level guide of what you'll need
to do (error handling is omitted for brevity).

Create an instance of the stock ```raft_io``` interface implementation (or
implement your own one if the one that comes with the library really does not
fit):

```C
const char *dir = "/your/raft/data";
struct uv_loop_s loop;
struct raft_uv_transport transport;
struct raft_io io;
uv_loop_init(&loop);
raft_uv_tcp_init(&transport, &loop);
raft_uv_init(&io, &loop, dir, &transport);
```

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
raft_bootstrap(&raft, &configuration);
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

To add more servers to the cluster use the ```raft_add()``` and
```raft_promote``` APIs.
  
Notable users
-------------

- [dqlite](https://github.com/canonical/dqlite)

Credits
-------

Of course the biggest thanks goes to Diego Ongaro :) (the original author of the
Raft dissertation)

A lot of ideas and inspiration was taken from other Raft implementations such
as:

- CoreOS' Go implementation for [etcd](https://github.com/etcd-io/etcd/tree/master/raft)
- Hashicorp's Go [raft](https://github.com/hashicorp/raft)
- Willem's [C implementation](https://github.com/willemt/raft)
- LogCabin's [C++ implementation](https://github.com/logcabin/logcabin)
