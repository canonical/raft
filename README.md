[![Build Status](https://travis-ci.org/CanonicalLtd/raft.png)](https://travis-ci.org/CanonicalLtd/raft) [![codecov](https://codecov.io/gh/CanonicalLtd/raft/branch/master/graph/badge.svg)](https://codecov.io/gh/CanonicalLtd/raft)

C implementation of the Raft consensus protocol.

See [raft.h](https://github.com/CanonicalLtd/raft/blob/master/include/raft.h) for full documentation.

Building
--------

```bash
autoreconf -i
./configure
make
```

Notable users
-------------

- [dqlite](https://github.com/CanonicalLtd/dqlite)
