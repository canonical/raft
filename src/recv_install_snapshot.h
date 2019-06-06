/* InstallSnapshpt RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include "../include/raft.h"

/* Process an InstallSnapshot RPC from the given server. */
int rpcRecvInstallSnapshot(struct raft *r,
                           const unsigned id,
                           const char *address,
                           struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
