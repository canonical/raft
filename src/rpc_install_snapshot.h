/**
 * InstallSnapshpt RPC handlers.
 */

#ifndef RAFT_RPC_INSTALL_SNAPSHOT_H_
#define RAFT_RPC_INSTALL_SNAPSHOT_H_

#include "../include/raft.h"

/**
 * Process an InstallSnapshot RPC from the given server.
 */
int raft_rpc__recv_install_snapshot(struct raft *r,
                                    const unsigned id,
                                    const char *address,
                                    struct raft_install_snapshot *args);

#endif /* RAFT_RPC_INSTALL_SNAPSHOT_H_ */
