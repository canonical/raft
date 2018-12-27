/**
 * Calculate checksums.
 */

#ifndef RAFT_CHECKSUM_H
#define RAFT_CHECKSUM_H

#include "../include/raft.h"

unsigned raft__crc32(const struct raft_buffer *buf);

#endif /* RAFT_CHECKSUM_H */
