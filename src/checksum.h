/**
 * Calculate checksums.
 */

#ifndef RAFT_CHECKSUM_H
#define RAFT_CHECKSUM_H

#include "../include/raft.h"

/**
 * Calculate the CRC32 checksum of the given data buffer.
 */
unsigned raft__crc32(const void *buf, const size_t size);

#endif /* RAFT_CHECKSUM_H */
